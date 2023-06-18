import configparser
import hashlib
import hmac
import json
import logging
import os
import shutil
from pynetdicom import AE, evt, AllStoragePresentationContexts, debug_logger, StoragePresentationContexts, DEFAULT_TRANSFER_SYNTAXES
import http.client
from utils import oauth2
from utils.dicom2fhir import process_dicom_2_fhir
from time import sleep
from pydicom.uid import JPEGLosslessSV1, JPEG2000Lossless
from utils.dbquery import dbquery

from pynetdicom.sop_class import Verification

config = configparser.ConfigParser()
config.read('router.conf')
url = config.get('satusehat', 'url')
organization_id = config.get('satusehat', 'organization_id')
dicom_pathsuffix = config.get('satusehat', 'dicom_pathsuffix')
fhir_pathsuffix = config.get('satusehat', 'fhir_pathsuffix')
self_ae_title = config.get('satusehat', 'ae_title')
dicom_port = config.get('satusehat', 'dicom_port')
dcm_dir = config.get('satusehat', 'dcm_dir')

token = str()

debug_logger()
LOGGER = logging.getLogger('pynetdicom')

def make_association_id(event):
  return event.assoc.name+"#"+str(event.assoc.native_id)

def make_hash(study_id):
  key = "e179317a-62b0-4996-8999-e91aabcd"
  byte_key = bytes(key, 'UTF-8')  # key.encode() would also work in this case
  message = study_id.encode()
  h = hmac.new(byte_key, message, hashlib.sha256).hexdigest()  
  return h

def get_service_request(accessionNumber):
  token = oauth2.get_token()
  conn = http.client.HTTPSConnection(url)
  payload = ''
  headers = {
    'Accept': 'application/json',
    'Authorization': 'Bearer ' + token
  }
  path = fhir_pathsuffix + "/ServiceRequest?identifier=http://sys-ids.kemkes.go.id/acsn/" + organization_id + "%7C" + accessionNumber + '&_sort=-_lastUpdated&_count=1'
  conn.request("GET", path, payload, headers)
  res = conn.getresponse()
  data = json.loads(res.read().decode("utf-8"))
  if(data["resourceType"]=="Bundle" and data["total"]>=1):
    _,patientID = data["entry"][0]["resource"]["subject"]["reference"].split("/")
    return data["entry"][0]["resource"]["id"],patientID
  raise Exception("ServiceRequest not found")

def get_imaging_study(accessionNumber):
  token = oauth2.get_token()
  conn = http.client.HTTPSConnection(url)
  payload = ''
  headers = {
    'Accept': 'application/json',
    'Authorization': 'Bearer ' + token
  }
  path = fhir_pathsuffix + "/ImagingStudy?identifier=http://sys-ids.kemkes.go.id/acsn/" + organization_id + "%7C" + accessionNumber + '&_sort=-_lastUpdated&_count=1'
  conn.request("GET", path, payload, headers)
  res = conn.getresponse()
  data = json.loads(res.read().decode("utf-8"))
  if(data["resourceType"]=="Bundle" and data["total"]>=1):
    _,patientID = data["entry"][0]["resource"]["subject"]["reference"].split("/")
    return data["entry"][0]["resource"]["id"]
  return None


def imagingstudy_post(filename, id):
  token = oauth2.get_token()
  conn = http.client.HTTPSConnection(url)
  payload = open(filename,'rb')
  headers = {
    'Authorization': 'Bearer ' + token,
    'Content-Type': 'application/json'
  }
  if id==None:
    conn.request("POST", fhir_pathsuffix+"/ImagingStudy", payload, headers)
  else:
    conn.request("PUT", fhir_pathsuffix+"/ImagingStudy/"+id, payload, headers)
  res = conn.getresponse()
  data = json.loads(res.read().decode("utf-8"))
  if(data["resourceType"]=="ImagingStudy"):
    return data["id"]
  return None



def dicom_push(assocId,study_iuid):
  print("[Info] - DICOM Push started")
  token = oauth2.get_token()
  conn = http.client.HTTPSConnection(url)

  subdir = make_hash(assocId)
  headers = {
    'Content-Type': 'application/dicom',
    'Accept': 'application/dicom+json',
    'Authorization': 'Bearer ' + token
  }

  instances = dbq.Query(dbq.GET_INSTANCES_PER_STUDY,[assocId,study_iuid])
  for n in range(len(instances)):
    series_iuid = instances[n][0]
    instance_uid = instances[n][1]
    filename = os.getcwd()+dcm_dir+subdir+"/"+study_iuid+"/"+series_iuid+"/"+instance_uid+".dcm"
    try:
      payload = open(filename,'rb')
      str = ""
      conn.request("POST", dicom_pathsuffix, payload, headers)
      res = conn.getresponse()
      data = res.read()
      str = data.decode("utf-8")
      print("[Info] - Sending Instance UID: "+instance_uid+" success")
      dbq.Update(dbq.UPDATE_INSTANCE_STATUS_SENT,[assocId,study_iuid,series_iuid,instance_uid])
    except Exception as err:
      print(err)
      print("[Error] - Sending Instance UID failed: "+instance_uid)
      raise Exception("Sending DICOM failed")

    # output = os.getcwd()+dcm_dir+subdir+ "dicom-push.json"
    # with open(output, 'w') as out:
    #   out.write(str)

    if(str.find("Instance already exists")>=0):
      print("[Warn] - Image already exists")

      # Remove Instance UID
      os.remove(os.getcwd()+dcm_dir+subdir+"/"+study_iuid+"/"+series_iuid+"/"+instance_uid+".dcm")

      # Remove Series UID Folder if Empty
      os.rmdir(os.getcwd()+dcm_dir+subdir+"/"+study_iuid+"/"+series_iuid)

  
  return True

# Implement a handler for evt.EVT_C_STORE
def handle_store(event):
  """Handle a C-STORE request event."""
  # Decode the C-STORE request's *Data Set* parameter to a pydicom Dataset
  print("[Info-Assoc] - handle_store")
  
  ds = event.dataset
  ds = ds[0x00030000:]

  # Add the File Meta Information
  ds.file_meta = event.file_meta

  # print("[Info-Assoc] - StudyInstanceUID      : " + ds.StudyInstanceUID)
  # print("[Info-Assoc] - SeriesInstanceUID     : " + ds.SeriesInstanceUID)
  # print("[Info-Assoc] - SOPInstanceUID        : " + ds.SOPInstanceUID)
  # print("[Info-Assoc] - event.assoc.name      : " + event.assoc.name)
  # print("[Info-Assoc] - event.assoc.native_id : " + str(event.assoc.native_id))

  assocId = make_association_id(event)
  subdir = make_hash(assocId)
  subdir = subdir + "/" + ds.StudyInstanceUID + "/" + ds.SeriesInstanceUID + "/"

  try:
    os.makedirs(os.getcwd()+dcm_dir+subdir, exist_ok=True)
    print("[Info] - Directory created")
  except:
    print("[Info] - Directory already created")
  filename = os.getcwd()+dcm_dir+subdir+ds.SOPInstanceUID+".dcm"
  ds.save_as(filename, write_like_original=False)

  # insert into db
  scu_ae=event.assoc.requestor.primitive.calling_ae_title
  scp_ae=event.assoc.requestor.primitive.called_ae_title
  entry = (
    make_association_id(event),
    scu_ae, 
    scp_ae,
    ds.AccessionNumber,
    ds.StudyInstanceUID, 
    ds.SeriesInstanceUID, 
    ds.SOPInstanceUID,  
    filename)
  try:
      dbq.Insert(dbq.INSERT_SOP , entry)
  except:
      print("Could not insert the entry into in-memory database.")


  # Return a 'Success' status
  return 0x0000

def handle_assoc_released(event):
  """Handle an ASSOCIATION RELEASE event."""
  print("[Info] - Processing DICOM start")
  assocId = make_association_id(event)
  try:
    dbq.Update(dbq.UPDATE_ASSOC_COMPLETED,[assocId])
    ids = dbq.Query(dbq.GET_IDS_PER_ASSOC,[assocId])
    print(ids);
    for stdy in range(len(ids)):
      study_iuid = ids[stdy][0]
      accession_no = ids[stdy][1]
      print("[Info] - Accession Number: "+accession_no)
      print("[Info] - Study IUID: "+study_iuid)
      imagingStudyID = get_imaging_study(accession_no)
      subdir = make_hash(assocId)
      study_dir = os.getcwd()+dcm_dir+subdir+"/"+study_iuid
      serviceRequestID = None
      patientID = None
      try:
        print("[Info] - Obtaining Patient ID and ServiceRequest ID")
        serviceRequestID, patientID = get_service_request(accession_no)
        print("[Info] - Patient ID and ServiceRequest ID obtained")
      except:
        print("[Error] - Failed to obtain Patient ID and ServiceRequest ID")

      # Create ImagingStudy
      imagingStudyCreated = False
      if serviceRequestID!=None and patientID!=None:
        try:
          print("[Info] - Start creating ImagingStudy")
          imagingStudy = process_dicom_2_fhir(study_dir, imagingStudyID, serviceRequestID, patientID)
          output = study_dir+ "/ImagingStudy.json"
          with open(output, 'w') as out:
            out.write(imagingStudy.json(indent=2))
          imagingStudyCreated = True
          print("[Info] - ImagingStudy "+study_iuid+" created")
        except:
          print("[Error] - Failed to create ImagingStudy for " + study_iuid)
      
      # POST ImagingStudy
      imagingStudyPosted = False
      if imagingStudyCreated:
        try:
          imaging_study_json = study_dir+ "/ImagingStudy.json"
          if imagingStudyID==None:
            print("[Info] - POST-ing ImagingStudy")
            id = imagingstudy_post(imaging_study_json, None)
            print("[Info] - ImagingStudy POST-ed, id: "+id)
          else:
            id = imagingstudy_post(imaging_study_json, imagingStudyID)
            print("[Info] - ImagingStudy already POST-ed, using PUT instead, id: "+id)
          imagingStudyPosted = True
        except:
          print("[Error] - Failed to POST ImagingStudy")
    
      # Send DICOM
      if imagingStudyPosted:
        try:
          dicom_push(assocId, study_iuid)
          print("[Info] - DICOM sent successfully")
        except:
          print("[Error] - Failed to send DICOM")
    
    # Check and delete if all clear
    unsentInstances = False
    instances = dbq.Query(dbq.GET_INSTANCES_PER_ASSOC,[assocId])
    for n in range(len(instances)):
      sent_status = instances[n][3]
      if sent_status==0:
        unsentInstances = True
    
    # # Delete if all sent
    # if unsentInstances==False:
    #   print("[Info] - Deleting association folder")
    #   try:
    #     subdir = make_hash(assocId)
    #     shutil.rmtree(os.getcwd()+dcm_dir+subdir)
    #   except BaseException as e :
    #     print(e)


  except Exception as e:
    print("Could not process association: "+assocId)
    print(e)

  # Return a 'Success' status
  return 0x0000


# ====================================================
# Main
# ====================================================

print("[Init] - DICOM Router start")

# Setup database
print("[Init] - Creating in-memory database interface")
dbq = dbquery()

# Setup event handlers
print("[Init] - Set up store handler")
handlers = [
  (evt.EVT_C_STORE, handle_store),
  (evt.EVT_RELEASED, handle_assoc_released)
  ]

# Initialise the Application Entity
ae = AE(ae_title=self_ae_title)

transfer_syntaxes = DEFAULT_TRANSFER_SYNTAXES + [JPEGLosslessSV1, JPEG2000Lossless]

for context in StoragePresentationContexts:
    ae.add_supported_context(context.abstract_syntax, transfer_syntaxes)

# Support verification SCP (echo)
ae.add_supported_context(Verification)

# Support presentation contexts for all storage SOP Classes
ae.supported_contexts = AllStoragePresentationContexts


# Set to require the *Called AE Title* must match the AE title
ae.require_called_aet = self_ae_title

# Purge and re-create the incoming folder
print("[Init] - Clearing incoming folder")
try:
  shutil.rmtree(os.getcwd()+dcm_dir)
except BaseException as err :
  print(err) 
os.mkdir(os.getcwd()+dcm_dir)

# Start listening for incoming association requests
print("[Init] - Spawning DICOM interface on port "+dicom_port+" with AE title: "+self_ae_title+".")
ae.start_server(("0.0.0.0", int(dicom_port)), evt_handlers=handlers)