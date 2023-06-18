
import json


class DcmModel:

  Str = ""
  StudyInstanceUID = ""
  RetrieveURL = ""
  ReferencedSOPSequence = ""
  ReferencedSOPClassUID = ""
  ReferencedStudyInstanceUID = ""
  InstanceURL = ""
  WarningDetail = ""
  Status = ""

  def __init__(self, str):
    self.Str = str
    data = json.loads(str)
    self.RetrieveURL = data["00081190"]["Value"][0]
    arr = self.RetrieveURL.split("/")
    self.StudyInstanceUID = arr[-1]
    self.InstanceURL = data["00081199"]["Value"][0]["00081190"]["Value"][0]



