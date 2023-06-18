import sqlite3
import threading


class dbquery:

  conn = sqlite3.connect(':memory:',check_same_thread = False)
  # conn = sqlite3.connect("file::memory:?cache=shared", uri=True, check_same_thread = False)
  cursorObject = conn.cursor()
  lock = threading.Lock()

  INSERT_SOP="INSERT INTO dicom_obj VALUES (null,?,?,?,?,?,?,?,?,0,0)"
  UPDATE_ASSOC_COMPLETED="UPDATE dicom_obj SET association_completed = 1 WHERE association_id = ?"
  UPDATE_INSTANCE_STATUS_SENT="UPDATE dicom_obj SET sent_status = 1 WHERE association_id = ? AND study_iuid = ? AND series_iuid = ? AND instance_uid = ?"


  GET_IDS_PER_ASSOC="SELECT DISTINCT study_iuid, accession_number FROM dicom_obj WHERE association_id = ?"
  GET_INSTANCES_PER_ASSOC="SELECT study_iuid, series_iuid, instance_uid, sent_status FROM dicom_obj WHERE association_id = ? ORDER BY study_iuid, series_iuid, instance_uid"
  GET_INSTANCES_PER_STUDY="SELECT series_iuid, instance_uid FROM dicom_obj WHERE association_id = ? AND study_iuid = ? ORDER BY series_iuid, instance_uid"

  QUERY_SOP="SELECT * FROM dicom_obj WHERE association_id = ?"

  def __init__(self):
      createDICOMObjsTable = """
      CREATE TABLE dicom_obj (
        id integer PRIMARY KEY AUTOINCREMENT,
        association_id varchar(256),
        scu_ae varchar(32),
        scp_ae varchar(32),
        accession_number varchar(32),
        study_iuid varchar(64),
        series_iuid varchar(64),
        instance_uid varchar(64),
        fs_location varchar(1024),
        sent_status short,
        association_completed short);
      """
      
      self.conn.execute(createDICOMObjsTable)
      self.cursorObject = self.conn.cursor()
      self.lock = threading.Lock()
      print("[Init] - In-memory database created.")

  def Update(self, query, entries):
      try:
          self.lock.acquire(True)
          cursorObject = self.conn.cursor()
          cursorObject.execute("BEGIN;")
          #print(query+","+str(entries[0])+","+str(entries[1]))
          cursorObject.execute(query,entries)
          cursorObject.execute("COMMIT;")
      except BaseException as err:
          print(err)    
      finally:
          self.lock.release()      

  def Insert(self, query, entries):
      try:
          self.lock.acquire(True)
          cursorObject = self.conn.cursor()
          cursorObject.execute("BEGIN;")
          cursorObject.execute(query,entries)
          cursorObject.execute("COMMIT;")
          #self.conn.commit()
      except BaseException as err:
          print(err)
      finally:
          self.lock.release()    

  def Delete(self, query, entries):
      try:
          self.lock.acquire(True)
          cursorObject = self.conn.cursor()
          cursorObject.execute("BEGIN;")
          cursorObject.execute(query,entries)
          cursorObject.execute("COMMIT;")
          #self.conn.commit()
      except BaseException as err:
          print(err)
      finally:
          self.lock.release()              


  def Query(self, query , entries):
      try:
          self.lock.acquire(True)
          self.cursorObject.execute(query, entries)
          retdataset = self.cursorObject.fetchall()
      except BaseException as err:
          print(err)
      finally:
          self.lock.release()
      # print("query : "+query)
      # for r in retdataset:
      #     var=""
      #     for i in r:
      #         var+=str(i)+" "
      #     print("returned object: \r\n"+var)
      return retdataset 


