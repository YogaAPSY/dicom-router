import http.client
import json
import configparser

config = configparser.ConfigParser()
config.read('router.conf')
client_key = config.get('satusehat', 'client_key')
secret_key = config.get('satusehat', 'secret_key')
url = config.get('satusehat', 'url')


def get_token():
  conn = http.client.HTTPSConnection(url)
  payload = 'client_id='+client_key+'&client_secret='+secret_key
  headers = {
    'Content-Type': 'application/x-www-form-urlencoded'
  }
  try:
    conn.request("POST", "/oauth2/v1/accesstoken?grant_type=client_credentials", payload, headers)
    res = conn.getresponse()
    data = json.loads(res.read().decode("utf-8"))
  except:
    print("[Error] - Authentication failed")
    return ""
  return data["access_token"]

