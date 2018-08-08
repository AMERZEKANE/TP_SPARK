# TP_Hbase

#!pip install requests_kerberos

import sys
#sys.path.append("/home/cdsw/test_project001/hadoop_remote_library/")
import base64
import requests
import json
import xml.etree.ElementTree as ET

from requests_kerberos import HTTPKerberosAuth, OPTIONAL


HBASE_REST_URL = "http://acfsv817bna182p.bigdata4sg.saint-gobain.net:20550"


class HBaseREST:


  def __init__(self, url=HBASE_REST_URL, namespace=None):
    self.url = url
    self.namespace = namespace
    self.auth = HTTPKerberosAuth(mutual_authentication=OPTIONAL)
    self._getHeaders = { "Accept": "text/xml" }
    self._putHeaders = { "Content-Type": "text/xml", "Accept": "text/xml" }


  def _getAPI(self, payload):
    r = requests.get(
      url=self.url + payload, auth=self.auth,
      headers=self._getHeaders
    )
    if r.status_code == 200:
      return ET.fromstring(r.content)
    raise Exception("Error: HBase REST API send HTTP Code: {}".format(r.status_code))


  def _putAPI(self, payload, data):
    r = requests.put(
      url=self.url + payload, auth=self.auth,
      headers=self._putHeaders, data=data
    )
    if r.status_code == 200:
      return True
    raise Exception("Error: HBase REST API send HTTP Code: {}".format(r.status_code))


  def version(self):
    return self._getAPI("/version/cluster").text


  def listTables(self):
    root = self._getAPI("/namespaces/{}/tables".format(self.namespace))
    return [child.attrib["name"] for child in root]


  def getRow(self, table, row):
    if self.namespace:
      root = self._getAPI("/{}:{}/{}".format(self.namespace, table, row))
    else:
      root = self._getAPI("/{}/{}".format(table, row))
    row = root[0]
    res = {}
    for cell in row:
      column = base64.b64decode(cell.attrib["column"])
      value = base64.b64decode(cell.text)
      res[column] = value
    return res


  def putCell(self, table, row, column, value):
    data = '<?xml version="1.0" encoding="UTF-8" standalone="yes"?><CellSet><Row key="%s"><Cell column="%s">%s</Cell></Row></CellSet>' % (
      base64.b64encode(row),
      base64.b64encode(column),
      base64.b64encode(value)
    )
    if self.namespace:
      self._putAPI(
        "/{}:{}/{}/".format(self.namespace, table, row),
        data  
      )
      
      
      
 
## good by 
# kill une demande
