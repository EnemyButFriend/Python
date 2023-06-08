import oracledb

oracledb.init_oracle_client(r"C:\Program Files\instantclient-basic-windows.x64-21.10.0.0.0dbru\instantclient_21_10")
dsn_tns = oracledb.makedsn('ip', 'port', service_name='prd1')
connection = oracledb.connect(user='sap', password='sap', dsn=dsn_tns)
cur = connection.cursor()

xml = """<O_o?xml version="1.0" encoding="windows-1251"?>
<Файл>
</Файл>"""

JMS_HEADER_TYPE = "SYS.AQ$_JMS_HEADER"
JMS_PROPS_TYPE = "SYS.AQ$_JMS_USERPROPARRAY"
JMS_PROPERTY = "SYS.AQ$_JMS_USERPROPERTY"

msg_type = connection.gettype('SYS.AQ$_JMS_BYTES_MESSAGE')
msg = msg_type.newobject()
msg.HEADER = connection.gettype(JMS_HEADER_TYPE).newobject()
msg.HEADER.PROPERTIES = connection.gettype(JMS_PROPS_TYPE).newobject()
jms_prop = connection.gettype(JMS_PROPERTY).newobject()
jms_prop.NAME = 'testName'
jms_prop.TYPE = 123
jms_prop.STR_VALUE = 'testValue'
jms_prop.NUM_VALUE = 12345
jms_prop.JAVA_TYPE = 1
msg.HEADER.TYPE = 'testHeaderType'
msg.HEADER.USERID = 'testHeaderUserId'
msg.HEADER.APPID = 'testHeaderAppId'
msg.HEADER.GROUPID = 'testHeaderGroupId'
msg.HEADER.GROUPSEQ = 12345

msg.HEADER.PROPERTIES.append(jms_prop)

msg.BYTES_LOB = xml
msg.BYTES_LEN = len(xml)

queue = connection.queue("TEST_INB", msg_type)
# queue = connection.queue("EDI_PARALEL_INB", msg_type)
queue.enqone(connection.msgproperties(payload=msg))
connection.commit()

