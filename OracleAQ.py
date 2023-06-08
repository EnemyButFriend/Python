import oracledb
import config

xml = """<O_o?xml version="1.0" encoding="windows-1251"?>

<Файл ИдФайл="ON_NSCHFDOPPOK_2BEe50945a4d98c11e280bf005056917125_2BE22ff883da8d24b2e9c8fd2d213173370_20230602_A29F7964-80A3-BE10-E100-0000C0A8320D"

      ВерсФорм="5.01" ВерсПрог="1.0">

    <СвУчДокОбор ИдОтпр="2BE22ff883da8d24b2e9c8fd2d213173370" ИдПол="2BEe50945a4d98c11e280bf005056917125">

        <СвОЭДОтпр НаимОрг="#OPERATOR_NAME#" ИННЮЛ="#OPERATOR_INN#" ИдЭДО="#OPERATOR_ID#"></СвОЭДОтпр>

    </СвУчДокОбор>

    <ИнфПок КНД="1115132" ДатаИнфПок="02.06.2023" ВремИнфПок="16.50.48"

            НаимЭконСубСост="Акционерное общество &#34;Х5 СИНЕРГИЯ&#34;"

            ОснДоверОргСост="по доверенности №56702786/2021 от 01.11.2021">

        <ИдИнфПрод ИдФайлИнфПр="#ORIG_FILENAME#" ДатаФайлИнфПр="#ORIG_DATE#" ВремФайлИнфПр="#ORIG_TIME#">

            <ЭП>#ORIG_SIGNATURE#</ЭП>

        </ИдИнфПрод>

        <СодФХЖ4 НаимДокОпрПр="#ORIG_DOCNAME#" Функция="#ORIG_DOCTYPE_FULL#" НомСчФИнфПр="#ORIG_INVOICE_EDI#"

                 ДатаСчФИнфПр="#ORIG_POSTING_DATE#" ВидОперации="Услуга">

            <СвПрин СодОпер="Услуги оказаны в полном объеме" ДатаПрин="02.06.2023"></СвПрин>

        </СодФХЖ4>

        <Подписант ОблПолн="3" Статус="3" ОснПолн="по доверенности №57205920/2021 от 01.11.2021"

                   ОснПолнОрг="по доверенности №56702786/2021 от 01.11.2021">

            <ЮЛ ИННЮЛ="7728029110" Должн="Главный эксперт">

                <ФИО Фамилия="Широкова" Имя="Наталья" Отчество="Анатольевна"></ФИО>

            </ЮЛ>

        </Подписант>

    </ИнфПок>

</Файл>"""

oracledb.init_oracle_client(

    config.oracledbPath

)


def connect():
    dsn_tns = oracledb.makedsn(config.ip, config.port, service_name=config.service_name)
    connection = oracledb.connect(user=config.user, password=config.pswrd, dsn=dsn_tns)
    return connection


def commit(connection):
    connection.commit()


def queueing():
    connection = connect()
    JMS_HEADER_TYPE = config.JMS_HEADER_TYPE
    JMS_PROPS_TYPE = config.JMS_PROPS_TYPE
    JMS_PROPERTY = config.JMS_PROPERTY
    MSG_TYPE = config.MSG_TYPE
    msg_type = connection.gettype(MSG_TYPE)
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
    queue = connection.queue(config.queue_name, msg_type)
    queue.enqone(connection.msgproperties(payload=msg))
    commit(connection)


queueing()
