from datetime import time

import psycopg2

from createCursor import makeConnection


createReportTable = """CREATE TABLE IF NOT EXISTS DMDV_REP_FRAUD (
    trans_id VARCHAR ( 255 ), event_dt timestamp, passport VARCHAR ( 255 ), fio VARCHAR ( 255 ), 
    phone VARCHAR ( 255 ), event_type varchar ( 255 ), 
    report_dt timestamp without time zone DEFAULT CURRENT_TIMESTAMP(1)
);"""

# where dmdv_DWH_FACT_transactions.update_dt > now() - INTERVAL '24 HOURS')
inputeDateInReportTableSign1 = """
WITH bigdata AS (Select * from dmdv_DWH_FACT_transactions 
left join dmdv_DWH_FACT_cards on dmdv_DWH_FACT_cards.card_num = dmdv_DWH_FACT_transactions.card_num
left join dmdv_DWH_FACT_accounts on dmdv_DWH_FACT_cards.account_num = dmdv_DWH_FACT_accounts.account_num
left join dmdv_DWH_FACT_clients on dmdv_DWH_FACT_accounts.client = dmdv_DWH_FACT_clients.client_id
left join dmdv_DWH_FACT_terminals on dmdv_DWH_FACT_transactions.terminal = dmdv_DWH_FACT_terminals.terminal_id)

INSERT INTO DMDV_REP_FRAUD (trans_id, event_dt, passport, fio, phone, event_type) SELECT bigdata.trans_id, 
bigdata.trans_date as event_dt, bigdata.pasport_num as passport, CONCAT(bigdata.first_name, ' ', bigdata.last_name,
' ', bigdata.patrinymic) AS fio, bigdata.phone, '1' as event_type FROM bigdata
where bigdata.pasport_valid_to < bigdata.trans_date or (select pasport_num from dmdv_dwh_fact_pasport_blacklist where 
dmdv_dwh_fact_pasport_blacklist.pasport_num = bigdata.pasport_num) IS NOT NULL

"""

inputeDateInReportTableSign2 = """
WITH bigdata AS (Select * from dmdv_DWH_FACT_transactions 
left join dmdv_DWH_FACT_cards on dmdv_DWH_FACT_cards.card_num = dmdv_DWH_FACT_transactions.card_num
left join dmdv_DWH_FACT_accounts on dmdv_DWH_FACT_cards.account_num = dmdv_DWH_FACT_accounts.account_num
left join dmdv_DWH_FACT_clients on dmdv_DWH_FACT_accounts.client = dmdv_DWH_FACT_clients.client_id
left join dmdv_DWH_FACT_terminals on dmdv_DWH_FACT_transactions.terminal = dmdv_DWH_FACT_terminals.terminal_id)

INSERT INTO DMDV_REP_FRAUD (trans_id, event_dt, passport, fio, phone, event_type) SELECT bigdata.trans_id, 
bigdata.trans_date as event_dt, bigdata.pasport_num as passport, CONCAT(bigdata.first_name, ' ', bigdata.last_name,
' ', bigdata.patrinymic) AS fio, bigdata.phone, '2' as event_type FROM bigdata
where bigdata.valid_to < bigdata.trans_date
"""

inputeDateInReportTableSign3 = """
WITH sign3 as(
select bigdata2.*, CASE
WHEN bigdata2.terminal_only_city!= bigdata2.previous_city and extract(epoch from bigdata2.trans_date - bigdata2.previous_trans_date) / 60 < 60
THEN 3
ELSE 0
END 
AS event_type_tmp
from 
(select *, split_part(terminal_address, ',', 1) as terminal_only_city, LAG(terminal_city) OVER (partition BY client_id) 
previous_city, LAG(trans_date) OVER (partition BY client_id) previous_trans_date from (
Select * from dmdv_DWH_FACT_transactions 
left join dmdv_DWH_FACT_cards on dmdv_DWH_FACT_cards.card_num = dmdv_DWH_FACT_transactions.card_num
left join dmdv_DWH_FACT_accounts on dmdv_DWH_FACT_cards.account_num = dmdv_DWH_FACT_accounts.account_num
left join dmdv_DWH_FACT_clients on dmdv_DWH_FACT_accounts.client = dmdv_DWH_FACT_clients.client_id
left join dmdv_DWH_FACT_terminals on dmdv_DWH_FACT_transactions.terminal = dmdv_DWH_FACT_terminals.terminal_id
order by client_id, trans_date) as bigdata ) as bigdata2)

INSERT INTO DMDV_REP_FRAUD (trans_id, event_dt, passport, fio, phone, event_type) SELECT sign3.trans_id, 
sign3.trans_date as event_dt, sign3.pasport_num as passport, CONCAT(sign3.first_name, ' ', sign3.last_name,
' ', sign3.patrinymic) AS fio, sign3.phone, '3' as event_type FROM sign3
where sign3.event_type_tmp = '3'

"""

inputeDateInReportTableSign4_uniq_clientId = """
select distinct(client_id) from dmdv_DWH_FACT_transactions 
left join dmdv_DWH_FACT_cards on dmdv_DWH_FACT_cards.card_num = dmdv_DWH_FACT_transactions.card_num
left join dmdv_DWH_FACT_accounts on dmdv_DWH_FACT_cards.account_num = dmdv_DWH_FACT_accounts.account_num
left join dmdv_DWH_FACT_clients on dmdv_DWH_FACT_accounts.client = dmdv_DWH_FACT_clients.client_id
left join dmdv_DWH_FACT_terminals on dmdv_DWH_FACT_transactions.terminal = dmdv_DWH_FACT_terminals.terminal_id
order by client_id
"""



def inputdateFrom_DWH_FACT_Tables_SignOne_SignTwo():
    print("вставка признак 1")
    try:
        conn = makeConnection()
        cursor = conn.cursor()
        cursor.execute(inputeDateInReportTableSign1)
        conn.commit()
    except psycopg2.OperationalError as exc:
        print('failed input date  sign 1', exc)
    finally:
        conn.close()
    print("вставка признак 2")
    try:
        conn = makeConnection()
        cursor = conn.cursor()
        cursor.execute(inputeDateInReportTableSign2)
        conn.commit()
    except psycopg2.OperationalError as exc:
        print('failed input date  sign 2', exc)
    finally:
        conn.close()


def inputdateFrom_DWH_FACT_Table_froad_sign3():
    try:
        print("вставка признак 3")
        conn = makeConnection()
        cursor = conn.cursor()
        cursor.execute(inputeDateInReportTableSign3)
        conn.commit()
    except psycopg2.OperationalError as exc:
        print('failed input date sign 3', exc)
    finally:
        conn.close()


def inputdateFrom_DWH_FACT_Table_froad_sign4():
    badtransaction_id_arr=[]
    try:
        print("вставка признак 4")
        conn = makeConnection()
        cursor = conn.cursor()
        cursor.execute(inputeDateInReportTableSign4_uniq_clientId)
        uniq_client_id = cursor.fetchall()
        for each in uniq_client_id:
            inputeDateInReportTableSign4_bigdata = f"""
            select oper_result, amt, trans_id, client_id, trans_date, LAG(trans_date) OVER (partition BY client_id) 
            previous_trans_date from (Select * from dmdv_DWH_FACT_transactions 
            left join dmdv_DWH_FACT_cards on dmdv_DWH_FACT_cards.card_num = dmdv_DWH_FACT_transactions.card_num
            left join dmdv_DWH_FACT_accounts on dmdv_DWH_FACT_cards.account_num = dmdv_DWH_FACT_accounts.account_num
            left join dmdv_DWH_FACT_clients on dmdv_DWH_FACT_accounts.client = dmdv_DWH_FACT_clients.client_id
            left join dmdv_DWH_FACT_terminals on dmdv_DWH_FACT_transactions.terminal = 
            dmdv_DWH_FACT_terminals.terminal_id where client_id='{each[0]}' order by trans_date) as bigdata"""
            conn2 = makeConnection()
            cursor2 = conn2.cursor()
            cursor2.execute(inputeDateInReportTableSign4_bigdata)
            sign4_bigdate = cursor2.fetchall()
            tmpArr = check_trans_of_client_sign4(sign4_bigdate)
            if len(tmpArr)!=0:
                badtransaction_id_arr += tmpArr
        input_sign4(badtransaction_id_arr)
    except psycopg2.OperationalError as exc:
        print('failed input date', exc)
    finally:
        conn.close()


def check_trans_of_client_sign4(tuple_client):
    badtransaction_id_arr = []
    for number in range(2, len(tuple_client)):
        trans1 = tuple_client[number]
        trans2 = tuple_client[number-1]
        trans3 = tuple_client[number-2]
        checkTime = (trans1[4] - trans3[4]).seconds % 60 < 20
        checkSum = trans1[1] < trans2[1] < trans3[1]
        checkRedult = trans1[0] == 'Успешно'
        if checkTime and checkSum and checkRedult:
            badtransaction_id_arr.append(trans1[2])
    return badtransaction_id_arr


def input_sign4(badtransaction_id_arr):
    insertRowWithSign4 = """
      WITH bigdata AS (Select * from dmdv_DWH_FACT_transactions 
      left join dmdv_DWH_FACT_cards on dmdv_DWH_FACT_cards.card_num = dmdv_DWH_FACT_transactions.card_num
      left join dmdv_DWH_FACT_accounts on dmdv_DWH_FACT_cards.account_num = dmdv_DWH_FACT_accounts.account_num
      left join dmdv_DWH_FACT_clients on dmdv_DWH_FACT_accounts.client = dmdv_DWH_FACT_clients.client_id
      left join dmdv_DWH_FACT_terminals on dmdv_DWH_FACT_transactions.terminal = dmdv_DWH_FACT_terminals.terminal_id)
      INSERT INTO DMDV_REP_FRAUD (trans_id, event_dt, passport, fio, phone, event_type) SELECT bigdata.trans_id, 
      bigdata.trans_date as event_dt, bigdata.pasport_num as passport, CONCAT(bigdata.first_name, ' ', bigdata.last_name,
      ' ', bigdata.patrinymic) AS fio, bigdata.phone, '4' as event_type FROM bigdata
      where bigdata.trans_id IN {}""".format(tuple(badtransaction_id_arr))

    try:
        conn3 = makeConnection()
        cursor3 = conn3.cursor()
        cursor3.execute(insertRowWithSign4)
        conn3.commit()
    except psycopg2.OperationalError as exc:
        print('failed input date', exc)
    finally:
        conn3.close()