import psycopg2
from createCursor import makeConnection
from readXLSX import readAndRenameFileToDate
import datetime
from datetime import datetime
from decimal import Decimal


def writeInBaseFromTransactions(date=None):
    if date is None:
        date = datetime.datetime.now().strftime("%d-%m-%Y")
    date_from_file_to_date_or_today = readAndRenameFileToDate(date, 1)

    try:
        connection = makeConnection()
        cursor = connection.cursor()
        print(date_from_file_to_date_or_today)
        if date_from_file_to_date_or_today is not None:
            for index, row in date_from_file_to_date_or_today.iterrows():
                cursor.execute("""INSERT INTO dmdv_DWH_FACT_transactions (trans_id, trans_date, card_num, oper_type, 
                amt, oper_result, terminal) VALUES (%s, %s, %s, %s, %s, %s, %s) ON CONFLICT (trans_id) DO UPDATE SET update_dt=CURRENT_TIMESTAMP;""",
                (row['trans_id'], row['date'], row['card'], row['oper_type'], Decimal(row['amount']), row['oper_result'], row['terminal']))
                connection.commit()
    except psycopg2.OperationalError as exc:
        print('failed INSERT into dmdv_DWH_FACT_transactions', exc)
    finally:
        connection.close()

    try:
        connection = makeConnection()
        cursor = connection.cursor()
        if date_from_file_to_date_or_today is not None:
            for index, row in date_from_file_to_date_or_today.iterrows():
                cursor.execute("""INSERT INTO dmdv_DWH_FACT_terminals (	terminal_id, terminal_type, terminal_city,
                terminal_address) VALUES (%s, %s, %s, %s) ON CONFLICT (terminal_id) DO UPDATE SET update_dt=CURRENT_TIMESTAMP;""", (row['terminal'], row['terminal_type'], row['city'], row['address']))
                connection.commit()
    except psycopg2.OperationalError as exc:
        print('failed INSERT into dmdv_DWH_FACT_terminals', exc)
    finally:
        connection.close()

    try:
        connection = makeConnection()
        cursor = connection.cursor()
        if date_from_file_to_date_or_today is not None:
            for index, row in date_from_file_to_date_or_today.iterrows():
                cursor.execute("""INSERT INTO dmdv_DWH_FACT_cards (card_num, account_num) VALUES (%s, %s) ON CONFLICT (card_num) DO UPDATE SET update_dt=CURRENT_TIMESTAMP;""",
                               (row['card'], row['account']))
                connection.commit()
    except psycopg2.OperationalError as exc:
        print('failed INSERT into dmdv_DWH_FACT_cards', exc)
    finally:
        connection.close()
    try:
        connection = makeConnection()
        cursor = connection.cursor()
        if date_from_file_to_date_or_today is not None:
            for index, row in date_from_file_to_date_or_today.iterrows():
                cursor.execute("""INSERT INTO dmdv_DWH_FACT_accounts (account_num, valid_to, client) 
                 VALUES (%s, %s, %s) ON CONFLICT (account_num) DO UPDATE SET update_dt=CURRENT_TIMESTAMP;""", (row['account'], row['account_valid_to'], row['client']))
                connection.commit()
    except psycopg2.OperationalError as exc:
        print('failed INSERT into dmdv_DWH_FACT_accounts', exc)
    finally:
        connection.close()
    try:
        connection = makeConnection()
        cursor = connection.cursor()
        if date_from_file_to_date_or_today is not None:
            for index, row in date_from_file_to_date_or_today.iterrows():
                cursor.execute("""INSERT INTO dmdv_DWH_FACT_clients (client_id, last_name, first_name, 
                 patrinymic, date_of_birth, pasport_num, pasport_valid_to, phone)      
                 VALUES (%s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (client_id) DO UPDATE SET update_dt=CURRENT_TIMESTAMP;""",
                               (row['client'], row['last_name'], row['first_name'], row['patronymic'],
                                row['date_of_birth'], row['passport'], row['passport_valid_to'], row['phone']))
                connection.commit()
    except psycopg2.OperationalError as exc:
        print('failed INSERT into dmdv_DWH_FACT_clients', exc)
    finally:
        connection.close()


def writeInBaseFromPasportBlackList(date=None):
    if date is None:
        date = datetime.datetime.now().strftime("%d-%m-%Y")
    date_from_file_to_date_or_today = readAndRenameFileToDate(date, 2)
    try:
        connection = makeConnection()
        cursor = connection.cursor()
        if date_from_file_to_date_or_today is not None:
            for index, row in date_from_file_to_date_or_today.iterrows():
                cursor.execute("""INSERT INTO dmdv_DWH_FACT_pasport_blacklist (pasport_num, entty_dt) VALUES (%s, %s);""",
                (row['passport'], row['start_dt']))
                connection.commit()
    except psycopg2.OperationalError as exc:
        print('failed INSERT into dmdv_DWH_FACT_transactions', exc)
    finally:
        connection.close()


def dropAllTable():
    try:
        connection = makeConnection()
        cursor = connection.cursor()
        cursor.execute("""DROP TABLE IF EXISTS  public.dmdv_dwh_fact_pasport_blacklist, public.dmdv_dwh_fact_clients, public.dmdv_dwh_fact_accounts, 
            public.dmdv_dwh_fact_cards, public.dmdv_dwh_fact_terminals, public.dmdv_dwh_fact_transactions, public.dmdv_rep_fraud""")
        connection.commit()
    except psycopg2.OperationalError as exc:
        print('failed drop', exc)
    finally:
        connection.close()
