import psycopg2

from createCursor import makeConnection
from createReportScripts import createReportTable

terminalsTable = """CREATE TABLE IF NOT EXISTS dmdv_DWH_FACT_terminals (
	terminal_id VARCHAR ( 255 ) UNIQUE,
    terminal_type VARCHAR ( 255 ),
	terminal_city VARCHAR ( 255 ),
	terminal_address VARCHAR ( 255 ),
    create_dt timestamp without time zone DEFAULT CURRENT_TIMESTAMP(1),
    update_dt timestamp without time zone DEFAULT CURRENT_TIMESTAMP(1)
);"""

pasportBlackListTable = """CREATE TABLE IF NOT EXISTS dmdv_dwh_fact_pasport_blacklist (
	pasport_num VARCHAR ( 255 ),
    entty_dt timestamp,
    create_dt timestamp without time zone DEFAULT CURRENT_TIMESTAMP(1),
    update_dt timestamp without time zone DEFAULT CURRENT_TIMESTAMP(1)
);"""

transactionsTable = """CREATE TABLE IF NOT EXISTS dmdv_DWH_FACT_transactions (
	trans_id VARCHAR ( 255 ) UNIQUE,
    trans_date timestamp,
    card_num VARCHAR ( 255 ),
    oper_type VARCHAR ( 255 ),
    amt numeric,
    oper_result VARCHAR ( 255 ),
    terminal VARCHAR ( 255 ),
    create_dt timestamp without time zone DEFAULT CURRENT_TIMESTAMP(1),
    update_dt timestamp without time zone DEFAULT CURRENT_TIMESTAMP(1)
);"""

cardsTable = """CREATE TABLE IF NOT EXISTS dmdv_DWH_FACT_cards (
	card_num VARCHAR ( 255 ) UNIQUE,
    account_num VARCHAR ( 255 ),
    create_dt timestamp without time zone DEFAULT CURRENT_TIMESTAMP(1),
    update_dt timestamp without time zone DEFAULT CURRENT_TIMESTAMP(1)
);"""

accountsTable = """CREATE TABLE IF NOT EXISTS dmdv_DWH_FACT_accounts (
	account_num VARCHAR ( 255 ) UNIQUE,
    valid_to timestamp,
    client VARCHAR ( 255 ),
    create_dt timestamp without time zone DEFAULT CURRENT_TIMESTAMP(1),
    update_dt timestamp without time zone DEFAULT CURRENT_TIMESTAMP(1)
);"""

clientsTable = """CREATE TABLE IF NOT EXISTS dmdv_DWH_FACT_clients (
	client_id VARCHAR ( 255 ) UNIQUE,
    last_name VARCHAR ( 255 ),
    first_name VARCHAR ( 255 ),
    patrinymic VARCHAR ( 255 ),
    date_of_birth timestamp,
    pasport_num VARCHAR ( 255 ),
    pasport_valid_to timestamp,
    phone VARCHAR ( 255 ),
    create_dt timestamp without time zone DEFAULT CURRENT_TIMESTAMP(1),
    update_dt timestamp without time zone DEFAULT CURRENT_TIMESTAMP(1),
    deleted_flg INTEGER DEFAULT NULL
);"""


def createAutuHist(sqlExpression):
    try:
        conn = makeConnection()
        cursor = conn.cursor()
        cursor.execute(sqlExpression)
        conn.commit()
    except psycopg2.OperationalError as exc:
        print('failed input date', exc)
    finally:
        conn.close()


def creteInputTable():
    createAutuHist(createReportTable)
    createAutuHist(terminalsTable)
    createAutuHist(transactionsTable)
    createAutuHist(cardsTable)
    createAutuHist(pasportBlackListTable)
    createAutuHist(accountsTable)
    createAutuHist(clientsTable)
