import pandas as pd
import os


def readXLSXFileWriteInDataBase(file_name, file_form):

    colNames =[]
    if int(file_form) == 1:
        colNames = ["trans_id", "date", "card", "account", "account_valid_to", "client", "last_name", "first_name", "patronymic", "date_of_birth", "passport",
                    "passport_valid_to", "phone", "oper_type", "amount", "oper_result", "terminal", "terminal_type", "city", "address"]

    elif int(file_form) == 2:
        colNames = ["passport", "start_dt"]
    else:
        print("fileForm is wrong by column")

    try:
        excel_data_df = pd.read_excel(file_name, names=colNames)
        try:
            os.rename(file_name, file_name + ".backup")
        except WindowsError:
            os.remove(file_name)
            os.rename(file_name, file_name + ".backup")
        return excel_data_df
    except FileNotFoundError as e:
        print("Excel file not found " + str(e))


def readAndRenameFileToDate(date, file_form):
    prefif = ""
    file_name_prefix_transaction = 'transactions_'
    file_name_prefix_passport = 'passports_blacklist_'
    file_type = '.xlsx'

    if int(file_form) == 1:
        prefif = file_name_prefix_transaction
    elif int(file_form) == 2:
        prefif = file_name_prefix_passport
    else:
        print("fileForm is wrong")

    return readXLSXFileWriteInDataBase(prefif + str(date) + file_type, file_form)

