from dirChecker import chrckDirectoryAndSearchDate
from writeInBase import writeInBaseFromTransactions, writeInBaseFromPasportBlackList


def startupProgramm():
    dateForRead = chrckDirectoryAndSearchDate()
    for date in dateForRead:
        print("Читаем эксель")
        writeInBaseFromTransactions(date)
        writeInBaseFromPasportBlackList(date)