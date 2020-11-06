import os
import re


def chrckDirectoryAndSearchDate():
    dates_files_for_read = []
    for root, dirs, files in os.walk("."):
        for filename in files:
            if filename.endswith(".xlsx"):
                sppendinElement = re.findall('\d{8}', filename)[0]
                if sppendinElement not in dates_files_for_read:
                    dates_files_for_read.append(sppendinElement)

    return dates_files_for_read