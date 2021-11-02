import utilities.utilities as ut
import pandas as pd
import os
from logs.logger import logger


def generateFolderStructureAndFiles(input,sheet, path_name):
    path = path_name
    try:
        excelFile = ut.readExcel(input)
    except Exception as err :
        logger.warn("File could not be loaded: {}".format(err))

    ohStreamSheet = pd.read_excel(excelFile,sheet)
    notifications = ohStreamSheet['topic'].unique()
    for notification in notifications:
        if notification != "-" and notification != 0 and not pd.isnull(notification):
            path = path + os.sep + notification
            if not os.path.exists(path):
                os.makedirs(path)
                print("Creating " +path)
                files=["r2r.musasabi","mee.musasabi"]
                for file in files:
                    with open(os.path.join(path, file), 'wb') as temp_file:
                        pass
                path = path_name