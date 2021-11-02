import utilities.utilities as ut
import pandas as pd
from logs.logger import logger

def returnEventsAndData(input):
    try :
        excelFile = ut.readExcel(input)
    except Exception as err :
        logger.warn("File could not be loaded: {}".format(err))
    ohStreamSheet = pd.read_excel(excelFile,"OH Stream < OnePAM") #read specific sheet
    ohStreamSheet['Table Description'] = ohStreamSheet['Table Description'].astype("|S") # return as string not as object

    events = ohStreamSheet['Table Description'].unique()
    
    eventsAndData = {"event":"", "fields":[]}

    for event in events:
    #excelFields = ohStreamSheet["Table Description"]
        excelFields = ohStreamSheet.loc[(ohStreamSheet['Table Description']==event) & (ohStreamSheet['Column Description']!='-') & (ohStreamSheet['Column Description']!='Job Run Date')]
        eventsAndData[event.decode('utf-8')] = excelFields['Column Description'] #decode because of faulty string return
       
    
    return eventsAndData

def IdentifyInjectionTarget(input,eventName):
    try :
        excelFile = ut.readExcel(input)
    except Exception as err :
        logger.warn("File could not be loaded: {}".format(err))
    ohStreamSheet = pd.read_excel(excelFile,"OH Stream < OnePAM")
    events = ohStreamSheet['Table Description'].unique()
    eventsAndNotification = {"event":"", "notification":""}
    for event in events:
        excelFields = ohStreamSheet.loc[(ohStreamSheet['Table Description']==event) & (ohStreamSheet['Column Description']!='-') & (ohStreamSheet['Column Description']!='-') & (ohStreamSheet['Column Description']!='Job Run Date')].replace(" ","")
        v = excelFields["OnePAM Notification Name"].unique()
        for i in v:
            eventsAndNotification[event] = i
       

    #print(eventsAndNotification)
    return eventsAndNotification.get(eventName)

def returnFieldsAndColums(input):
    try :
        excelFile = ut.readExcel(input)
    except Exception as err :
        logger.warn("File could not be loaded: {}".format(err))
    ohStreamSheet = pd.read_excel(excelFile,"OH Stream < OnePAM") #read specific sheet
    ohStreamSheet['Table Description'] = ohStreamSheet['Table Description'].astype("|S") # return as string not as object

    events = ohStreamSheet['Table Description'].unique()
    
    eventsAndData = {"event":"", "fields":[]}

    for event in events:
    #excelFields = ohStreamSheet["Table Description"]
        excelFields = ohStreamSheet.loc[(ohStreamSheet['Table Description']==event) & (ohStreamSheet['Column Description']!='-')  & (ohStreamSheet['OH_ONEPAM Table Name'].notnull()) & (ohStreamSheet['OH_ONEPAM Table Name']!='-') & (ohStreamSheet['Column Description']!='Job Run Date')]
        eventsAndData[event.decode('utf-8')] = excelFields['OH_ONEPAM Column Name'] +" := $"+excelFields['Column Description']  #decode because of faulty string return
       
    
    return eventsAndData
