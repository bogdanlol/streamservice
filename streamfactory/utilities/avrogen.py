import os
import json
import requests
import pandas as pd
from logs.logger import logger


#define static structure#
lstTopc = [
           {"name": "TOPC", "type": "string", "doc": "SourceTopic"},
           {"name": "KAFKA_OFST", "type": "long", "doc": "SourceTopicOffset"},
           {"name": "KAFKA_PARTITION", "type": "long", "doc": "SourceTopicPartition"}
          ]
dictSkeleton = {
                "type":"record",
                "name": "",
                "namespace": "",
                "fields": []
                }


def getInputSchema(schemaRegistryUrl, rulesetPath, avroPath, version):
    schRegDict = json.loads(schemaRegistryUrl)
    schRegTstUrl = schRegDict["dev"]

    # sch = "internalnotifyoccupation5topic"
    for sch in os.listdir(rulesetPath):
        avroVerPath = avroPath+os.sep+sch+os.sep+version
        avroSchPath = avroVerPath+os.sep+sch+".avsc"
        getUrl = '{}/subjects/{}-value/versions/latest'.format(schRegTstUrl, sch)

        try:
            r = requests.get(getUrl)
            payload = r.json()["schema"]
            jsPayload = json.loads(payload)
            r.close()
        except Exception as err:
            logger.warning("Encountered an error while getting the schema for: {}. Error message: {}".format(getUrl, err))

        if not os.path.exists(avroVerPath):
            os.makedirs(avroVerPath)

        try:
            with open(avroSchPath, "w") as avroSchTst:
                json.dump(jsPayload, avroSchTst, indent=4)
        except:
            logger.warning("Error while writing schema {} to {}.avsc".format(sch, sch))


def getOutputSchemaAttr(inputExcel, sheetName):
    try :
        streamSheet = pd.read_excel(inputExcel, sheetName, dtype = object, keep_default_na=False
                                              , na_values=['', '#N/A', '#N/A N/A', '#NA', '-1.#IND', '-1.#QNAN', '-NaN', '-nan', '1.#IND', '1.#QNAN', '<NA>', 'N/A', 'NA', 'NaN', 'n/a', 'nan']
                                              , usecols=["event", "table_column_name", "column_description", "table_column_type", "table_column_nullable"])  # read a specific sheet
    except Exception as err :
        logger.warn("Excel file could not be loaded: {}".format(err))

    dictEvent = {}
    prevEventName = ""

    try:
        for index, row in streamSheet.iterrows():
            if str(row["table_column_name"]) != "-":
                eventName = row["event"]
                columnName = row["table_column_name"]
                doc = row["column_description"]
                dataType = "string" #default to sring for now (rulest gen doesn't support long yet)
                if row["table_column_nullable"] == "NULL":
                    type = ("null",dataType)
                    dType = list(type)
                else:
                    dType = dataType
            else:
                continue

            if "null" in dType:
                dictAttr = {"name": columnName, "type": dType, "doc": doc, "default": None}
            else:
                dictAttr = {"name": columnName, "type": dType, "doc": doc}

            if prevEventName != eventName:
                dictEvent[eventName] = [dictAttr]
                prevEventName = eventName
            else:
                dictEvent[eventName].append(dictAttr)
    except Exception as err:
        logger.warning("Error while retrieving the events attributes from the excel file {}: {}".format(inputExcel ,err))
    finally:
        return dictEvent


def createOutputSchema(dictEvent, dictSkeleton, lstTopc, avroPath, version, prefix):
    for item in dictEvent:
        name = (prefix).capitalize()+item+"Event"
        namespace = "com.ing.dlf.model."+prefix+"."+(item).lower()
        fields = dictEvent[item] + lstTopc

        dictAvro = dictSkeleton
        dictAvro["name"] = name
        dictAvro["namespace"] = namespace
        dictAvro["fields"] = fields

        topicName = prefix+(item).lower()+"topic"
        avroVerPath = avroPath+os.sep+topicName+os.sep+version
        avroSchPath = avroVerPath+os.sep+topicName+".avsc"

        if not os.path.exists(avroVerPath):
            os.makedirs(avroVerPath)

        try:
            with open(avroSchPath, "w") as avroFile:
                json.dump(dictAvro, avroFile, indent=4)
        except Exception as err:
            logger.warning("Failed writing the json structure to the avro output file {}: {}".format((item).lower(), err))


def executeOutputSchema(mappingSheet, avroPath, version, prefix, sheetName):
    try:
        dictEvent = getOutputSchemaAttr(mappingSheet, sheetName)
        createOutputSchema(dictEvent, dictSkeleton, lstTopc, avroPath, version, prefix)
    except Exception as err:
        logger.warning("Output schema creation failed: {}".format(err))

