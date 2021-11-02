import os
import utilities.utilities as ut
import pandas as pd
from logs.logger import logger
import subprocess

header = "syntax = \"proto2\";"

dlffeedbacktopicMessage = "{header}\n\n" \
                          "message dlffeedbacktopic {{\n" \
                            "\toptional string eventtime = 1; // timestamp of the message causing the problem\n" \
                            "\toptional string topicname = 2;//  name of topic (nginxacceslog, mobile, business event, etc)\n" \
                            "\toptional string topicversion = 3;//  name of topic (nginxacceslog, mobile, business event, etc)\n" \
                            "\toptional string originalmessagepayload = 4;// dump of event causing the problem, one string, all values, in a map ?\n" \
                            "\toptional string severity = 5; // error, warning, (info ?)\n" \
                            "\toptional string message = 6; // error/warning message of the system (e.g. cannot calculate a/b, b is empty, event dumped)\n" \
                            "\toptional string jobname = 7;  // in which jobs this occured (PreProc, FexScoring)\n" \
                            "\toptional string operatorname = 8; // in what operator trhis occured\n" \
                            "\toptional string nodename  = 9; // optional node identifier\n" \
                          "}}"

dlfmanagestatetopicMessage =  "{header}\n\n" \
                              "message dlfmanagestatetopic {{\n" \
                                "\toptional string targetState = 1; // the type of state that should be stored here. Note that this should be either `lookup` or `historical data`.\n" \
                                "\toptional string eventTime = 2; // In iso8601 format including the timezone.\n" \
                                "\toptional string compoundKey = 3; // the key on which the data should be stored.\n" \
                                "\toptional string actions = 4; //A json array describing all the actions that need to be performed on the key/state_type combination.\n" \
                              "}}"

dlfscoringtopicMessage =  "{header}\n\n" \
                          "message dlfscoringtopic {{\n" \
                            "\toptional string eventtime = 1;\n" \
                            "\toptional string scoreResult = 2;  // the calculated model result\n" \
                            "\toptional string scoreModelType = 3; // the type of the model that was scored\n" \
                            "\toptional string scoreProbability = 4; // the probability of the model thas was scored\n" \
                            "\toptional string scoreCategoryValues = 5; // the category values of the model\n" \
                            "\toptional string scoreConfidence = 6; // the confidence of the score\n" \
                            "\toptional string scoreAffinity = 7; // the affinity of the score\n" \
                            "\toptional string scoreAffinityRanking = 8; // the affinity ranking of the score\n" \
                            "\toptional string scoreEntityAffinity = 9; // the entity affinity of the score\n" \
                            "\toptional string scoreEntityIdRanking = 10; // the entityIdRanking of the score\n" \
                            "\toptional string scoreDescription = 11; // the descriptiption of the score\n" \
                            "\toptional string scoreExplanation = 12; // the explanation of the score\n" \
                            "\toptional string beName = 13; // the name of the business event that triggered the model\n" \
                            "\toptional string beVersion = 14; // the version of the business event that triggered the model\n" \
                            "\toptional string modelName = 15; // the model name\n" \
                            "\toptional string modelVersion = 16; // the model name\n" \
                            "\toptional string businessEventPayload = 17; // original payload of the event.\n" \
                            "\toptional string featureSet = 18; // the calculated featureset for the model\n" \
                          "}}"

dlfintraappeventstopicMessage = "{header}\n\n" \
                                "message dlfintraappeventstopic {{\n" \
                                "{excelBody}" \
                                "}}"


def createProto(path, **kwargs):
    for arg in kwargs:
        prPath = path+os.sep+arg+os.sep+"1.0.0"

        if not os.path.exists(prPath):
            try:
                os.makedirs(prPath)
            except Exception as err:
                logger.warning("Can't write topics conf file ".format(err))

        protoFile = arg+".c3.proto"

        with open(prPath+os.sep+protoFile, "w+") as proto:
            print("Writing to "+ prPath+os.sep+protoFile)
            proto.write(kwargs[arg])
            

def returnExcelAttributes(input,sheet,sdaBussinesFields,sdaMessageTracking):
    try :
        excelFile = ut.readExcel(input)
    except Exception as err :
        logger.warn("File could not be loaded: {}".format(err))

    streamSheet = pd.read_excel(excelFile, sheet, usecols=["column_description", "table_column_type"])  # read a specific sheet

    k = 0
    varRows = ""
    uniqueArray = []
    for index, row in streamSheet.iterrows():
        if str(row["column_description"]) != "-" :
            desc = str(row["column_description"])
            if not uniqueArray:
                uniqueArray.append(desc)
            if desc in uniqueArray and len(uniqueArray)>1:
                continue
            else:
                uniqueArray.append(desc)
                k += 1
                dataType = "string" if ("VARCHAR2" in str(row["table_column_type"]) or "NVARCHAR2" in str(row["table_column_type"])) else "string" #default to string for now
                varRows += "\toptional " + dataType + " " + desc + " = " + str(k) + ";\n"

    
        else:
            continue
    if sdaBussinesFields:
        for field in sdaBussinesFields:
            k += 1
            varRows += "\toptional " + "string" + " " + field + " = " + str(k) + ";\n"

    if sdaMessageTracking: 
        for field in sdaMessageTracking:
            k += 1
            varRows += "\toptional " + "int64" + " " + field + " = " + str(k) + ";\n"

    return varRows


#main
def createProtobuf(protoPath, mappingSheet,sheet,sdaBussinesFields,sdaMessageTracking):
    excelBody = returnExcelAttributes(mappingSheet,sheet,sdaBussinesFields,sdaMessageTracking)
    createProto(protoPath, dlffeedbacktopic=dlffeedbacktopicMessage.format(header = header), dlfmanagestatetopic=dlfmanagestatetopicMessage.format(header = header), dlfscoringtopic=dlfscoringtopicMessage.format(header = header), dlfintraappeventstopic=dlfintraappeventstopicMessage.format(header = header, excelBody = excelBody))


def compileProto(protoPath,protoc):
    for root, dirs, files in os.walk(protoPath, topdown=False):
        for name in files:
            subprocess.run([protoc,os.path.join(root, name),"-o",os.path.join(root, name.replace("proto","desc"))])   
            print("Compiling " + os.path.join(root, name))