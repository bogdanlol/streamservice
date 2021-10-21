import os
import json
from logs.logger import logger


def getSchemaRootMessage(avroPath, avroVer, sch):
    avroSch = avroPath+os.sep+sch+os.sep+avroVer+os.sep+sch+'.avsc'

    try:
        with open(avroSch, "r") as avroFile:
            jsonFile = json.load(avroFile)
            schemaRootMessage = jsonFile["namespace"]+'.'+jsonFile["name"]
            eventName = str(jsonFile["namespace"]).split(".")[-1]
            return schemaRootMessage, eventName
    except Exception as err:
        logger.warning("Error in getting the schema root message: {}".format(err))

def getSecretName(rulesetPath):
    dctEv = {}

    for topic in os.listdir(rulesetPath):
        if not os.path.isdir(rulesetPath+os.sep+topic):
            continue
        with open(rulesetPath+os.sep+topic+os.sep+"r2r.musasabi", "r") as rulesetFile:
            for line in rulesetFile:
                if "event " in line:
                    evList = line.replace("{", "").replace("}", "").replace("\n", "").split(" ")
                    evName = evList[evList.index("event") + 1]
                    dctEv[evName.lower()] = topic
    return dctEv


def createTopicsConf(header, struct, topicsFile):
    if not os.path.exists(os.path.dirname(topicsFile)):
        try:
            os.makedirs(os.path.dirname(topicsFile))
        except Exception as err:
            logger.warning("Can't write topics conf file: {}".format(err))
    with open(topicsFile, "w+") as topicsConfFile:
        topicsConfFile.write(header)
        topicsConfFile.write(struct)
        

def addTopicsConf(struct, topicsFile,):
    with open(topicsFile, "a") as topicsConfFile:
        topicsConfFile.write(struct)

def createDirStructure(path):
    if not os.path.exists(path):
        os.makedirs(path)

header = "include required(\"../global.conf\")\n" \
         "#Uncomment lines below for offline testing\n" \
         "#schema.registry.path = ${baseUri}/avro\n" \
         "#protobufs.path = ${baseUri}/protobuf\n\n" \
         "protobufs.path = ${configVar.binaryFilesLocation}/protobuf\n\n" \
         "topics {\n"
dlfTopicStruct = "\t{topic} {{\n" \
                 "\t\tC3.file = ${{protobufs.path}}/{topic}/{version}/{protofile}\n" \
                 "\t\tC3.secret = \"${{secretKey_topics_{topic}_C3_secret}}\"\n" \
                 "\t\teventTimeField = eventtime\n" \
                 "\t\teventTimeFormat = iso8601\n" \
                 "\t\tname = {topic}\n" \
                 "\t\tversion = \"{version}\"\n" \
                 "\t}}\n"
inputTopicStruct = "\t{topic} {{\n" \
                   "\t\tname = {topic}\n" \
                   "\t\teventTimeFormat = iso8601\n" \
                   "\t\tserializerType = \"AvroSerde\"\n" \
                   "\t\tschemaRootMessage = \"{rootMessage}\"\n" \
                   "\t\tsecret = \"${{secretKey_topics_{topic}_secret}}\"\n" \
                   "\t}}\n"
outputTopicStruct = "\t{topic} {{\n" \
                    "\t\tname = {topic}\n" \
                    "\t\tserializerType = \"AvroSerde\"\n" \
                    "\t\tschemaRootMessage = \"{rootMessage}\"\n" \
                    "\t\tsecret = \"${{secretKey_topics_{topicIn}_secret}}\"\n" \
                    "\t}}\n"
envSpecificTopicsConf = "# envrironment-specific topics settings\n\n" \
                        "include required(\"../topics.conf\")"
endTopic = "\n}"
env = ["dev", "tst", "acc", "prd"]


def executeTopicsConf(protoPath,avroPath,topicsConfPath,rulesetPath,outTopicPrefix):
    # Add the environment specific topics.conf and create folder structure
    for envi in env:
        createDirStructure(topicsConfPath + os.sep + envi)

        with open(topicsConfPath + os.sep + envi + os.sep + 'topics.conf', "w+") as envConf:
            print("Writing topics conf to "+ topicsConfPath + os.sep + envi + os.sep + 'topics.conf')
            envConf.write(envSpecificTopicsConf)

    # Add the protobuf topics
    if not os.path.exists(protoPath):
        os.makedirs(protoPath)

    for sch in os.listdir(protoPath):
        topicVersion = os.listdir(protoPath+os.sep+sch)[0]
        protoFile = [descFile for descFile in os.listdir(protoPath+os.sep+sch+os.sep+topicVersion) if descFile.endswith(".proto")][0].replace(".proto", ".desc")
        protoTopicStruct = dlfTopicStruct.format(topic = sch, version = topicVersion, protofile = protoFile)
        topicsConfFile = topicsConfPath+os.sep+'topics.conf'

        if not os.path.exists(topicsConfFile):
            createTopicsConf(header, protoTopicStruct, topicsConfFile)
        elif os.path.exists(topicsConfFile) and os.path.getsize(topicsConfFile) == 0:
            createTopicsConf(header, protoTopicStruct, topicsConfFile)
        else:
            addTopicsConf(protoTopicStruct, topicsConfFile)

    #Get the events dictionary for secrets
    dictSecrets = getSecretName(rulesetPath)

    # Add the avro topics
    for sch in sorted(os.listdir(avroPath), reverse=True):
        try:
            topicVersion = os.listdir(avroPath+ os.sep + sch)[0]
            rootMessage, eventName = getSchemaRootMessage(avroPath, topicVersion, sch)
            if sch.startswith("internal") or sch.startswith("itrca_chm"):
                topicStruct = inputTopicStruct.format(topic = sch, version = topicVersion, rootMessage=rootMessage)
            elif sch.startswith(outTopicPrefix):
                try:
                    topicIn = dictSecrets[eventName]
                except Exception as err:
                    logger.warning("Event name cannot be found in the secrets dict. Assigning a default value: {}".format(err))
                    topicIn = sch

                topicStruct = outputTopicStruct.format(topic=sch, version=topicVersion, rootMessage=rootMessage, topicIn=topicIn)

            if not os.path.exists(topicsConfFile):
                createTopicsConf(header, topicStruct, topicsConfFile)
            elif os.path.exists(topicsConfFile) and os.path.getsize(topicsConfFile) == 0:
                createTopicsConf(header, topicStruct, topicsConfFile)
            else:
                addTopicsConf(topicStruct, topicsConfFile)
        except Exception as err:
            logger.warning("Error while populating topics.conf: {}".format(err))
    
    with open(topicsConfFile,'a') as tpcConfFile:
        tpcConfFile.write(endTopic)
        tpcConfFile.close()