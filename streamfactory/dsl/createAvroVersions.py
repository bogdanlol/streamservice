import requests
import json
import os
import paramiko
import time
from logs.logger import logger

def createAvroVersions(sch, topicVersion, avro_path):
    with open(avro_path, "w+") as avroVersionsFile:
        lstElem = [{"name": sch, "environments": [{"environment": "local", "versions": [{"version": "{}".format(topicVersion), "id": 1}]},
                                                  {"environment": "dev", "versions": [{"version": "{}".format(topicVersion), "id": 1}]},
                                                  {"environment": "tst", "versions": [{"version": "{}".format(topicVersion), "id": 1}]},
                                                  {"environment": "acc", "versions": [{"version": "{}".format(topicVersion), "id": 1}]},
                                                  {"environment": "prd", "versions": [{"version": "{}".format(topicVersion), "id": 1}]}]}]
        mainDict = {"topics": lstElem}

        json.dump(mainDict, avroVersionsFile, indent=2)


def addAvroVersions(sch, topicVersion, avro_path):
    with open(avro_path, "r+") as avroVersionsFile:
        jsonDict = json.load(avroVersionsFile)

        dictElem = {"name": sch, "environments": [{"environment": "local", "versions": [{"version": "{}".format(topicVersion), "id": 1}]},
                                                  {"environment": "dev", "versions": [{"version": "{}".format(topicVersion), "id": 1}]},
                                                  {"environment": "tst", "versions": [{"version": "{}".format(topicVersion), "id": 1}]}]}
                                                #   {"environment": "acc", "versions": [{"version": "{}".format(topicVersion), "id": 1}]},
                                                #   {"environment": "prd", "versions": [{"version": "{}".format(topicVersion), "id": 1}]}]}
        jsonDict["topics"].append(dictElem)

        avroVersionsFile.seek(0)
        avroVersionsFile.truncate()
        json.dump(jsonDict, avroVersionsFile, indent=2)


def updateAvroVersions(sch, env, schid, avroVerPath):
    with open(avroVerPath, "r+") as avroVersionsFile:
        jsonDict = json.load(avroVersionsFile)

        for topic in jsonDict["topics"]:
            if topic["name"] == sch:
                for envi in topic["environments"]:
                    if envi["environment"] == env:
                        envi["versions"][0]["id"] = schid

        avroVersionsFile.seek(0)
        avroVersionsFile.truncate()
        json.dump(jsonDict, avroVersionsFile, indent=2)


def curlGet(url):
    r = requests.get(url)
    r.close()

    return r.json()['id']

def curlSSH(url,sshUsername,sshPassword):
    client = paramiko.SSHClient()
    client.load_system_host_keys()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    username = sshUsername
    password = sshPassword

    try:
        client.connect('clrv0000202739.ic.ing.net', 22, username=username, password=password)
        cmd = "curl -X GET {}".format(url)
        stdin, stdout, stderr = client.exec_command(cmd)
        time.sleep(1)
        res = json.loads(stdout.read().decode("utf8"))

        client.close()

        # Print output of command. Will wait for command to finish.
        return res["id"]
    except Exception as err:
        logger.warning("Error while establishing connection to clrv0000202739.ic.ing.net: {}".format(err))
        client.close()


def executeAvroVersions(avroPath,avroVerPath,sshUsername,sshPassword):

    # envList = ["dev", "tst", "acc", "prd"]
    envList = ["dev", "tst"]

    # Create the initial topics.avroVersions file
    for sch in os.listdir(avroPath):
        topicVersion = os.listdir(os.path.join(avroPath,sch))[0]

        if not os.path.exists(avroVerPath):
            createAvroVersions(sch, topicVersion, avroVerPath)
        elif os.path.exists(avroVerPath) and os.path.getsize(avroVerPath) == 0:
            createAvroVersions(sch, topicVersion, avroVerPath)
        else:
            addAvroVersions(sch, topicVersion, avroVerPath)


    #Update all the schema ids
    for sch in os.listdir(avroPath):
        for env in envList:
            if env in ["dev", "tst"]:
                get_url = 'http://kafka-{}-0.europe.intranet:8081/subjects/{}-value/versions/latest'.format(env, sch)
                try:
                    schid = curlGet(get_url)
                    print("Writing avro schema for env:"+env + " for topic " + sch)
                    updateAvroVersions(sch, env, schid, avroVerPath)
                except Exception as err:
                    logger.warn("Could not connect to url: {}, schema: {}, {}".format(get_url, sch, err))
            # elif env in ["acc", "prd"]:
            #     if env == "acc":
            #         get_url = 'http://kafka-{}-1.europe.intranet:8081/subjects/{}-value/versions/latest'.format(env, sch)
            #     elif env == "prd":
            #         get_url = 'http://kafka-wpr-1.europe.intranet:8081/subjects/{}-value/versions/latest'.format(sch)

            #     try:
            #         schid = curlSSH(get_url,sshUsername,sshPassword)
            #         print("Writing avro schema for env:"+env + " for topic " + sch)
            #         updateAvroVersions(sch, env, schid, avroVerPath)
            #     except Exception as err:
            #         logger.warn("Could not connect to url: {}, schema: {}, {}".format(get_url, sch, err))

    #Update a specific schema id
    # sch = ""
    # env = ""
    #
    # print("\nUpdating schema --> {} in env:".format(sch))
    #
    # get_url = 'http://kafka-{}-0.europe.intranet:8081/subjects/{}-value/versions/latest'.format(env, sch)
    # schid = curlGet(get_url)
    #
    # updateAvroVersions(sch, env, schid, avroVerPath)