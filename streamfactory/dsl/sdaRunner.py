from logs.logger import logger
import os

sdaSh = "#!/usr/bin/env bash\n\n" \
        "java -cp musasabi-offline-{}.jar nl.ing.musasabi.offline.SdaOffline --config ./sda.conf"

sdaCmd = "java -cp musasabi-offline-{}.jar nl.ing.musasabi.offline.SdaOffline --config ./sda.conf"

def createSdaRunners(path, version):
    try:
        with open(path+os.sep+"sda.sh", "w+", newline='\n') as sdaShFile, open(path+os.sep+"sda.cmd", "w+", newline='\r\n') as sdaCmdFile:
            print("Creating sda executables in main folder "+ path)
            sdaShFile.write(sdaSh.format(version))
            sdaCmdFile.write(sdaCmd.format(version))
    except Exception as err:
        logger.warning("Can't write sda SH/CMD file: {}".format(err))