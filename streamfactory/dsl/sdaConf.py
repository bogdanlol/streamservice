import os
import json
from logs.logger import logger 

def createDirStructure(path):
    if not os.path.exists(path):
        os.makedirs(path)

def createConf(path, **kwargs):
    if not os.path.exists(path):
        os.makedirs(path)
    for arg in kwargs:
        pathArg = path+os.sep+arg+".conf"
        with open(pathArg, "w+") as config:
            config.write(kwargs[arg])

def createGlobalConf(path, glConf):
    pathArg = path + os.sep+"global.conf"
    if not os.path.exists(path):
        os.makedirs(path)
    with open(pathArg, "w+") as pth:
        pth.write(glConf)


r2rGlobalConf = "# R2R job settings shared between environments\n\n" \
          "r2r.eventBus.input.clusters = \"tpa\"\n" \
          "r2r.eventBus.output.clusters = \"tpa\"\n\n" \
          "r2r {{\n" \
          "\tinput.topics = {inTopics}\n" \
          "\toutput.intraApplication.topic = dlfintraappeventstopic\n" \
          "\tlogging.topic = dlffeedbacktopic\n" \
          "\t}}\n\n" \
          "job.maxEventsWithoutWatermark = 1000\n" \
          "job.watermarkMaxOutOfOrderness = \"0 ms\"\n\n" \
          "eventBus.tpa.partitionIdleTimeout = \"500 ms\"\n" \
          "eventBus.tpa.kafka.max.poll.records = 1000"

r2rEnvConf =    "# envrironment-specific R2R job settings\n\n" \
                "include required(\"../../global.conf\") \n" \
                "include required(\"../r2r.conf\") \n" \
                "include required(\"job.conf\")\n" \
                "include required(\"eventbus.conf\") \n" \
                "include required(\"api.conf\")\n\n" \
                "configVar.job.id = \"R2R\"\n" \
                "configVar.env = \"{env}\" \n" \
                "configVar.binaryFilesLocation = \"http://flinkjob-dependencies.tp-1e0016-\"${{configVar.env}}\"/config\"\n" \
                "job.api.phoenix.dTab = \"/endpoint=>/$/inet/runtime-definitions-server.tp-1e0016-\"${{configVar.env}}\"/80\"\n\n" \
                "environment = ${{configVar.env}}"

meeGlobalConf = "# MEE job settings shared between environments\n\n" \
          "mee.eventBus.input.clusters = \"tpa\"\n" \
          "mee.eventBus.output.clusters = \"tpa\"\n\n" \
          "mee {{\n" \
          "\tinput.intraApplication.topic = dlfintraappeventstopic\n" \
          "\toutput.topics = {outTopics}\n" \
          "\tlogging.topic = dlffeedbacktopic\n" \
          "\t}}\n\n" \
          "job.maxEventsWithoutWatermark = 10\n" \
          "job.watermarkMaxOutOfOrderness = \"100 ms\"\n\n" \
          "eventBus.tpa.partitionIdleTimeout = \"100 ms\""

meeEnvConf =    "# envrironment-specific MEE job settings\n\n" \
                "include required(\"../../global.conf\") \n" \
                "include required(\"../mee.conf\") \n" \
                "include required(\"job.conf\")\n" \
                "include required(\"eventbus.conf\") \n" \
                "include required(\"api.conf\")\n\n" \
                "configVar.job.id = \"MEE\"\n" \
                "configVar.env = \"{env}\" \n" \
                "configVar.binaryFilesLocation = \"http://flinkjob-dependencies.tp-1e0017-\"${{configVar.env}}\"/config\"\n" \
                "job.api.phoenix.dTab = \"/endpoint=>/$/inet/runtime-definitions-server.tp-1e0017-\"${{configVar.env}}\"/80\"\n\n" \
                "environment = ${{configVar.env}}"

jobGlobalConf = "# job settings shared between environments\n\n" \
          "job.tenantId = ${configVar.tenantId}\n" \
          "job.name = ${configVar.tenantId}_${configVar.job.id}\n" \
          "job.maxParallelism = 1024\n" \
          "job.nrOfSlotsPerNodes = 1\n" \
          "job.autoWatermarkInterval = \"30 ms\"\n" \
          "job.watermarkPeriodicMode = true\n" \
          "job.timerResolution = \"1 ms\"\n" \
          "job.bufferTimeout = \"5 ms\"\n" \
          "job.enableObjectReuse = true\n" \
          "job.enableDetailedMetrics = true\n\n" \
          "#job.stateBackend.enableIncrementalCheckpointing = true\n" \
          "#job.stateBackend.numCheckpointRetained = 1\n" \
          "#job.stateBackend.type = \"filesystem\"\n\n" \
          "system.graphite.period = \"20 s\""

jobEnvConf =    "# envrironment-specific job settings\n\n" \
                "include required(\"../job.conf\") \n" \
                "include required(\"../../topics/{env}/topics.conf\")\n\n" \
                "job.checkpointTimeout = \"10 minutes\"\n" \
                "job.checkpointingMode = \"AT_LEAST_ONCE\" \n" \
                "job.enableCheckpointing = \"15 minutes\"\n" \
                "job.minDurationBetweenCheckpoints = \"10 minutes\"\n\n" \
                "system.graphite.host = \"graphite-ohm.europe.intranet\" \n" \
                "system.graphite.port = 2003 \n" \
                "system.graphite.prefix = streamingdataanalyticsengine.${{configVar.env}}.ichp.${{consoleVar.datacenter}}.host.taskexecutor.${{configVar.tenantId}}_${{configVar.job.id}}"

eventbusGlobalConf =  "# eventbus settings shared between environments \n\n" \
                "eventBus.tpa.consumerOffsetMode = \"group\" \n" \
                "eventBus.tpa.kafka.client.id = ${configVar.tenantId}-${configVar.job.id}\n" \
                "eventBus.tpa.kafka.group.id = ${configVar.tenantId}-${configVar.job.id}-${configVar.env}-consumergroup\n" \
                "eventBus.tpa.kafka.auto.commit.enable = true\n" \
                "eventBus.tpa.kafka.auto.commit.interval.ms = 60000\n" \
                "eventBus.tpa.kafka.flink.poll-timeout = 10\n" \
                "eventBus.tpa.kafka.flink.disable-metrics = false\n" \
                "eventBus.tpa.kafka.flink.progression-based-last-handover-timestamp = true \n\n" \
                "eventBus.tpa.kafka.sda.phase = \"premigration\" \n" \
                "eventBus.tpa.kafka.slotRange = \"0%-100%\"\n" \
                "eventBus.tpa.messageMaxBytesCutoff = \"750000\" \n" \
                "eventBus.tpa.messageMaxBytesWarning = \"600000\"\n\n" \
                "eventBus.tpa.kafka.security.protocol = \"SSL\"\n" \
                "eventBus.tpa.kafka.ssl.enabled.protocols = \"TLSv1.2\"\n" \
                "eventBus.tpa.kafka.ssl.key.password = \"${secretKey_eventBus_tpa_kafka_ssl_key_password}\"\n" \
                "eventBus.tpa.kafka.ssl.keystore.location = \"/flink/config/keystore/keystore.jks\" # TODO: need to generate a new server-independent keystore file for R2R and MEE\n" \
                "eventBus.tpa.kafka.ssl.keystore.password = \"${secretKey_eventBus_tpa_kafka_ssl_keystore_password}\"\n" \
                "eventBus.tpa.kafka.ssl.keystore.type = \"JKS\"\n" \
                "eventBus.tpa.kafka.ssl.truststore.location = \"/flink/config/truststore/truststore.jks\"\n" \
                "eventBus.tpa.kafka.ssl.truststore.password = \"${secretKey_eventBus_tpa_kafka_ssl_truststore_password}\" \n" \
                "eventBus.tpa.kafka.ssl.truststore.type = \"JKS\""

eventbusEnvConf =   "# envrironment-specific EVENTBUS settings\n\n" \
                    "include required(\"../eventbus.conf\")\n\n" \
                    "eventBus.tpa.kafka.bootstrap.servers = \"{bootstrapServers}\"\n" \
                    "schema.registry.url = \"{schemaRegistry}\""

apiGlobalConf =     "# API settings shared between environments \n\n" \
                    "job.api.clientLabel = ${configVar.tenantId}_${configVar.job.id}\n\n" \
                    "#job.api.domain = \"api.ing.com\"\n" \
                    "job.api.endpointPrefix= \"/streaming-data-analytics-management\"\n" \
                    "job.api.initialMinimalTimestamp = \"2020-03-01T00:00:00.000Z\" \n" \
                    "job.api.maxAgeInitialMinimalTimestamp = \"35 days\"\n" \
                    "job.api.maxResponseSize = 5242880\n" \
                    "job.api.phoenix.destinationPrefix = \"/endpoint\"\n" \
                    "job.api.phoenix.pollInterval = \"60000 ms\"\n" \
                    "job.api.requeryInterval = \"60000 ms\" \n" \
                    "job.api.timeouts.requestTimeout = \"10000 ms\" \n" \
                    "job.api.timeouts.totalTimeout = \"30000 ms\""

apiEnvConf = "include required(\"../api.conf\")"

globalConf =    "# Global settings for tenant\n" \
                "configVar.tenantId = \"DLF\"\n" \
                "configVar.protobufDir = \"protobuf\"\n\n" \
                "configVar.binaryFilesLocation = /config/ //this is the path where all the binary files will be located on the pod \n" \
                "//the following files should be delivered somehow to configVar.binaryFilesLocation on the pod:\n" \
                "//thisRepo/keystores/ENV/serverIndependentKeystoreFile_R2R.jks.base64 -> keystores/serverIndependentKeystoreFile_R2R.jks\n" \
                "//thisRepo/keystores/ENV/serverIndependentKeystoreFile_MEE.jks.base64 -> keystores/serverIndependentKeystoreFile_MEE.jks\n" \
                "//thisRepo/truststores/ENV/R2R.truststore.ENV.jks.base64 -> truststore/R2R.truststore.jks \n" \
                "//thisRepo/truststores/ENV/MEE.truststore.ENV.jks.base64 -> truststore/MEE.truststore.jks \n" \
                "//thisRepo/protobuf/*/*/*.desc -> protobuf/*/*/*.desc "

sdaConf =   "baseUri = ${{uri.workingDir}}\n\n" \
            "#Uncomment line below for offline testing\n" \
            "#system.folder.ruleset = ${{baseUri}}/ruleset_inactive\n" \
            "#Also copy the topics.avroVersions inside ruleset to ruleset_inactive in order for offline testing to work\n" \
            "system.folder.ruleset = ${{baseUri}}/ruleset\n" \
            "inputfiles.path = ${{baseUri}}/dataFiles/r2rInputData\n" \
            "outputfiles.path = ${{baseUri}}/dataFiles/sdaOutputData\n\n\n" \
            "offline {{\n" \
            "  \tshowProgress = true\n" \
            "  \toutput {{\n" \
            "    \t\tsuppressTrackingInfo = true\n" \
            "    \t\tdata.r2r.path = ${{outputfiles.path}}/data_r2r.jsonl\n" \
            "    \t\tdata.mee.path = ${{outputfiles.path}}/data_mee.jsonl\n" \
            "    \t\tlogging.r2r.path = ${{outputfiles.path}}/logging_r2r.jsonl\n" \
            "    \t\tlogging.mee.path = ${{outputfiles.path}}/logging_mee.jsonl\n" \
            "    \t\truntimeDefs.path = ${{outputfiles.path}}/rundefs.json\n" \
            "  \t}}\n" \
            "  \tinput.data = onepam\n" \
            "  \tdata {{\n" \
            "    \t\ttestSet1 {{\n" \
            "      \t\t\tformat = kafkaproducer\n" \
            "      \t\t\tpaths = ${{inputfiles.path}}/testSet1.jsonl\n" \
            "    \t\t}}\n" \
            "  \t}}\n" \
            "  \tdata {{\n" \
            "    \t\tonepam {{\n" \
            "      \t\t\tformat = kafkaproducer\n" \
            "      \t\t\tpaths = {dataInPaths}\n" \
            "    \t\t}}\n" \
            "  \t}}\n" \
            "}}\n\n" \
            "r2r {{\n" \
            "\tinput.topics = {inTopics}\n" \
            "\toutput.intraApplication.topic = dlfintraappeventstopic\n" \
            "\tlogging.topic = dlffeedbacktopic\n" \
            "\t}}\n\n" \
            "mee {{\n" \
            "\tinput.intraApplication.topic = dlfintraappeventstopic\n" \
            "\toutput.topics = {outTopics}\n" \
            "\tlogging.topic = dlffeedbacktopic\n" \
            "\t}}\n\n" \
            "include \"topics/topics.conf\""

def executesdaConf(avro_path, jobsettings_path, global_path, dictBootstrapServers, dictSchemaRegistry, outTopicPrefix):
    inTopics = ";".join([topic for topic in os.listdir(avro_path) if topic.startswith("internal") or topic.startswith("itrca_chm")])
    dataInPaths = ";".join(["${inputfiles.path}/"+topic+".jsonl" for topic in os.listdir(avro_path) if topic.startswith("internal") or topic.startswith("itrca_chm")])
    outTopics = ";".join([topic for topic in os.listdir(avro_path) if topic.startswith(outTopicPrefix)])

    env = ["global", "dev", "tst", "acc", "prd"]

    dictBootstrapServers = json.loads(dictBootstrapServers)
    dictSchemaRegistry = json.loads(dictSchemaRegistry)

    #Create global.conf
    createGlobalConf(global_path, globalConf)

    #Create sda.conf
    createConf(global_path, sda=sdaConf.format(inTopics=inTopics, outTopics=outTopics, dataInPaths=dataInPaths))
    print("Writing sda.conf...")
    for envi in env:
        if envi == "global":
            jsettPath = jobsettings_path
            try:                    
                createDirStructure(jsettPath)
                createConf(jsettPath, r2r=r2rGlobalConf.format(inTopics = inTopics), mee=meeGlobalConf.format(outTopics = outTopics), job=jobGlobalConf, eventbus=eventbusGlobalConf, api=apiGlobalConf)
            except Exception as err :
                logger.warn("Couldnt write to global jobconfig: {}".format(err))
        else:
            jsettPath = jobsettings_path+os.sep+envi

            try:
                createDirStructure(jsettPath)
                createConf(jsettPath, r2r=r2rEnvConf.format(env = envi), mee=meeEnvConf.format(env = envi), job=jobEnvConf.format(env = envi), eventbus=eventbusEnvConf.format(bootstrapServers = dictBootstrapServers[envi], schemaRegistry = dictSchemaRegistry[envi]), api=apiEnvConf)
            except Exception as err :
                logger.warn("Couldnt write to env {} jobconfig: {}".format(envi, err))