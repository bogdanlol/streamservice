import os
import utilities.structureGenerator as structureGenerator
import utilities.avrogen as avrogen
import utilities.downloadMusasabiJar as musasabi
import dsl.ruleset as ruleset
import dsl.createAvroVersions as avroVersions
import configparser
import dsl.topicsConf as tconf
import dsl.sdaConf as sdaconf
import dsl.secrets as secret
import dsl.protobuf as protobuf
import dsl.sdaRunner as sdaRunner
import dsl.compileConfig as compileConfig
import platform
import sys , getopt

def main(argv):
    # config = configparser.ConfigParser()
    # config.read("./config/config.ini")
    argumentList  = sys.argv[1:]
    options = "c:m:"
    long_options = ["config,mappingsheet"]
    config = configparser.ConfigParser()
    # mapping_sheet = ""
    mapping_sheet_new = ""
    try:

        arguments, values = getopt.getopt(argumentList, options, long_options)
        for currentArgument, currentValue in arguments:
            if currentArgument in("-c","--config"):
                config.read(currentValue)
                print(currentValue)
            elif currentArgument in ("-m","mappingsheet"):
                mapping_sheet_new = currentValue
                print(currentValue)
        
        sheet_name = config["Mapping"]["SheetName"]
        ruleset_path = config["Ruleset"]["RulesetPath"]
        avro_path = config["Ruleset"]["AvroPath"]
        inputVersion = config["Versions"]["input"]
        outputVersion = config["Versions"]["output"]
        avroVersionsPath = ruleset_path + os.sep + config["Ruleset"]["AvroVersions"]
        schemaRegistryUrl = config["Url"]["SchemaRegistry"]
        proto_path = config["Protobuf"]["ProtoPath"]
        topicsConf = config["Topics"]["TopicsPath"]
        global_path = config["Global"]["Path"]
        jobsettings_path = config["JobSettings"]["Path"]
        secrets_path = config["Secrets"]["Path"]
        secrets_filename = config["Secrets"]["Filename"]
        bootstrapServers = config["Url"]["BootstrapServers"]
        musasabiVersion = config["Musasabi"]["Version"]
        username = config["Musasabi"]["Username"]
        password = config["Musasabi"]["Password"]
        sshUsername = config["SSH"]["Username"]
        sshPassword = config["SSH"]["Password"]
        dsl_compiler_path = config["CompileConfig"]["Path"]
        dsl_compiler_file = config["CompileConfig"]["Filename"]
        out_topic_prefix = config["Outputtopics"]["prefix"]
        protoc = config["Protobuf"][platform.system()]
        sdaBussinesFields = config["Protobuf"]["SdaBussinesFields"]
        sdaMessageTracking = config["Protobuf"]["SdaMessageTracking"]
        sdaBussinesFields = eval(sdaBussinesFields)
        sdaMessageTracking = eval(sdaMessageTracking)


        structureGenerator.generateFolderStructureAndFiles(mapping_sheet_new,sheet_name,ruleset_path)
        ruleset.generate_r2r(config)
        ruleset.generate_mee(config)
        avrogen.getInputSchema(schemaRegistryUrl, ruleset_path, avro_path, inputVersion)
        avrogen.executeOutputSchema(mapping_sheet_new, avro_path, outputVersion, out_topic_prefix, sheet_name)
        avroVersions.executeAvroVersions(avro_path,avroVersionsPath,sshUsername,sshPassword)
        protobuf.createProtobuf(proto_path, mapping_sheet_new,sheet_name,sdaBussinesFields,sdaMessageTracking)
        protobuf.compileProto(proto_path,protoc)
        tconf.executeTopicsConf(proto_path,avro_path,topicsConf,ruleset_path,out_topic_prefix)
        sdaconf.executesdaConf(avro_path, jobsettings_path, global_path, bootstrapServers, schemaRegistryUrl, out_topic_prefix)
        secret.executeSecretsCreation(ruleset_path,secrets_path,secrets_filename)
        sdaRunner.createSdaRunners(global_path, musasabiVersion)
        compileConfig.createCompileConfig(dsl_compiler_path,dsl_compiler_file)
        #musasabi.downloadMusasabiJar(musasabiVersion, username, password, global_path)
    except getopt.error as err:
    # output error, and return with an error code
        print (str(err))


if __name__ == "__main__":
    main(sys.argv[1:])