import configparser
import sys , getopt
import os
import json
from utilities import utilities 
def main(rs,connector_class,file_path,connectors_path,schema_registry,input_key,ingest_offset,input_version,input_r2r_path,argv): 
    argumentList  = sys.argv[1:]
    options = "i:s:"
    long_options = ["input,secret"]
    all_topics = []
    try:
        arguments, values = getopt.getopt(argumentList, options, long_options)

        for currentArgument, currentValue in arguments:

            #if a secret is passed it means the connector script will generate the connector classes for every topic that is currently resided in the project
            #the connectors can then be ran from f55 and generate messages (please generate into the same folder)
            if currentArgument in ("-s", "--secret"):
                for root, dirs, files in os.walk(rs, topdown=False):
                    for topic in dirs:
                        all_topics.append(topic)
                for topic in all_topics:

                    connector = {"name" : "local-file-sink-"+utilities.stripPrefixAndSuffix(topic)+"-dlf",
                        "config":{
                            "connector.class" : connector_class,
                            "tasks.max" : "1",
                            "file" : file_path,
                            "file.pattern" : "'.'yyyy-MM-dd-HH-mm",
                            "topics": topic,
                            "value.converter": "io.confluent.connect.avro.IngAvroConverter",
                            "value.converter.value.ing.serde.avro.sharedsecret": currentValue,
                            "value.converter.schema.registry.url": schema_registry
                        }
                    }
                    connectorJson = json.dumps(connector, indent=4)
                    newConnector = open(connectors_path+os.sep+"file-sink-"+topic+".json",'w')
                    print(connectorJson,file=newConnector)
                    newConnector.close()

            #input is the folder in which the message files from f55 will be placed
            #the method converts the messages into jsonl that can be read by sda
            #then placed in the folder of r2r input
            elif currentArgument in ('-i',"--input"):
                for root, dirs, files in os.walk(currentValue, topdown=False):
                    for name in files:
                        with open(os.path.join(root, name),'r') as f:
                            try: 
                                messages = f.readlines()
                            except Exception as err :
                                print ("Cannot read messages from file : "+ name + str(err))
                            #try to create r2r input data if not existent
                            if not os.path.exists(input_r2r_path):
                                os.makedirs(input_r2r_path)

                            for message in messages:
                                header = '{topic:"'+ name +'","version":"'+input_version+'",ingestOffset":'+ingest_offset+',"key":"'+input_key+'","payload":'
                                inputMessage = header + message.rstrip() + "}"
                                with open(os.path.join(input_r2r_path,name+".jsonl"),'a+') as newInput: # we use a+ in order to append or create file if not existent
                                    newInput.write(inputMessage)
                                    newInput.write("\n")



    except getopt.error as err:
    # output error, and return with an error code
        print (str(err))


    


if __name__ =="__main__":

    config = configparser.ConfigParser()
    config.read("./config/config.ini")
    ruleset_path =  config["Ruleset"]["RulesetPath"]
    connector_class = config["Connectors"]["ConnectorClass"]
    file_path = config["Connectors"]["FilePath"]
    connectors_path = config["Connectors"]["ConnectorsPath"]
    schema_registry_url = config["Url"]["SchemaRegistry"]
    dct = eval(    schema_registry_url)
    schema_registry = dct['tst']
    input_key = config["Connectors"]["InputKey"]
    ingest_offset = config["Connectors"]["IngestOffset"]
    input_version = config["Versions"]["Input"]
    input_r2r_path = config["DataFiles"]["r2rInputPath"]
    main(ruleset_path,connector_class,file_path,connectors_path,schema_registry,input_key,ingest_offset,input_version,input_r2r_path,sys.argv[1:])