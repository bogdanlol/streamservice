from logs.logger import logger
import os

def createSecretsPath(secrets_path):
    if not os.path.exists(os.path.dirname(secrets_path)):
        try:
            os.makedirs(os.path.dirname(secrets_path))
        except Exception as err: # Guard against race condition
            logger.warn("Can't write topics conf file ".format(err))

header = "apiVersion: v1\n" \
         "kind: Secret\n" \
         "metadata:\n" \
         "\tname: <<purposeID>>-secrets\n" \
         "\tnamespace: <<consumerNamespace>>-<<environment>>\n" \
         "type: Opaque \n" \
         "data:\n"

defaultSecrets = "\tsecretKey-eventBus-tpa-kafka-ssl-key-password: <<secretKey-eventBus-tpa-kafka-ssl-key-password>> \n" \
  "\tsecretKey-eventBus-tpa-kafka-ssl-keystore-password: <<secretKey-eventBus-tpa-kafka-ssl-keystore-password>> \n" \
  "\tsecretKey-eventBus-tpa-kafka-ssl-truststore-password: <<secretKey-eventBus-tpa-kafka-ssl-truststore-password>> \n" \
  "\tsecretKey-job-api-applicationCertificate-keystore-passphrase: <<secretKey-job-api-applicationCertificate-keystore-passphrase>> \n" \
  "\tsecretKey-job-api-applicationCertificate-privateKey-passphrase: <<secretKey-job-api-applicationCertificate-privateKey-passphrase>> \n" \
  "\tsecretKey-job-api-peerToken-jwt: <<secretKey-job-api-peerToken-jwt>> \n" \
  "\tsecretKey-job-api-peerToken-keystore-passphrase: <<secretKey-job-api-peerToken-keystore-passphrase>> \n" \
  "\tsecretKey-job-api-trustTokens-keystore-passphrase: <<secretKey-job-api-trustTokens-keystore-passphrase>> \n" \
  "\ttruststore.jks: <<secretKey-truststore-file>> \n" \
  "\tkeystore.jks: <<secretKey-keystore-file>> \n" \
  "\tpeertoken.jks: <<secretKey-peertoken-file>> \n" 


#read proto path once we start creating them and get the proto topics from there but for now we write the generic proto topics here
protoSecrets = "\tsecretKey-topics-dlfintraappeventstopic-C3-secret: <<secretKey-topics-dlfintraappeventstopic-C3-secret>> \n" \
  "\tsecretKey-topics-dlfmanagestatetopic-C3-secret: <<secretKey-topics-dlfmanagestatetopic-C3-secret>> \n" \
  "\tsecretKey-topics-dlfscoringtopic-C3-secret: <<secretKey-topics-dlfscoringtopic-C3-secret>> \n" \
  "\tsecretKey-topics-dlffeedbacktopic-C3-secret: <<secretKey-topics-dlffeedbacktopic-C3-secret>> \n"

def generateInputSecrets(ruleset_path):
    all_topics = []
    topicSecrets = ""
    for root, dirs, files in os.walk(ruleset_path, topdown=False):
        for topic in dirs:
            all_topics.append(topic)
    for topic in all_topics:
        topicSecrets += "\tsecretKey-topics-"+topic+"-secret: <<secretKey-topics-"+topic+"-secret>>\n"
    
    return topicSecrets


def executeSecretsCreation(ruleset_path,secrets_path,secrets_filename):
    createSecretsPath(secrets_path)
    secrets = header + defaultSecrets + protoSecrets + generateInputSecrets(ruleset_path)
    with open(os.path.join(secrets_path,secrets_filename), "w") as file:
        print("Writing to " + secrets_path)
        file.write(secrets)
        file.close() 
  
    