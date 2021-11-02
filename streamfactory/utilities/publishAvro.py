import os
import textwrap
import requests
import json
import configparser


def curl_put(url, payload):
    r = requests.put(url, json=payload)
    r.close()

    return r.status_code

def curl_post(url, payload, headers):
    r = requests.post(url, data=payload, headers=headers)
    r.close()

    return r.status_code


def curl_get(url):
    r = requests.get(url)
    r.close()

    return r.json()['id']



def main():
    config = configparser.ConfigParser()
    config.read("../config/config.ini")

    avro_path = "../sda/avro"
    outputVersion = config["Versions"]["output"]
    out_topic_prefix = config["Outputtopics"]["prefix"]


    avroFile = avro_path
    env = "dev"

    for sub in os.listdir(avroFile):
        if sub.startswith(out_topic_prefix):
            flnm = avroFile+os.sep+sub+os.sep+outputVersion+os.sep+sub+".avsc"
            print("Updating schema --> {}".format(sub))

            avroFl = open(flnm)
            json_string = json.load(avroFl)
            json_string_minify = json.dumps(json_string, separators=(',', ":")).replace('"', '\\"')  # Compact JSON structure
            payload = f"{{\"schema\":\"{json_string_minify}\"}}"
            post_url = 'http://kafka-{}-0.europe.intranet:8081/subjects/{}-value/versions'.format(env, sub)
            headers = {'Content-Type': 'application/vnd.schemaregistry.v1+json'}

            # print(json_string_minify)

            put_url = 'http://kafka-{}-0.europe.intranet:8081/config/{}-value'.format(env, sub)
            payload_put = {"compatibility": "NONE"}

            print("Updating Compatibility for schema --> {}".format(sub))
            ret_put = curl_put(put_url, payload_put)

            if ret_put == 200:
                print("Compatibility update successful. POSTing the schema.")
                ret = curl_post(post_url, payload, headers)

                if ret == 200:
                    print("Schema registered successfully")
                else:
                    print("Schema registration failed")

            else:
                print("Compatibility update error")



if __name__ == "__main__":
    main()