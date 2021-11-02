import os
import requests
import urllib3
from logs.logger import logger


def downloadMusasabiJar(version, username, password, global_path):
    url = f'https://artifactory.ing.net:443/artifactory/releases_mvn_SDP_public/nl/ing/musasabi/musasabi-offline/{version}/musasabi-offline-{version}.jar'
    print("Downloading musasasabi jar for offline testing version : "+version)
    try:
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

        r = requests.get(url, auth=(username, password), verify=False)
        path = global_path + os.sep + r.headers['X-Artifactory-Filename']
        open(path, 'wb').write(r.content)
        return r.status_code
    except Exception as err:
        logger.warning("Error while fetching url {} : {}".format(url, err))