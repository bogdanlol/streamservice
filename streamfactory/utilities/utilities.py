import pandas as pd
from logs.logger import logger
# def get_paths(source):
#     paths = []
#     if isinstance(source, collections.MutableMapping):  # found a dict-like structure...
#         for k, v in source.items():  # iterate over it; Python 2.x: source.iteritems()
#             paths.append([k])  # add the current child path
#             paths += [[k] + x for x in get_paths(v)]  # get sub-paths, extend with the current
#     # else, check if a list-like structure, remove if you don't want list paths included
#     elif isinstance(source, collections.Sequence) and not isinstance(source, str):
#         #                          Python 2.x: use basestring instead of str ^
#         for i, v in enumerate(source):
#             paths.append([i])
#             paths += [[i] + x for x in get_paths(v)]  # get sub-paths, extend with the current
#     return paths


def readExcel(input):
    #reads and returns the input file given
    try:
        excelFile = pd.ExcelFile(input)
        return excelFile
    except Exception as err:
        logger.warning("Error while trying to load input file: {}".format(err))

def convertFolderName(str):
    #reads a topic and converts it to a internal topic name
    prefix = "internal"
    str = str.lower()
    suffix = "topic"
    return prefix + str + suffix

def stripPrefixAndSuffix(str):
    return str.replace("internal","").replace("topic","").replace("5","")