import utilities.structureGenerator as structureGenerator
import utilities.utilities as ut
import configparser
import pandas as pd
import os
config = configparser.ConfigParser()
config.read("./config/config.ini")

#define which input gets tested input
mapping_sheet = config["Mapping"]["MappingSheetNew"]

sheet_name = config["Mapping"]["SheetName"]
ruleset_path = config["Ruleset"]["TestRulesetPath"]

def cleanup():
    #clean the test directory
    for root, dirs, files in os.walk(ruleset_path, topdown=False):
        for name in files:
            os.remove(os.path.join(root,name))
        for name in dirs:
            os.rmdir(os.path.join(root,name))

def test_generateFolderStructureAndFiles():
    #clean at startup 
    cleanup()

    #read input
    excelFile = ut.readExcel(mapping_sheet)

    ohStreamSheet = pd.read_excel(excelFile,sheet_name)

    actualFolders = []

    #input topics from mapping sheet
    folders = ohStreamSheet['topic'].unique()

    #make sure formatting doesn't affect folderNames
    for f in folders:
        if f != "-" and f != 0 and not pd.isnull(f):
            actualFolders.append(f)

    expectedFolders = []
    #expected filenames
    expectedFiles = ['r2r.musasabi','mee.musasabi']
    fileCount = 0

    #generate the folders
    structureGenerator.generateFolderStructureAndFiles(mapping_sheet,sheet_name,ruleset_path)


    #check generated folders
    for root, dirs, files in os.walk(ruleset_path, topdown=False):
        for name in dirs:
            #get the names of the folders
            expectedFolders.append(name)
        for name in files:
            #check if the names are correct and then count them
            if name in expectedFiles:
                fileCount +=1

    #check if the number of input topics from excel match the number of generated folders
    assert len(actualFolders) == len(expectedFolders)

    #check if the generated folders are actually in the excel file
    assert all([actualFolders == expectedFolders for actualFolders,expectedFolders in zip(actualFolders,actualFolders)])

    #check if the file count is correct (in this case two per folder)
    assert (len(actualFolders) *2) == fileCount
    
    
    #if everything is well, it means the structure and files are generated correctly
