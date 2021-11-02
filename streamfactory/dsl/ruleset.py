from utilities.dataExtractor import *
import os, errno
from logs.logger import logger

def write_dsl(dsl, output_file):
    '''
    Function to write DSL to musasabi file (R2R or MEE)
    '''
    if not os.path.exists(output_file):
        try:
            os.makedirs(os.path.dirname(output_file))
        except OSError as exc: # Guard against race condition
            if exc.errno != errno.EEXIST:
                raise

    topic = output_file.split(os.sep)[-2]
    dsl_type = output_file.split(os.sep)[-1].split('.')[0]
    event_name = dsl.split('publish')[1].split()[0]

    with open(output_file, 'a') as dsl_file:
        logger.info(f'Writing {dsl_type} DSL for {event_name} ({topic})')
        dsl_file.write(dsl)

def generate_r2r(config):
    input_file = "/tmp/sf/input.xls"
    output_folder_path = config["Ruleset"]["RulesetPath"]

    try :
        excelFile = pd.ExcelFile(input_file)
        input_data = pd.read_excel(excelFile, "OnePam - EDL new")
    except Exception as err :
        logger.error("File could not be loaded: {}".format(err))
        raise

    # initiate local variables
    indent = '    '
    root_path = ''
    preprocess_dsl = ''
    event_dsl = ''
    in_version = config['Versions']['input']
    out_version = config['Versions']['output']

    events_list = input_data['event'].unique()
    for event in events_list:
        '''
        For each event initiate new pre process dsl.
        '''
        event_data = input_data.loc[(input_data['event']==event)]

        # initiate event dsl
        event_dsl = f'event {event} version "{out_version}" {{\n'
        event_dsl += f'{indent}fields {{\n'

        # initiate preprocess dsl
        preprocess_dsl = f'preprocess preprocess_{event} version "{out_version}" {{\n{indent} event ie1 {{\n {indent*2} fields {{\n'
        array_data = event_data.loc[(input_data['schema_type']=='array')] # handle event array data separately
        row_dsl = f'{indent*3}'

        for row in event_data.itertuples():
            '''
            Process excel file row by row and build DSL.
            '''
            # event DSL
            if row.column_description != '-':
                event_dsl += f'{indent*2}string {row.column_description};\n'

            # preprocess DSL
            if row.column_description == '-':
                '''
                If column description column in excel mapping equals "-", the root path
                must be reset to the topic_column value
                '''

                root_path = row.topic_column

                if row.schema_type == 'array':
                    # add array information in the "fields" section
                    # of the event then skip to the next row

                    list_name = root_path.split('.')[-1]
                    row_dsl += f'private list[json] l_{list_name} := COALESCE($.{root_path}, (list[json])"");\n'
                    preprocess_dsl += row_dsl
                    row_dsl = f'{indent*3}'
                continue

            if root_path.lower().startswith('header'):
                # Header DSL
                if 'transactiontype' in row.topic_column.lower():
                    # transaction type value must be saved into a separate variable
                    # to be used for filtering in a later phase
                    row_dsl += f'private string pTransactionType := $.{root_path}[?(@.{row.topic_column})]["value"][0];\n'
                    row_dsl += f'{indent*3}{row.column_description} := pTransactionType;\n'
                else:
                    row_dsl += f'{row.column_description} := (string)$.{root_path}[?(@.{row.topic_column})]["value"][0];\n'
            elif row.schema_type!='array':
                # Body DSL
                row_dsl += f'{row.column_description} := $.{".".join([root_path, row.topic_column])};\n'.replace(".-", "")
            
            if row_dsl.strip():
                # append the generated row DSL to the event DSL
                preprocess_dsl += row_dsl
            row_dsl = f'{indent*3}'

        # append sourceOffset and sourcePartition standard columns to event DSL
        event_dsl += f'{indent*2}long sourceOffset;\n'
        event_dsl += f'{indent*2}long sourcePartition;\n{indent}}}\n'
        event_dsl += f'{indent}topic dlfintraappeventstopic version "1.0.0"\n'
        event_dsl += f'{indent}keyby serviceEventCorrelationId\n}}\n\n'
        
        # append sourceOffset and sourcePartition standard columns to preprocess DSL
        preprocess_dsl += f'{indent*3}sourceOffset := $_offset;\n{indent*3}sourcePartition := $_partition;\n{indent*2}}}\n'
        preprocess_dsl += f'{indent*2}topic {row.topic} version "{in_version}"\n'
        preprocess_dsl += f'{indent*2}where (pTransactionType!="deleteINGContract" && pTransactionType!="deleteINGPartyWithAncestors")\n'
        preprocess_dsl += f'{indent*2}keyby serviceEventCorrelationId\n{indent}}}\n'

        '''
        Arrays must be treated separately because they represent a different
        section after the "fields" part of the preprocess event.
        '''
        array_dsl = ''
        for row in array_data.itertuples():
            if row.column_description == '-':
                # each array must be given a name so we use the last value from the column path
                list_name = row.topic_column.split('.')[-1]
                
                array_dsl += f'{indent}from ie1.l_{list_name} as current{list_name.capitalize()}\n'
                array_dsl += f'{indent}publish {event} version "{out_version}" {{\n'
            else:
                array_dsl += f'{indent*2}{row.column_description} := EVALUATE_PATH(current{list_name.capitalize()}, "$.{row.topic_column}", string);\n'

        if array_dsl:
            '''
            If the event contains an array, its processing must be appendend
            to the DSL before publishing to intra app events topic.
            '''
            array_dsl += f'{indent}}}'
            preprocess_dsl += f'\n{array_dsl}'
        else:
            preprocess_dsl += f'{indent}publish {event} version "{out_version}"'
        preprocess_dsl += '\n}\n\n\n'

        write_dsl(event_dsl + preprocess_dsl, os.path.join(output_folder_path, row.topic, 'r2r.musasabi'))


def generate_mee(config):
    input_file = "/tmp/sf/input.xls"
    output_folder_path = config["Ruleset"]["RulesetPath"]

    try :
        excelFile = pd.ExcelFile(input_file)
        input_data = pd.read_excel(excelFile, "OnePam - EDL new")
    except Exception as err :
        logger.error("File could not be loaded: {}".format(err))

    # initiate local variables
    indent = '    '
    dsl = ''
    out_version = config['Versions']['output']
    prefix = config['Outputtopics']['prefix']

    events_list = input_data['event'].unique()
    for event in events_list:
        '''
        For each event initiate new model dsl.
        '''
        event_data = input_data.loc[(input_data['event']==event)]

        # initiate event dsl
        dsl = f'model model_{event} version "{out_version}" {{\n'
        dsl += f'{indent}triggered by event {event} version "{out_version}" {{\n'
        dsl += f'{indent*2}string a := "dummy";\n{indent}}}\n\n'
        dsl += f'{indent}publish {prefix}{event.lower()}topic version "{out_version}" {{\n'

        for row in event_data.itertuples():
            '''
            Process excel file row by row and build DSL.
            '''
            # event DSL
            if row.column_description != '-':
                dsl += f'{indent*2}{row.table_column_name} := ${row.column_description};\n'
        
        # add standard columns
        dsl += f'{indent*2}TOPC := "{row.topic}";\n'
        dsl += f'{indent*2}KAFKA_OFST := $sourceOffset;\n'
        dsl += f'{indent*2}KAFKA_PARTITION := $sourcePartition;\n{indent}}}\n}}\n\n\n'

        write_dsl(dsl, os.path.join(output_folder_path, row.topic, 'mee.musasabi'))
       