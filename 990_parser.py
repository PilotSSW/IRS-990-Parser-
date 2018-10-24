import boto3
import os
from irsx.xmlrunner import XMLRunner
import pandas as pd
import time
import flatdict
from collections import defaultdict

################################################################
# SCRIPT SETUP #################################################
################################################################

# Amazon Irs 990 S3 bucket
repo = 'irs-form-990'
selected_year = 2017

################################################################
# SCRIPT SETUP #################################################
################################################################

# Setup Amazon S3 client
s3_client = boto3.client('s3')
s3_resource = boto3.resource('s3')

bucket = s3_resource.Bucket(name=repo)
current_working_directory = os.getcwd()
start_time = time.time()

dataframes = {}
files = bucket.objects.filter(Prefix=str(selected_year))

current_file = 1
for file in files:
    # Get just the id of the 990 record from the file name
    record_id = file.key.split('_')[0]
    parsed_filing = XMLRunner().run_filing(record_id)

    progress = current_file / len(files) * 100
    elapsed_time = int(time.time() - start_time)
    print('Time: ' + str(elapsed_time // 100) + ':' + str(round(elapsed_time % 100, 1)) + '\n' +
          'Parser Progress: ' + str(round(progress, 2)) + '%\n' +
          'File: ' + str(current_file) + ' of ' + str(len(files)) + '\n' +
          'ID: ' + record_id + '\n',
          end='\r')

    for sked in parsed_filing.get_result():
        fields = flatdict.FlatterDict(sked['schedule_parts'], delimiter=":")
        dictionary_of_fields = defaultdict(list)
        for key, value in fields.items():
            dictionary_of_fields[key].append(value)

        if sked['schedule_name'] in dataframes.keys():
            # Add new data to an existing section
            current_frame = dataframes[sked['schedule_name']]
            new_frame = pd.DataFrame().from_dict(dictionary_of_fields)
            updated_frame = pd.concat([current_frame, new_frame], join='outer', sort=True, ignore_index=True)
            dataframes[sked['schedule_name']] = updated_frame
        else:
            # This section hasn't been seen yet - create it
            dataframes[sked['schedule_name']] = pd.DataFrame().from_dict(dictionary_of_fields)

    current_file += 1

writer = pd.ExcelWriter('output.xlsx')
current_file = 1
for key_schedule, frame in dataframes.items():
    progress = current_file / len(dataframes) * 100
    print('Export progress: ' + str(progress) + '%', end='\r')
    frame.to_excel(writer, str(selected_year)+'_990_records.xlsx', sheet_name=key_schedule)
    current_file += 1
    print('Export progress: ' + str(progress) + '%', end='\r')

writer.save()
