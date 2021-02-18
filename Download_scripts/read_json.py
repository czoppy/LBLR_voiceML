import pandas as pd
import json
from datetime import timedelta

def path_to_json(path):
    '''take a local path, read it and return an json variable'''
    f = open(path)
    # return Json object as a dictunary
    json_data = json.load(f)
    # close json file
    f.close()
    return json_data

def json_result_lblr_to_df(json_data):
    '''takes a json format of lblr results and flatten it to a general data frame
    the data frame it returns should be processed farther in perpose to be ready to analisys '''
    # creating the first df from the first 'result' part of the json file
    base_df = pd.json_normalize(json_data['results'])
    # save the number of rows (jobs) which the base_df contains
    num_of_jobs = base_df.shape[0]

    # list to save each job df separately for future concatenate
    df_list = []

    # loop which run over all rows (jobs) read every page and save a job df into the df_list
    for i in range(num_of_jobs):
        # save the row separately
        row = base_df.iloc[i]
        # save df with the information of the job multiply by the number of pages in the job
        temp_df_multiply = pd.concat([row.drop(['Payload.Pages', 'PagesCount'])] * int(row['PagesCount']),ignore_index=True, axis=1).transpose()
        # flatten the page from json format into a df
        temp_df_pages = pd.json_normalize(row['Payload.Pages'])
        # concatenate the page df and the general job df into one df
        temp_df = pd.concat([temp_df_multiply,temp_df_pages],axis=1)
        # appending the df into the list
        df_list.append(temp_df)

    # combining all separate jobs df into one df
    df = pd.concat(df_list)

    return df

def separate_times_types(time):
    ''''''
    time_types = ['start_perform', 'end_perform', 'start_review', 'end_review']
    time_to_return = [None,None,None,None]
    for i in range(len(time_types)):
        for time_dict in time:
            if time_dict['type'] == time_types[i]:
                if time_to_return[i] == None:
                    time_to_return[i] = int(time_dict['date'])
                elif time_to_return[i] < int(time_dict['date']):
                    time_to_return[i] = int(time_dict['date'])

    series_time =pd.to_datetime(pd.Series(time_to_return),unit='ms')
    # [date,preform_duration,review_duration]
    # print(series_time)
    date_and_duration = [series_time[1].date(),series_time[1]-series_time[0],series_time[3]-series_time[1]]
    series_date_and_duration = pd.Series(date_and_duration)
    # print(series_date_and_duration)
    return pd.Series(series_time.tolist()+series_date_and_duration.tolist())

def add_time_columns(df):
    '''take a df after flattened and convert the time into 9 colomns
    'start_perform' - timedate
    'end_perform' - timedate
    'start_review'- timedate
    'end_review'- timedate
    'date'- timedate
    'perform_duration'- timedelta
    'review_duration'- timedelta
    'perform_duration_sec'- float of seconds
    'review_duration_sec'float of seconds] '''
    temp_df = df
    temp_df[['start_perform', 'end_perform', 'start_review', 'end_review','date','perform_duration','review_duration']] = temp_df['time'].apply(separate_times_types)
    temp_df['perform_duration_sec'] = temp_df['perform_duration'].apply(timedelta.total_seconds)
    temp_df['perform_review_sec'] = temp_df['perform_duration'].apply(timedelta.total_seconds)
    return temp_df

def find_descriptors(descriptors,MaxSelectable = 1):
    '''supposed to use with df.apply on the descriptors columns
    takes a descriptors list (from lblr json format) and return a list of all the answers (which was marked true in the selected key in the dict)
    the length of the list default to be 1 but it can change in related to the value of MaxSelectable'''
    descriptor_list = [None]*MaxSelectable
    count = 0
    for descriptor in descriptors:
        if descriptor['Selected'] == True:
            descriptor_list[count] = descriptor['Descriptor']
            count += 1
    if MaxSelectable == 1:
        return descriptor_list[0]
    else:
        return pd.Series(descriptor_list)

def list_to_item(list):
    '''handle with results which comes in form of list but have just one value'''
    if len(list) == 1:
        return list[0]
    elif len(list) == 0:
        return None
    else:
        return list

def classifier_json_to_df(path):
    '''takes a path of classifier json result form and return a df of results'''
    df = json_result_lblr_to_df(path_to_json(path))
    df = add_time_columns(df)
    df['Descriptor_choose'] = df['Descriptors'].apply(lambda x: find_descriptors(x))
    return df

def transcription_json_to_df(path):
    df = json_result_lblr_to_df(path_to_json(path))
    df = add_time_columns(df)
    df['Annotations'] = df['Annotations'].apply(list_to_item)
    return df


### test the code
folder = 'test_files/'
# print(transcription_json_to_df('test_transcription_results.json'))
print(classifier_json_to_df('test_fiels/test_clasification_results.json'))

