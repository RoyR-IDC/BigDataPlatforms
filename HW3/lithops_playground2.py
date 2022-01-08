import glob
import os
import sqlite3
from io import StringIO, BytesIO

import lithops
import numpy as np
import pandas as pd
from lithops import Storage


global_config = ...


def init_configurations():
    # Set the value options
    global max_rows, csv_columns, db_file_name, csv_file_name, columns_type
    global csv_index, csv_ending, amount_of_files
    global map_reduce_folder_names, amount_of_process, map_regex, db_columns
    global db_columns_type, db_table_name, reduce_regex_init, reduce_regex_final

    Current_python_file_path = os.getcwd()
    max_rows = 10
    amount_of_files = 20
    amount_of_process = 2
    csv_columns = ['firstname', 'secondname', 'city']
    db_columns = ['key', 'value']
    map_reduce_folder_names = [os.path.join(Current_python_file_path, 'mapreducetemp'),
                               os.path.join(Current_python_file_path, 'mapreducefinal')]

    db_columns_type = ['text', 'text']

    db_file_name = 'mydata.db'
    csv_file_name = 'myCSV'
    csv_ending = '.csv'
    map_regex = 'part-tmp-'
    reduce_regex_init = 'part-'
    reduce_regex_final = '-final'

    db_table_name = 'temp_results'

    return


def inverted_map(document_name: str):
    try:
        # Preparations
        storage = Storage(config=global_config)
        data = storage.get_object(global_config['ibm_cos']['storage_bucket'], document_name)

        # Begin
        csv_df = pd.read_csv(BytesIO(data), encoding='utf8', sep=",", index_col=0)
        csv_size = csv_df.shape[0]
        csv_columns = csv_df.columns.to_list()
        output_list = []
        for i_col in csv_columns:
            col_vals = csv_df[i_col].to_list()
            curr_ouput = list(map(lambda x, y, z: (x + '_' + y, z), csv_size * [i_col], col_vals, csv_size * [document_name]))
            output_list += curr_ouput

    except Exception as exception:
        return [], False

    return output_list, True


def inverted_reduce(data):
    try:
        value, documents = data
        ducument_name_list = documents.split(',')
        ducument_name_list_no_duplicates = list(set(ducument_name_list))
        string_ducument_name_list_no_duplicates = (', ').join(ducument_name_list_no_duplicates)
        return_list = [value, string_ducument_name_list_no_duplicates]

    except Exception as exception:
        return [], False

    return return_list, True


class MapReduceServerlessEngine(object):
    def __init__(self):
        # create a function executor
        self._fexec = lithops.FunctionExecutor(config=global_config)

    def execute(self, input_data, map_function, reduce_function):
        """
        execute map and reduce

        :param input_data: assumption: given as:     input_data = 'cos://bucket/<path to CSV data>'
        :param map_function:
        :param reduce_function:
        :return:
        """
        #  1) For each key  from the  input_data, start a new Python thread that executes
        #     map_function(key)
        #  2) Each serverless action will store results of the map_function into
        #     mapreducetemp/part-tmp-X.csv where X
        #     is a unique number per each thread.
        #  3) Keep the list of all threads and check whether they are completed

        region, bucket_name, extra = self._parse_input_data(input_data)
        self._bucket_name = global_config['ibm_cos']['storage_bucket']

        curr_config = global_config
        self._storage = Storage(config=curr_config)

        list_of_objects = self._storage.list_objects(bucket=bucket_name)
        list_of_csv_objects = [item for item in list_of_objects if '.csv' in item['Key']]
        list_of_csv_names = [item['Key'] for item in list_of_csv_objects if '.csv' in item['Key']]

        # call function async-ly
        response_list = []
        for csv_name in list_of_csv_names:
            response = self._fexec.call_async(func=map_function, data={'document_name': csv_name})
            response_list.append(response)

        try:
            results = self._fexec.get_result(fs=response_list)  # internally calls wait. note: if one fails, all fails !
            self._fexec.clean()
        except Exception as exception:
            status = 'Map Reduce Failed'
            return status

        # get list of succeed or failed of threads
        boolean_results = [boolean for output, boolean in results]

        # validate that all threads are completed succesfully
        if False in boolean_results:
            status = 'Map Reduce Failed'
            return status

        # 4) Once all threads completed, load content of all CSV files into the temp_results
        #    table in SQLite

        # get new files names
        outputs = [output for output, boolean in results]

        # write generated csv files to sql data base
        sql_conn = sqlite3.connect(db_file_name)
        for output in outputs:
            result_df = pd.DataFrame(data=output, columns=['key', 'value'])
            result_df.to_sql('temp_results', sql_conn, if_exists='append', index=False)

        # dont forget to close connection after finishing
        sql_conn.close()

        # 5) **Write SQL statement** that generates a sorted list by key of the form
        #    `(key, value)` where value is concatenation of ALL values in the value column
        #     that match specific key. For example, if table has records

        # query data base using GROUP_CONCAT and GROUP BY  and ORDER BY
        generated_list = self._get_grouped_info_from_db_by_key(key='key')

        # 6) **Start a new thread** for each value from
        #    the generated list in the previous step, to execute `reduce_function(key,value)
        #    Begin by Performing REDUCE actions
        #    we will open a thread for each REDUCE
        # 7) Each thread will store results of reduce_function into
        #   `mapreducefinal/part-X-final.csv` file

        # 8) Keep list of all threads and check whether they are completed

        # call function async-ly
        response_list = []
        for item in generated_list:
            response = self._fexec.call_async(func=reduce_function, data={'data': item})
            response_list.append(response)

        try:
            results = self._fexec.get_result(fs=response_list)  # internally calls wait. note: if one fails, all fails !
            self._fexec.clean()
        except Exception as exception:
            status = 'Map Reduce Failed'
            return status

        # 9) Once all threads completed, print on the screen
        #   `MapReduce Completed` otherwise print `MapReduce Failed`

        # get list of succeed or failed of threads
        boolean_results = [boolean for output, boolean in results]

        # validate that all threads are completed succesfully
        if False in boolean_results:
            status = 'Map Reduce Failed'
            return status

        #
        outputs = [output for output, boolean in results]
        reduce_df = pd.DataFrame(data=outputs)
        return reduce_df


    def _get_grouped_info_from_db_by_key(self, key):
        con = sqlite3.connect(db_file_name)
        cur = con.cursor()
        return_list = []
        for row in cur.execute(
                'SELECT key, GROUP_CONCAT(value) FROM ' + db_table_name + ' GROUP BY ' + key + ' ORDER BY ' + key):
            # print(row)
            return_list.append(row)
        con.close()
        return return_list

    def _parse_input_data(self, input_data: str):
        """

        :param input_data: assumption: format is something like :  input_data = 'cos://eu-de/cloud-object-storage-mq-cos-standard-8s4/myCSV0.csv'
        :return:
        """

        if input_data is None or input_data == '':
            raise AssertionError(f'Bad arguments passed: input_data is None or input_data == ''')

        parts = input_data.split(sep='cos://')[1].split(sep='/')

        region = parts[0]
        bucket = parts[1]
        extra = parts[2:]

        return region, bucket, extra


if __name__ == '__main__':
    # Prepare
    init_configurations()

    # input_data = 'cos://eu-de/cloud-object-storage-mq-cos-standard-8s4/'
    separator = '/'
    ibm_intro = 'cos://'
    extra_path = ''
    input_data = ibm_intro + global_config['ibm_cos']['region'] + separator + global_config['ibm_cos']['storage_bucket'] + separator
    print(f'input_data:\n{input_data}')

    # Run
    mapreduce = MapReduceServerlessEngine()
    result = mapreduce.execute(input_data, inverted_map, inverted_reduce)
    print(result)
