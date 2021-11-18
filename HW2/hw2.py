"""
**Grading:**
- Q1 - 5 points - Initial Steps
- Q2 - 50 points - MapReduceEngine
- Q3 - 30 points - Implement the MapReduce Inverted index of the JSON documents
- Q4 - 5 points - Testing Your MapReduce
- Q5 - 10 points - Final Thoughts 

`Total: 100`
"""
import sqlite3
import pandas as pd
import numpy as np 
import random
import os 
import gc
import pyarrow.parquet as pq
import pyarrow as pa
import dask
from dask.dataframe import to_parquet
import csv
from dask.dataframe import from_pandas
import gc 
import glob
from shutil import copyfile

gc.collect()

# general
import os
import time
import random
import warnings
import threading # you can use easier threading packages
import string 

# ml
import numpy as np
import scipy as sp
import pandas as pd

# visual
import seaborn as sns
import matplotlib.pyplot as plt

# notebook
from IPython.display import display
from pathlib import Path
warnings.filterwarnings('ignore')
from joblib import Parallel, delayed
from functools import partial


"""
<br><br><br><br>
# Question 1
# Initial Steps

Write Python code to create 20 different CSV files in this format:  `myCSV[Number].csv`, where each file contains 10 records. 

The schema is `(‘firstname’,’secondname’,city’)`  

Values should be randomly chosen from the lists: 
- `firstname` : `[John, Dana, Scott, Marc, Steven, Michael, Albert, Johanna]`  
- `city` : `[New York, Haifa, München, London, Palo Alto,  Tel Aviv, Kiel, Hamburg]`  
- `secondname`: any value  

"""



class single_record():
    firstname = ''
    secoundname = ''
    city = ''
    
    # Set the value options
    firstname_option_list  = ['John', 'Dana', 'Scott', 'Marc', 'Steven', 'Michael', 'Albert', 'Johanna']
    secoundname_option_list  = ['John', 'Dana', 'Scott', 'Marc', 'Steven', 'Michael', 'Albert', 'Johanna']
    city_option_list = ['NewYork', 'Haifa', 'Munchen', 'London', 'PaloAlto',  'TelAviv', 'Kiel', 'Hamburg']
    def __init__(self, id):
        self.firstname = random.choice(self.firstname_option_list)	
        random_name  = ''.join(random.choice(string.ascii_uppercase + string.ascii_lowercase) for _ in range(random.randint(2,4)))
        self.secoundname = random_name
        self.city = random.choice(self.city_option_list)
        
def init_data_set_configuration():
    # Set the value options
    global max_rows, csv_columns, db_file_name, csv_file_name, columns_type
    global parquet_file_name_using_dask,parquet_file_name_using_pyarray 
    global parquet_file_name_using_pandas, csv_index, csv_ending, amount_of_files
    global map_reduce_folder_names, amount_of_process, map_regex, db_columns
    global db_columns_type, db_table_name, reduce_regex_init, reduce_regex_final
    
    
    Current_python_file_path = os.getcwd()
    max_rows = 10
    amount_of_files = 5 # neeed to be 20
    amount_of_process = 2
    csv_columns = ['firstname','secondname','city']
    db_columns = ['key', 'value']
    map_reduce_folder_names =  [Current_python_file_path+ '\\mapreducetemp', Current_python_file_path+'\\mapreducefinal']

    db_columns_type = [ 'text',  'text']

    db_file_name = 'mydata.db'
    csv_file_name = 'myCSV'
    csv_ending = '.csv'
    map_regex = 'part-tmp-'
    reduce_regex_init = 'part-'
    reduce_regex_final = '-final'

    db_table_name = 'temp_results'

    #parquet_file_name_using_dask = 'mydatapyarrow_dask.parquet'
    #parquet_file_name_using_pyarray = 'mydatapyarrow_pyarray.parquet'
    #parquet_file_name_using_pandas = 'mydatapyarrow_pandas.parquet'

    return

def create_csvdatabase_file(max_rows):
    # this loop generate single fruit
    create_n_list_in_advance = max_rows*[None]
    for i_row_index in range(max_rows):
        i_record = single_record(i_row_index)
        create_n_list_in_advance[i_row_index] = [i_record.firstname, i_record.secoundname, 
                                                 i_record.city]
    
    mydata_df = pd.DataFrame(create_n_list_in_advance, columns = csv_columns )
    mydata_df.to_csv(csv_file_name)
    return mydata_df

def generate_n_csv_file():
    for i_csv_index in range(0, amount_of_files):
        mydata_df = create_csvdatabase_file(max_rows)
        mydata_df.to_csv(csv_file_name + str(i_csv_index) + csv_ending )
    return

def create_new_folder(path):
    Path(path).mkdir(parents=True, exist_ok=True)
    return 

def generate_map_reduce_folders():
    for i_folder in map_reduce_folder_names:
        create_new_folder(i_folder)
    return 

def create_db_database(db_file_name):
    is_exist = os.path.exists(db_file_name)
    if  is_exist:
        os.remove(db_file_name)
    con = sqlite3.connect(db_file_name)
        
    
    cur = con.cursor()
    
    # Create table
    columns_type_list = list(map(lambda x,y: x+' ' + y, db_columns, db_columns_type))
    columns_type_list_string = "("+", ".join(map(str, columns_type_list))+")"

    cur.execute(''' 
                CREATE TABLE temp_results
                ''' + columns_type_list_string + \
               '''''')
    
    con.commit()
    con.close()
    return con, cur


def fill_db_data_base_using_csv_data_base_with_same_keys(mydata_df):
    con = sqlite3.connect(db_file_name)
    cur = con.cursor()

    for i_csv_row in range(mydata_df.shape[0]):
        # Take row from csv file
        i_row = mydata_df.iloc[i_csv_row]
        i_row_as_list = i_row.to_list()
        
        i_row_as_list_string = "('"+"','".join(map(str, i_row_as_list))+"')"
        #cur.execute("INSERT INTO stocks VALUES ('2006-01-05','BUY','RHAT','100')")

        #Insert a csv row of data base
        cur.execute("INSERT INTO temp_results VALUES "  \
                    +i_row_as_list_string + \
                    "")
    con.commit()
    con.close()
    return



# init_data_set_configuration settings
init_data_set_configuration()

# 
generate_n_csv_file()


generate_map_reduce_folders()
# create csv file as data frame 


input_data = glob.glob(csv_file_name+'*'+csv_ending  )
        


def map_function(filename: str) -> dict:
    #print(filename)
    df = pd.read_csv(filename)
    Dict = {'key': df['firstname'].to_list(), 'value': [filename]*len(df)}
    return Dict


def reduce_function(key: str, value: str) -> dict:
    #print(filename)
    amount_of_csv_files = value.split(',').__len__()
    Dict = {'key': key, 'value': amount_of_csv_files}
    return Dict

def run_at_parallel_map(i_item_index, i_item, function):
    new_path = None
    succeed = True
    try:
        
        new_path  = map_reduce_folder_names[0] + '\\'+ map_regex + str(i_item_index) + csv_ending
    
        dict_result = function(i_item)
        result_df = pd.DataFrame(data=dict_result)

        result_df.to_csv(new_path) 
        
        succeed = os.path.exists(new_path)
    except:
        succeed = False
        
    return  succeed, new_path


def run_at_parallel_reduce(i_item_index, i_item, function):
    new_path = None
    succeed = True
    try:
        new_path  = map_reduce_folder_names[1] + '\\'+ reduce_regex_init + str(i_item_index) + reduce_regex_final + csv_ending
        print(new_path)
        key = i_item[0]
        value = i_item[1]

        dict_result = function(key, value)
        result_df = pd.DataFrame.from_records([dict_result])

        result_df.to_csv(new_path) 
        
        succeed = os.path.exists(new_path)
    except:
        succeed = False
        
    return  succeed, new_path


def print_db_file_info_bsase_single_key(key):
    con = sqlite3.connect(db_file_name)
    cur = con.cursor()
    return_list = []
    for row in cur.execute('SELECT key, GROUP_CONCAT(value) FROM ' +db_table_name+ ' GROUP BY ' + key + ' ORDER BY ' + key):
        print(row)
        return_list.append(row)
    con.close()
    return return_list

def print_db_file_info_bsase_single_key_hafoocha(key):
    con = sqlite3.connect(db_file_name)
    cur = con.cursor()
    for row in cur.execute('SELECT value, GROUP_CONCAT(key) FROM ' +db_table_name+ ' GROUP BY ' + key + ' ORDER BY ' + key):
        print(row)
    con.close()
    return

def print_db_file_info():
    con = sqlite3.connect(db_file_name)
    cur = con.cursor()
    for row in cur.execute('SELECT * FROM '+db_table_name):
        print(row)
    con.close()
    return

succeed_new_path_list  = Parallel(n_jobs=amount_of_process, backend="threading", prefer="processes")(delayed(run_at_parallel_map)(
            index, item, map_function) for index, item in enumerate(input_data))

boolean_results = [boolean for boolean, path in succeed_new_path_list]
filepaths = [path for boolean, path in succeed_new_path_list]

sql_conn, cur = create_db_database(db_file_name)

sql_conn = sqlite3.connect(db_file_name)
list(map(lambda x: pd.read_csv(x, index_col=0).to_sql('temp_results',sql_conn, if_exists='append',index=False), filepaths ))    
sql_conn.close()

    
if False in boolean_results:
    print('Map Reduce Failed')
    
    
print_db_file_info()

List = print_db_file_info_bsase_single_key(key='key')


# Begin by Performing REDUCE actions
# we will open a thread for each REDUCE
reduce_return_dict_succeed  = Parallel(n_jobs=amount_of_process, backend="threading", prefer="processes")(delayed(run_at_parallel_reduce)(
    index, item, reduce_function) for index, item in enumerate(List))

boolean_results = [boolean for boolean, path in reduce_return_dict_succeed]
reduce_dict = [reduce_dict for boolean, reduce_dict in reduce_return_dict_succeed]
        
reduce_df = pd.DataFrame(data= reduce_dict)
#print_db_file_info_bsase_single_key_hafoocha(key='value')

print("hoi)")
"""
Use python to Create `mapreducetemp` and `mapreducefinal` folders
"""




"""
<br><br><br>
# Question 2
## MapReduceEngine

Write Python code to create an SQLite database with the following table

`TableName: temp_results`   
`schema: (key:TEXT,value:TEXT)`

"""


pass


"""
1. **Create a Python class** `MapReduceEngine` with method `def execute(input_data, map_function, reduce_function)`, such that:
    - `input_data`: is an array of elements
    - `map_function`: is a pointer to the Python function that returns a list where each entry of the form (key,value) 
    - `reduce_function`: is pointer to the Python function that returns a list where each entry of the form (key,value)

<br><br>

**Implement** the following functionality in the `execute(...)` function:

<br>

1. For each key  from the  input_data, start a new Python thread that executes map_function(key) 
<br><br>
2. Each thread will store results of the map_function into mapreducetemp/part-tmp-X.csv where X is a unique number per each thread.
<br><br>
3. Keep the list of all threads and check whether they are completed.
<br><br>
4. Once all threads completed, load content of all CSV files into the temp_results table in SQLite.

    Remark: Easiest way to loop over all CSV files and load them into Pandas first, then load into SQLite  
    `data = pd.read_csv(path to csv)`  
    `data.to_sql(‘temp_results’,sql_conn, if_exists=’append’,index=False)`
<br><br>

5. **Write SQL statement** that generates a sorted list by key of the form `(key, value)` where value is concatenation of ALL values in the value column that match specific key. For example, if table has records
<table>
    <tbody>
            <tr>
                <td style="text-align:center">John</td>
                <td style="text-align:center">myCSV1.csv</td>
            </tr>
            <tr>
                <td style="text-align:center">Dana</td>
                <td style="text-align:center">myCSV5.csv</td>
            </tr>
            <tr>
                <td style="text-align:center">John</td>
                <td style="text-align:center">myCSV7.csv</td>
            </tr>
    </tbody>
</table>

    Then SQL statement will return `(‘John’,’myCSV1.csv, myCSV7.csv’)`  
    Remark: use GROUP_CONCAT and also GROUP BY ORDER BY
<br><br><br>
6. **Start a new thread** for each value from the generated list in the previous step, to execute `reduce_function(key,value)` 
<br>    
7. Each thread will store results of reduce_function into `mapreducefinal/part-X-final.csv` file  
<br>
8. Keep list of all threads and check whether they are completed  
<br>
9. Once all threads completed, print on the screen `MapReduce Completed` otherwise print `MapReduce Failed` 


"""

# implement all of the class here


class MapReduceEngine():
    def execute(input_data: list, map_function, reduce_function):
        
        # Begin by Performing MAP actions
        # we will open a thread for each MAP
        succeed_new_path_list  = Parallel(n_jobs=amount_of_process, backend="threading", prefer="processes")(delayed(run_at_parallel)(
            index, item, map_function) for index, item in enumerate(input_data))

        boolean_results = [boolean for boolean, path in succeed_new_path_list]
        filepaths = [path for boolean, path in succeed_new_path_list]
        
        #
        
        if False in boolean_results:
            print('Map Reduce Failed')
            return
        
        List = print_db_file_info_bsase_single_key(key='key')
        
        # Begin by Performing REDUCE actions
        # we will open a thread for each REDUCE
        succeed_new_path_list  = Parallel(n_jobs=amount_of_process, backend="threading", prefer="processes")(delayed(run_at_parallel)(
            index, item, reduce_function) for index, item in enumerate(List))

        boolean_results = [boolean for boolean, path in succeed_new_path_list]
        filepaths = [path for boolean, path in succeed_new_path_list]
        
        #
        
        if False in boolean_results:
            print('Map Reduce Failed')
            return
        
    
    
"""
<br><br><br><br>

# Question 3
## Implement the MapReduce Inverted index of the JSON documents

Implement a function `inverted_map(document_name)` which reads the CSV document from the local disc and return a list that contains entries of the form (key_value, document name).

For example, if myCSV4.csv document has values like:  
`{‘firstname’:’John’,‘secondname’:’Rambo’,‘city’:’Palo Alto’}`

Then `inverted_map(‘myCSV4.csv’)` function will return a list:  
`[(‘firstname_John’,’ myCSV4.csv’),(‘secondname_Rambo’,’ myCSV4.csv’), (‘city_Palo Alto’,’ myCSV4.csv’)]`

"""
def inverted_map(document_name):
    pass


"""
Write a reduce function `inverted_reduce(value, documents)`, where the field “documents” contains a list of all CSV documents per given value.   
This list might have duplicates.   
Reduce function will return new list without duplicates.

For example,  
calling the function `inverted_reduce(‘firstname_Albert’,’myCSV2.csv, myCSV5.csv,myCSV2.csv’)`   
will return a list `[‘firstname_Albert’,’myCSV2.csv, myCSV5.csv,myCSV2.csv’]`
"""

def inverted_reduce(value, documents):
    pass


"""

<br><br><br><br>
# Question 4
## Testing Your MapReduce

**Create Python list** `input_data` : `[‘myCSV1.csv’,.. ,‘myCSV20.csv’]`


"""
input_data = None

"""
**Submit MapReduce as follows:**
"""

mapreduce = MapReduceEngine()
status = mapreduce.execute(input_data, inverted_map, inverted_reduce)
print(status)

"""
Make sure that `MapReduce Completed` should be printed and `mapreducefinal` folder should contain the result files.
**Use python to delete all temporary data from mapreducetemp folder and delete SQLite database:**

"""
pass

"""
<br><br><br><br>

# Question 5
# Final Thoughts

The phase where `MapReduceEngine` reads all temporary files generated by maps and sort them to provide each reducer a specific key is called the **shuffle step**.

Please explain **clearly** what would be the main problem of MapReduce when processing Big Data, if there is no shuffle step at all, meaning reducers will directly read responses from the mappers.

            If you say "I dont know" you will get 2 points :)
"""























