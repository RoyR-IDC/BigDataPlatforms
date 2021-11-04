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
gc.collect()
"""
Create local CSV file “mydata.csv” with 1000000 
rows with columns (id, fruit, price, color). 
fruit has one of the values ['Orange', 'Grape', 'Apple', 'Banana','Pineapple', 'Avocado'] 
colors are ['Red', 'Green', 'Yellow', 'Blue']. 
Price should be random integer between 10 and 100. 
Filed id should be an index number starting from 1.
"""
class single_fruit():
    fruit_name = ''
    fruit_price = ''
    fruit_color = ''
    fruit_id = ''
    
    # Set the value options
    fruit_options_list = ['Orange', 'Grape', 'Apple', 'Banana','Pineapple', 'Avocado']
    colors_options_list = ['Red', 'Green', 'Yellow', 'Blue']
    price_options_range = [10,100]
    
    def __init__(self, id):
        self.fruit_name = random.choice(self.fruit_options_list)	
        self.fruit_price = np.random.randint(self.price_options_range[0], self.price_options_range[1])
        self.fruit_color = random.choice(self.colors_options_list)
        self.fruit_id = id
        

def init_data_set_configuration():
    # Set the value options
    global max_rows, csv_columns, db_file_name, csv_file_name, columns_type
    global parquet_file_name_using_dask,parquet_file_name_using_pyarray 
    global parquet_file_name_using_pandas
    max_rows = 10
    csv_columns = ['id', 'fruit', 'price', 'color']
    columns_type = ['integer', 'text', 'integer', 'text']

    db_file_name = 'mydata.db'
    csv_file_name = 'mydata.csv'
    parquet_file_name_using_dask = 'mydatapyarrow_dask.parquet'
    parquet_file_name_using_pyarray = 'mydatapyarrow_pyarray.parquet'
    parquet_file_name_using_pandas = 'mydatapyarrow_pandas.parquet'

    return

def create_csvdatabase_file(max_rows):
    # this loop generate single fruit
    create_n_list_in_advance = max_rows*[None]
    for i_row_index in range(max_rows):
        i_friut = single_fruit(i_row_index)
        create_n_list_in_advance[i_row_index] = [i_friut.fruit_id, i_friut.fruit_name, 
                                                 i_friut.fruit_price, i_friut.fruit_color]
    
    mydata_df = pd.DataFrame(create_n_list_in_advance, columns = csv_columns )
    mydata_df.to_csv(csv_file_name)
    return mydata_df

def create_db_database(db_file_name):
    is_exist = os.path.exists(db_file_name)
    if  is_exist:
        os.remove(db_file_name)
    con = sqlite3.connect(db_file_name)
        
    
    cur = con.cursor()
    
    # Create table
    columns_type_list = list(map(lambda x,y: x+' ' + y, csv_columns, columns_type))
    columns_type_list_string = "("+", ".join(map(str, columns_type_list))+")"

    cur.execute(''' 
                CREATE TABLE stocks
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
        cur.execute("INSERT INTO stocks VALUES "  \
                    +i_row_as_list_string + \
                    "")
    con.commit()
    con.close()
    return

def print_db_file_info_bsase_single_key(key):
    con = sqlite3.connect(db_file_name)
    cur = con.cursor()
    for row in cur.execute('SELECT * FROM stocks ORDER BY '+ key):
        print(row)
    con.close()
    return

    
def print_db_file_info_base_one_column_and_equal_value(key, value):
    con = sqlite3.connect(db_file_name)
    cur = con.cursor()
    key_index = csv_columns.index(key)
    key_type = columns_type[key_index]
    
    if key_type == 'integer':
        query = f'SELECT * FROM stocks  WHERE {key} = {value}'
    else:
        query = f'SELECT * FROM stocks  WHERE {key} = \'{value}\''
    
    for row in cur.execute(query):
        print(row)

    con.close()
    return 

def print_db_file_info_base_one_column_and_equal_value2(key, value):
    con = sqlite3.connect(db_file_name)
    cur = con.cursor()
    key_index = csv_columns.index(key)
    key_type = columns_type[key_index]
    
    if key_type == 'integer':
        query = f'SELECT FROM stocks  WHERE {key} = {value}'
    else:
        query = f'SELECT FROM stocks  WHERE {key} = \'{value}\''
    
    for row in cur.execute(query):
        print(row)

    con.close()
    return 

def print_db_file_multiplecolumns_base_one_column_and_equal_value(key, value, columns:list):
    con = sqlite3.connect(db_file_name)
    cur = con.cursor()
    key_index = csv_columns.index(key)
    key_type = columns_type[key_index]
    
    if columns is None or len(columns) == 0:
        columns = '*'
    else:
        columns = ','.join(columns)
    
    if key_type == 'integer':
        query = f'SELECT {columns} FROM stocks  WHERE {key} = {value}'
    else:
        query = f'SELECT {columns} FROM stocks  WHERE {key} = \'{value}\''
    
    for row in cur.execute(query):
        print(row)

    con.close()
    return 

def print_db_file_info_base_one_column_and_geater_than_value(key, value):
    con = sqlite3.connect(db_file_name)
    cur = con.cursor()
    key_index = csv_columns.index(key)
    key_type = columns_type[key_index]
    for row in cur.execute('SELECT * FROM stocks  WHERE '+key+'>'+"%s"  % value):
        print(row)
    con.close()
    return 

def print_db_file_info_base_one_column_and_lower_than_value(key, value):
    con = sqlite3.connect(db_file_name)
    cur = con.cursor()
    key_index = csv_columns.index(key)
    key_type = columns_type[key_index]
    for row in cur.execute('SELECT * FROM stocks  WHERE '+key+'<'+"%s"  % value):
        print(row)
    con.close()
    return 
def first_chunk():
    f2 = open(csv_file_name, "rb")
    f2.seek(0, 0)
    d2 = f2.read(middle).decode(encoding='utf-8')
    amount_of_rows = d2.count('\n')
    
    was_endline = False
    if d2[-1] != '\n':
        amount_of_rows +=1
        was_endline = True
        
    print('first_cunk amount of rows is = ' + str(amount_of_rows))
    return amount_of_rows, was_endline

def last_chunk():
    f2 = open(csv_file_name, "rb")
    f2.seek(middle+1, 0)
    d2 = f2.read(middle).decode(encoding='utf-8')
    amount_of_rows = d2.count('\n')

    print('last_cunk amount of rows is = ' + str(amount_of_rows))
    return amount_of_rows

def get_number_of_lines_in_file():
    amount_of_rows_first, was_endline = first_chunk()
    amount_of_rows_last = last_chunk()
    total_rows = amount_of_rows_first + amount_of_rows_last
    if was_endline is True:
        total_rows -= 1
        
    print('total_rows amount of rows is = ' + str(total_rows))
    return total_rows

def current_chunk(location, chunk_size):
    f2 = open(csv_file_name, "rb")
    f2.seek(location, 0)
    d2 = f2.read(chunk_size).decode(encoding='utf-8')
    amount_of_rows = d2.count('\n')
    
    was_endline = False
    if d2[-1] != '\n':
        amount_of_rows +=1
        was_endline = True
        
    #print('first_cunk amount of rows is = ' + str(amount_of_rows))
    return amount_of_rows, was_endline

def get_number_of_lines_in_file_v2(file_size):
    total_rows = 0
    chunk_size = 11
    for location in range(0,file_size,chunk_size):    
        amount_of_rows, was_endline = current_chunk(location, chunk_size)
        total_rows += amount_of_rows
        if was_endline is True:
            total_rows -= 1
        
    print('total_rows amount of rows is = ' + str(total_rows))
    return total_rows
    

# init_data_set_configuration settings
init_data_set_configuration()

# create csv file as data frame 
mydata_df = create_csvdatabase_file(max_rows)

# create a db data base
con, cur = create_db_database(db_file_name)

# copy csv data into db file
fill_db_data_base_using_csv_data_base_with_same_keys(mydata_df)
 
# print db base single key
print_db_file_info_bsase_single_key('id')

# 3 option 2 query our data base predicate and projection
print_db_file_info_base_one_column_and_equal_value('color', 'Blue')
print_db_file_info_base_one_column_and_geater_than_value('id',2)
print_db_file_info_base_one_column_and_lower_than_value('id',2)
print_db_file_multiplecolumns_base_one_column_and_equal_value('color', 'Blue', ['color', 'id'])
# read csv file 
with open(csv_file_name) as csv_file:
    csv_reader = csv.reader(csv_file, delimiter=',')
    line_count = 0
    csv_rows = []
    for row in csv_reader:
        if line_count == 0:
            columns_list = row[1::]
        else:
            current_row = row[1::]
            csv_rows.append(current_row)
        line_count+=1


"""
def read_csv_file_and_get_size(max_size=inf):
    with open(csv_file_name) as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',')
        line_count = 0
        file_size = 0
        csv_rows = []
        for row in csv_reader:
            if line_count == 0:
                columns_list = row[1::]
            else:
                current_row = row[1::]
                csv_rows.append(current_row)
            line_size = get_size(row)
            file_size += line_size
            
            line_count+=1
            if file_size>max_size:
                break
    return csv_rows, file_size
"""
# create new data frame base csv file 
mydata_df_with_out_pandas = pd.DataFrame(csv_rows, columns = columns_list )
print('The amount of row in the following csv is ' + str(line_count))

# write csv with 3 options
pyarray_table = pa.Table.from_pandas(mydata_df_with_out_pandas)
pq.write_table(pyarray_table, parquet_file_name_using_pyarray)
dask_df = from_pandas(mydata_df, npartitions=4)
#dask.dataframe.to_parquet(dask_df, parquet_file_name_using_dask)
dask_df.to_parquet(parquet_file_name_using_dask)
mydata_df_with_out_pandas.to_parquet(parquet_file_name_using_pandas)

memory_usage_size = mydata_df_with_out_pandas.memory_usage(deep = False).sum()
memory_usage_size = os.path.getsize(csv_file_name)
middle = memory_usage_size//2



first_chunk()
last_chunk()
get_number_of_lines_in_file()

get_number_of_lines_in_file_v2(memory_usage_size)
 
"""
Question: Examine generated Parquet files. Why do you think Dask generated Parquet file
differently than PyArrow and Pandas? What might be explanaJon for this?

Answer:
    Unlke parquet files generated from pyarrow and pandas which are saved as a single parquet file, 
    the parquet file generated from dask
    is separated into the predefined number of partitions 
    (if we defined npartitions=4, then our dataframe will be split into 4 datasets,
     which is later saved as 4 parquet files)
    the reason dask splits the data into 4 sets, is to allow parrallel computing.
    
"""

"""
Question:
    Explain which parts of the statement are predicate and which parts are projecJon

Answer:
    1) Predicate Operation : 
       a)   This operation is used to select rows from a
            table (relation) that specifies a given logic, which is called as
            a predicate. 
       b)  The predicate is a user defined condition to select
           rows of user's choice.
    2) Project Operation : 
        a)  If the user is interested in selecting the values
            of a few attributes, rather than selection all attributes of the Table
            then one should go for PROJECT Operation.  
"""


"""
Question:
    Explain why total number of lines from the first chunk and second chunk is larger than
    the number of lines calculated in the step (1) of Task 2

Answer:
    there are 2 factors that change the number of rows calculated in task2:
        1. in task 2 we did not count the header
        2. in this task, the file chunk might end in the mddle of a row, and so 
        that row could be counted twice.
        
"""




