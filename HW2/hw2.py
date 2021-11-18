"""
**Grading:**
- Q1 - 5 points - Initial Steps
- Q2 - 50 points - MapReduceEngine
- Q3 - 30 points - Implement the MapReduce Inverted index of the JSON documents
- Q4 - 5 points - Testing Your MapReduce
- Q5 - 10 points - Final Thoughts 

`Total: 100`
"""


# general
import os
import time
import random
import warnings
import threading # you can use easier threading packages

# ml
import numpy as np
import scipy as sp
import pandas as pd

# visual
import seaborn as sns
import matplotlib.pyplot as plt

# notebook
from IPython.display import display


warnings.filterwarnings('ignore')

%%javascript
IPython.OutputArea.prototype._should_scroll = function(lines) {
    return false;
}


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

firstname = ['John', 'Dana', 'Scott', 'Marc', 'Steven', 'Michael', 'Albert', 'Johanna']
city = ['NewYork', 'Haifa', 'Munchen', 'London', 'PaloAlto',  'TelAviv', 'Kiel', 'Hamburg']
secondname = None # please use some version of random

pass


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
    def execute(input_data, map_function, reduce_function):
        pass
    
    
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























