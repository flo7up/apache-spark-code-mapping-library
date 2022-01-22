# PySpark_code_mapping_library

PySpark_code_mapping_library is a Python library for dealing with code mappings
The library allows users to specify mappings in a code mapping csv.file.
The library returns a mapping for a given Spark dataframe.
Optionally, users can specify which input columns from the dataframe should be used as mapping inputs and which columns from the code mapping csv file should be taken into account.



## Installation

The library was  created for installation in your local Python environment.
Create a build, then install the library in your Python environment.
For installation on Azure Synapse, push the file to azure datalake storage and Synapse by using the Azure DevOps pipeline.  


## code_mapping.csv 

The code_mapping.csv holds the mapping values. 
During the mapping, entries from the code_mapping will be selected based on the provided mapping_name.
Only values that are within the valid_to and valid_from date will be considered in the mapping.
Mapping can have up to three input columns (col_1, col_2, col3) and a single target column.
The target column that contains the returned values is by default col_0. However, users can also define col_1, col_2, or col_3 as the target.
A default_value can be set for each mapping_name that will be used when there is no match between the input and mapping.

Example code_mapping.csv

+------------+----------+----------+-------------+--------------+--------+-------+-------+
|mapping_name|valid_from|  valid_to|default_value|         col_0|   col_1|  col_2|  col_3|
+------------+----------+----------+-------------+--------------+--------+-------+-------+
| professions|2021-01-01|2021-12-31|         null|    M_Attribute_1|3000|      M|      A|
| professions|2021-01-01|2021-12-31|         null|  F_Attribute_2|3000|      W|      A|
| professions|2021-01-01|2021-12-31|         null|     M_Attribute_3|2000|      M|      B|
| professions|2021-01-01|2021-12-31|         null|   F_Attribute_3|2000|      W|      B|
| professions|2021-01-01|2021-12-31|         null|  M_Attribute_4|4000|      M|      D|
| professions|2021-01-01|2021-12-31|         null|F_Attribute_4|4000|      W|      D|
| professions|2021-01-01|2021-12-31|         null|        M_Attribute_5|5000|      M|      D|
| professions|2021-01-01|2021-12-31|         null|      F_Attribute_5|5000|      W|      D|
| professions|2021-01-01|2021-12-31|         true|       Unknown| Unknown|Unknown|Unknown|
+------------+----------+----------+-------------+--------------+--------+-------+-------+


## Functions

### ConfigReader: Reads the code mapping csv from a given path (for example on the datalake)
code_mapping_path = 'tests/files/code_mapping.csv'
config = ConfigReader(code_mapping_path)

### get_code_mapping: Displays the selection of valid values from the code_mapping.csv 
config.get_code_mapping(mapping_name)

### CodeMapper: Class that uses valid values from the code_mapping.csv as the foundation for the mapping
mapping = config.get_code_mapping('professions')
cm = CodeMapper(mapping)

### apply_mapping: Applies the mapping function to a given Spark DataFrame
apply_mapping(df, df_input=[], df_target='output', map_input=['col_1', 'col_2', 'col_3'], map_target='col_0', return_input = False) 

Args:
    df (Spark DataFrame): The Spark DataFrame to which the mapping will be applied
    df_input (list, optional): One or multiple column names in the input dataframe that will be used as the input to the mapping. Defaults to [].
    df_target (str, optional): The column name to which the mapping output will be written. Defaults to 'output'.
    map_input (list, optional): One, two, or three mapping input columns, which will be used to apply the mapping. Defaults to ['col_1', 'col_2', 'col_3'].
    map_target (str, optional): A single mapping column that will be used as the mapping output. Defaults to 'col_0'.
    return_input (bool, optional): If set to True, the input is returned along with the target mapping. If set to False, only the target mapping is returned. Defaults to False.
            


## Usage

### necessary imports
from code_lib_spark import ConfigReader, CodeMapper

### Sample Input Spark DataFrame df

+---+----------+------------+------------+------+------------+--------------+------+---------------+
| id|claim_type|date_occured|     comment|amount|loss_country|professioncode|gender|profession_type|
+---+----------+------------+------------+------+------------+--------------+------+---------------+
|  1|         2|  10.01.2021|       Test1|    50|          CH|      3000|     M|              A|
|  2|         2|  11.01.2021|       Test2|   100|          CH|      3000|     W|              A|
|  3|         2|  12.01.2021|       Test3|   150|          EN|      2000|     M|              A|
|  4|         3|  13.01.2021|       Test4|   200|          EN|      2000|     W|              A|
|  5|         4|  14.01.2021|       Test5|   250|          EN|      2000|     W|              B|
|  6|         4|  14.01.2021|Default Test|   250|          EN|         x|     W|              A|
|  7|         4|  14.01.2021|      Defaut|   250|        blub|         x|     W|              A|
|  8|         3|  13.01.2021|       Test4|   200|          EN|      4000|     W|              C|
|  9|         4|  14.01.2021|       Test5|   250|          EN|      4000|     W|              C|
| 10|         3|  13.01.2021|       Test4|   200|          EN|      5000|     W|              C|
| 11|         4|  14.01.2021|       Test5|   250|          EN|      5000|     W|              C|
|  8|         3|  13.01.2021|       Test4|   200|          EN|      4000|     W|              B|
|  9|         4|  14.01.2021|       Test5|   250|          EN|      4000|     W|              B|
| 10|         3|  13.01.2021|       Test4|   200|          EN|      5000|     W|              A|
| 11|         4|  14.01.2021|       Test5|   250|          EN|      5000|     W|              A|
+---+----------+------------+------------+------+------------+--------------+------+---------------+


### return code_mapping.csv for a given mapping_name
config.get_code_mapping('professions').show()

returns:
+------------+----------+----------+-------------+--------------+--------+-------+-------+
|mapping_name|valid_from|  valid_to|default_value|         col_0|   col_1|  col_2|  col_3|
+------------+----------+----------+-------------+--------------+--------+-------+-------+
| professions|2021-01-01|2021-12-31|         null|    M_Attribute_1|3000|  M|      A|
| professions|2021-01-01|2021-12-31|         null|  F_Attribute_2|3000|  W|      A|
| professions|2021-01-01|2021-12-31|         null|     M_Attribute_3|2000|  M|      B|
| professions|2021-01-01|2021-12-31|         null|   F_Attribute_3|2000|  W|      B|
| professions|2021-01-01|2021-12-31|         null|  M_Attribute_4|4000|  M|      D|
| professions|2021-01-01|2021-12-31|         null|F_Attribute_4|4000|  W|      D|
| professions|2021-01-01|2021-12-31|         null|        M_Attribute_5|5000|  M|      D|
| professions|2021-01-01|2021-12-31|         null|      F_Attribute_5|5000|  W|      D|
| professions|2021-01-01|2021-12-31|         true|       Unknown| Unknown|Unknown|Unknown|
+------------+----------+----------+-------------+--------------+--------+-------+-------+

### apply simple 1:1 mapping and return Spark dataframe with the mapped values   
df_1_col = df.select('professioncode')
cm = CodeMapper(config.get_code_mapping('professions'))
df_output = cm.apply_mapping(df_1_col)
df_output.show()

returns:
+--------------+
|        output|
+--------------+
|  F_Attribute_2|
|  F_Attribute_2|
|   F_Attribute_3|
|   F_Attribute_3|
|   F_Attribute_3|
|       Unknown|
|       Unknown|
|F_Attribute_4|
|F_Attribute_4|
|      F_Attribute_5|
|      F_Attribute_5|
|F_Attribute_4|
|F_Attribute_4|
|      F_Attribute_5|
|      F_Attribute_5|
+--------------+

### apply 1:1 mapping and return Spark dataframe with the mapped values   
cm = CodeMapper(config.get_code_mapping('professions'))
df_output = cm.apply_mapping(df, 
            df_input=['professioncode'], 
            map_input=['col_1'])
df_output.show()

+--------------+
|        output|
+--------------+
|  F_Attribute_2|
|  F_Attribute_2|
|   F_Attribute_3|
|   F_Attribute_3|
|   F_Attribute_3|
|       Unknown|
|       Unknown|
|F_Attribute_4|
|F_Attribute_4|
|      F_Attribute_5|
|      F_Attribute_5|
|F_Attribute_4|
|F_Attribute_4|
|      F_Attribute_5|
|      F_Attribute_5|
+--------------+

### apply 2:1 mapping and return Spark dataframe with the mapped values   
cm = CodeMapper(config.get_code_mapping('professions'))
df_output = cm.apply_mapping(df, 
            df_input=['professioncode', 'gender'], 
            map_input=['col_1', 'col_2'])
df_output.show()

returns:
+--------------+
|        output|
+--------------+
|  F_Attribute_2|
|  F_Attribute_2|
|   F_Attribute_3|
|   F_Attribute_3|
|   F_Attribute_3|
|       Unknown|
|       Unknown|
|F_Attribute_4|
|F_Attribute_4|
|      F_Attribute_5|
|      F_Attribute_5|
|F_Attribute_4|
|F_Attribute_4|
|      F_Attribute_5|
|      F_Attribute_5|
+--------------+

### apply 3:1 mapping and return Spark dataframe with the mapped values   
cm = CodeMapper(config.get_code_mapping('professions'))
df_output = cm.apply_mapping(df, 
            df_input=['professioncode', 'gender', 'profession_type'], 
            map_input=['col_1', 'col_2', 'col_3'])
df_output.show()

returns:
+------------+
|      output|
+------------+
|  M_Attribute_1|
|F_Attribute_2|
|     Unknown|
|     Unknown|
| F_Attribute_3|
|     Unknown|
|     Unknown|
|     Unknown|
|     Unknown|
|     Unknown|
|     Unknown|
|     Unknown|
|     Unknown|
|     Unknown|
|     Unknown|
+------------+

### apply 3:1 mapping and return Spark dataframe with the mapped values attached    
cm = CodeMapper(config.get_code_mapping('professions'))
df_output = cm.apply_mapping(df, 
            df_input=['professioncode', 'gender', 'profession_type'], 
            map_input=['col_1', 'col_2', 'col_3'],
            return_input=True)
df_output.show()

returns:
+---+----------+------------+------------+------+------------+--------------+------+---------------+------------+
| id|claim_type|date_occured|     comment|amount|loss_country|professioncode|gender|profession_type|      output|
+---+----------+------------+------------+------+------------+--------------+------+---------------+------------+
|  1|         2|  10.01.2021|       Test1|    50|          CH|      3000|     M|              A|  M_Attribute_1|
|  2|         2|  11.01.2021|       Test2|   100|          CH|      3000|     W|              A|F_Attribute_2|
|  3|         2|  12.01.2021|       Test3|   150|          EN|      2000|     M|              A|     Unknown|
|  4|         3|  13.01.2021|       Test4|   200|          EN|      2000|     W|              A|     Unknown|
|  5|         4|  14.01.2021|       Test5|   250|          EN|      2000|     W|              B| F_Attribute_3|
|  6|         4|  14.01.2021|Default Test|   250|          EN|             x|     W|              A|     Unknown|
|  7|         4|  14.01.2021|      Defaut|   250|        blub|             x|     W|              A|     Unknown|
|  8|         3|  13.01.2021|       Test4|   200|          EN|      4000|     W|              C|     Unknown|
|  9|         4|  14.01.2021|       Test5|   250|          EN|      4000|     W|              C|     Unknown|
| 10|         3|  13.01.2021|       Test4|   200|          EN|      5000|     W|              C|     Unknown|
| 11|         4|  14.01.2021|       Test5|   250|          EN|      5000|     W|              C|     Unknown|
|  8|         3|  13.01.2021|       Test4|   200|          EN|      4000|     W|              B|     Unknown|
|  9|         4|  14.01.2021|       Test5|   250|          EN|      4000|     W|              B|     Unknown|
| 10|         3|  13.01.2021|       Test4|   200|          EN|      5000|     W|              A|     Unknown|
| 11|         4|  14.01.2021|       Test5|   250|          EN|      5000|     W|              A|     Unknown|
+---+----------+------------+------------+------+------------+--------------+------+---------------+------------+

### specify a custom output target   
cm = CodeMapper(config.get_code_mapping('professions'))
df_output = cm.apply_mapping(df, 
            df_input=['professioncode'], 
            map_input=['col_1'],
            df_target='new_column_name')
df_output.show()

returns:
+---------------+
|new_column_name|
+---------------+
|   F_Attribute_2|
|   F_Attribute_2|
|    F_Attribute_3|
|    F_Attribute_3|
|    F_Attribute_3|
|        Unknown|
|        Unknown|
| F_Attribute_4|
| F_Attribute_4|
|       F_Attribute_5|
|       F_Attribute_5|
| F_Attribute_4|
| F_Attribute_4|
|       F_Attribute_5|
|       F_Attribute_5|
+---------------+

### specify a custom map_target   
cm = CodeMapper(config.get_code_mapping('professions'))
df_output = cm.apply_mapping(df, 
            df_input=['professioncode'], 
            map_input=['col_1'],
            map_target='col_2')
df_output.show()

returns:
+-------+
| output|
+-------+
|      W|
|      W|
|      W|
|      W|
|      W|
|Unknown|
|Unknown|
|      W|
|      W|
|      W|
|      W|
|      W|
|      W|
|      W|
|      W|
+-------+


## Possible Errors
ValueError: Error 300: The function recognizes the mapping task as a 1:1 mapping. However, the given value for map_input is not in the list of allowed values, which are 'col_0', 'col_1', 'col_2', 'col_3'.
ValueError: Error 301: The mapping function can handle up to 3 input values, but more than 3 input df values were given.
ValueError: Error 302: The mapping function can handle up to 3 input values, but more than 3 mapping values were given.
ValueError: Error 303: If mapping inputs and dataframe inputs are manually specified, they need to have the same number of arguments.
ValueError: Error 400: The function recognizes the mapping task as a 2:1 mapping. However, there is a mismatch between the number of columns in the dataframe and the map_input.
ValueError: Error 500: The function recognizes the mapping task as a 2:1 mapping. However, there is a mismatch between the number of columns in the dataframe and the map_input.
ValueError: Error 600: The function recognizes the mapping task as a 3:1 mapping. However, there is a mismatch between the number of columns in the dataframe and the map_input.
ValueError: Error 700: There is a mismatch between the number of defined mapping columns and the number of defined input columns



# Commands for Developers

## Run all tests
pytest -v -k test_

## Run all tests for 2 inputs
pytest -v -k test_2

## Build and create wheel file
python setup.py bdist_wheel  


