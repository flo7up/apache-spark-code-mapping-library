from datetime import datetime
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import col, when, isnull, udf


class ConfigReader:
    def __init__(self, code_mapping_path, spark=None):
        """A utility function that reads the code mapping csv file.

        Args:
            code_mapping_path (str): the path to the mapping csv
            spark (SparkSession, optional): The spark session to be used. \
            Defaults to None.
        """
        if not spark:
            spark = SparkSession(SparkContext.getOrCreate())
        self.code_map = spark.read.option("inferSchema", True) \
            .option("delimiter", ";") \
            .option("header", True) \
            .option("dateFormat", "yyyy-MM-dd") \
            .csv(code_mapping_path)

    def get_code_mapping(self, mapping_name):
        """A utility function that filters the code mapping to a specific \
        mapping name and returns only valid mapping entries.

        Args:
            mapping_name (str): The category name of the mapping entries, \
            e.g., "professioncode"

        Raises:
            ValueError 1: The code mapping does not contain any entries \
            for the given mapping name
            ValueError 2: There are mapping entries for the given mapping \
            name but none of them lies within the valid period of time

        Returns:
            Spark DataFrame: A spark dataframe that cointains the filtered \
            mapping entries
        """

        """ args """
        """ source codes to target codes """
        """ errors """
        """ Error 100: no mapping found for mapping_name """
        """ Error 200: No mapping entry found for the combination of \
        mapping_name """
        code_map_filtered = self.code_map.filter(
            col("mapping_name") == mapping_name)
        today = datetime.today().strftime('%Y-%m-%d')
        code_map_valid = code_map_filtered.filter(
            (col("valid_from") < today) & (today < col("valid_to")))

        if code_map_filtered.count() < 1:
            raise ValueError(
                f'Error 100: No mapping found for the given mapping name {mapping_name}')
        if code_map_valid.count() < 1:
            raise ValueError(
                f'Error 200: No mapping entry found for the given mapping_name that have a valid period {mapping_name}')

        return code_map_valid


class CodeMapper:
    """A utility class that handles the mapping of codes
    """

    def __init__(self, code_map):
        """The init function that hands over the code map to self

        Args:
            code_map (Spark DataFrame): the code map that contains prefiltered valid entries
        """
        self.code_map = code_map

    def apply_mapping(
            self,
            df,
            df_input=[],
            df_target="output",
            map_input=[
                'col_1',
                'col_2',
                'col_3'],
            map_target="col_0",
            return_input = False):
        """The main util function that applies the mapping logic to a given Spark dataframe

        Args:
            df (Spark DataFrame): The Spark DataFrame to which the mapping will be applied
            df_input (list, optional): One or multiple column names in the input dataframe that will be used as the input to the mapping. Defaults to [].
            df_target (str, optional): The column name to which the mapping output will be written. Defaults to "output".
            map_input (list, optional): One, two, or three mapping input columns, which will be used to apply the mapping. Defaults to ['col_1', 'col_2', 'col_3'].
            map_target (str, optional): A single mapping column that will be used as the mapping output. Defaults to "col_0".
            return_input (bool, optional): If set to True, the input is returned along with the target mapping. If set to False, only the target mapping is returned. Defaults to False.
            

        Raises:
            ValueError: Error 300: The function recognizes the mapping task as a 1:1 mapping. However, the given value for map_input is not in the list of allowed values, which are 'col_0', 'col_1', 'col_2', 'col_3'.
            ValueError: Error 301: The mapping function can handle up to 3 input values, but more than 3 input df values were given.
            ValueError: Error 302: The mapping function can handle up to 3 input values, but more than 3 mapping values were given.
            ValueError: Error 303: If mapping inputs and dataframe inputs are manually specified, they need to have the same number of arguments.
            ValueError: Error 400: The function recognizes the mapping task as a 2:1 mapping. However, there is a missmatch between the number of columns in the dataframe and the map_input.
            ValueError: Error 500: The function recognizes the mapping task as a 2:1 mapping. However, there is a missmatch between the number of columns in the dataframe and the map_input.
            ValueError: Error 600: The function recognizes the mapping task as a 3:1 mapping. However, there is a missmatch between the number of columns in the dataframe and the map_input.
            ValueError: Error 700: There is a missmatch between the number of defined mapping columns and the number of defined input columns

        Returns:
            Spark DataFrame: A DataFrame that contains the input data and the mapped target values
        """

        """ args """
        """ df: the dataframe to which the  mapping is applied"""
        map_input_count = len(map_input)
        df_input_count = len(df_input)
        df_columns_count = len(df.columns)
        code_map = self.code_map
        df_input = list(df.columns) if df_input == [] else df_input
        default_mapping_values = True if map_input == ['col_1', 'col_2', 'col_3'] else False
        default_value = code_map.select(col("col_0")).filter(
            col('default_value')).head()[0]

        if (map_input[0] not in (['col_0', 'col_1', 'col_2', 'col_3'])):
            raise ValueError(
                f"Error 300: the given map_input value ({map_input[0]}) is not in the list of allowed values ('col_0', 'col_1', 'col_2', 'col_3')")
        if df_input_count > 3:
            raise ValueError(
                f"Error 301: df_input can have up to three input values, but {df_input_count} were given")
        if map_input_count > 3:
            raise ValueError(
                f"Error 302: map_input can have up to three input values, but {map_input_count} were given")
        if (df_input_count >= 1) & (default_mapping_values == False)  & (map_input_count != df_input_count):
            raise ValueError(
                f"Error 303: the number of arguments in map_input {map_input_count} does not match with the number of arguments in df_inputs {df_columns_count}")
            
        
        # 1:1 Mapping - Input: Dataframe with 1 column, 1 mapping input, only the mapping target is returned
        if (
            df_columns_count >= 1) & (
            (map_input_count == 1) | (default_mapping_values) & (
                df_input_count == 0)) | (default_mapping_values) & (
                df_input_count == 1) & (
                    df_columns_count == 1):
            # No columns defined and df has exactly two columns           
            def mapping_lookup(key1):
                target_value = mapping_dict.get(key1)
                if target_value is None:
                    return default_value
                else:
                    return target_value
            mapping_lookup_udf = udf(mapping_lookup)
            mapping_dict = {row[map_input[0]]: row[map_target] for row in code_map.collect()}
            df_final = df.withColumn(df_target, mapping_lookup_udf(col(df_input[0])))
            if return_input == True:
                return df_final
            return df_final.select(df_target)    
        # 2:1 Mapping - Input: Dataframe 2 or more columns, 2 mapping inputs
        elif (df_columns_count >= 2) & ((map_input_count == 2) |
                                        ((default_mapping_values) & (df_input_count <= 2))):
            # two column name and no custom df column names given OR two
            # columns specified for a dataframe that has two or more columns
            if (df_columns_count == 2) | (
                    (df_columns_count >= 2) & (df_input_count == 2)):
                if (default_mapping_values) | (
                    (default_mapping_values == False) & (
                        map_input_count >= 2)):
                    def mapping_lookup(key1, key2):
                        target_value = mapping_dict.get(key1 + key2)
                        if target_value is None:
                            return default_value
                        else:
                            return target_value
                    mapping_lookup_udf = udf(mapping_lookup)
                    mapping_dict = {row[map_input[0]] + row[map_input[1]]: row[map_target] for row in code_map.collect()}
                    df_final = df.withColumn(df_target, mapping_lookup_udf(col(df_input[0]), col(df_input[1])))
                    if return_input == True:
                        return df_final
                    return df_final.select(df_target)    
                else:
                    raise ValueError(
                        f'Error 400: The mapping task was recognized as a 2:1 mapping. mapping values ambiguisly defined.'
                        f'map_input_count: {map_input_count}, df_columns_count: {df_columns_count}')
            # custome df names but incorrectly defined
            else:
                raise ValueError(
                    f"Error 500: The mapping task was recognized as a 2:1 mapping. Number of defined mapping columns ({map_input_count}) "
                    f"exceeds the number of columns in the dataframe: {df_input_count}")

        # 3:1 Mapping -Input: Dataframe with 3 or mor columns, 3 mapping inputs
        elif (df_columns_count >= 3) & ((map_input_count == 3) | ((default_mapping_values) & (df_input_count <= 3))):
            # no custom df names
            if default_mapping_values | (
                (default_mapping_values == False) & (
                    map_input_count >= 3)):
                mapping_dict = {row[map_input[0]] + row[map_input[1]] +
                                row[map_input[2]]: row[map_target] for row in code_map.collect()}
                def mapping_lookup(key1, key2, key3):
                    target_value = mapping_dict.get(key1 + key2 + key3)
                    if target_value is None:
                        return default_value
                    else:
                        return target_value
                mapping_lookup_udf = udf(mapping_lookup)
                df_final = df.withColumn(df_target, mapping_lookup_udf(col(df_input[0]), col(df_input[1]), col(df_input[2])))
                if return_input == True:
                    return df_final
                return df_final.select(df_target)    
            else:
                raise ValueError(
                    "Error 600: The mapping task was recognized as a 3:1 mapping. However, the mapping values are ambiguisly defined. "
                    f'map_input_count: {map_input_count}, df_columns_count: {df_columns_count}')

        # custome df names but incorrectly defined
        else:
            raise ValueError(
                f"Error 700: encountered a mismatch between the number of defined mapping columns: {map_input_count}, "
                f"and the number of defined df input columns: {df_input_count}")
