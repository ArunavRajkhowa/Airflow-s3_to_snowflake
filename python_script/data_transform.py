import pandas as pd
import json

#Load the JSON file into Snowflake
df_json = pd.read_json("E:\\from_desktop\\30-days\\snowflake-learn\\Colgate_training\\data.json")
print(df_json.to_string())
print("*"*50)

# create table
cursor = con.cursor()
cursor.execute("create or replace  table temp_test (json_data  variant);")

# Define the custom format
cursor.execute("""
    create or replace file format my_json_format
    type = 'JSON'
    strip_outer_array = true;
 """)

# creating temporary internal_stage

cursor.execute("""
    create or replace stage json_temp_int_stage
  file_format = my_json_format;
 """)


# load json to internal stage
cursor.execute("put file://E:\\from_desktop\\30-days\\snowflake-learn\\Colgate_training\\data.json @json_temp_int_stage;")


# Copy the data into Target Table
cursor.execute('''copy into temp_test
    from  @json_temp_int_stage/data.json
    on_error = 'skip_file';''')

# print the content from the table
cursor.execute("select * from temp_test")

print('-------------------------------------------------')
print("Result of show tables query:")
print(cursor.fetchall())