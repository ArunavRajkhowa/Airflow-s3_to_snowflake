import snowflake.connector
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# Connect to Snowflake
con = snowflake.connector.connect(
   user='username',
   password='password',
   account='account',
   database='ANALYTICS',
   schema='PUBLIC',
   warehouse='TRANSFORMING'
)

 #Load the JSON file into Snowflake
df_json = pd.read_json("D:\Coding_Projects\Colgate_training\data.json")
print(df_json.head())


# PUT into internal stage 
cursor = con.cursor() #"D:\Coding_Projects\Colgate_training\data.json"

cursor.execute("""
   PUT file://data.json @~/
""")
print('-------------------------------------------------')
print("Result of show PUT query:")
print(cursor.fetchall())
print('-------------------------------------------------')
cursor.close()

cursor = con.cursor()
ex=cursor.execute("""
   LIST @~/
""")
print("Result of list command:")
print(cursor.fetchall())
cursor.close()

cursor = con.cursor()
cursor.execute("SHOW TABLES IN ANALYTICS.PUBLIC")
print('-------------------------------------------------')
print("Result of show tables query:")
print(cursor.fetchall())
cursor.close()


# Load the JSON data into a table, specifying the MATCH_BY_COLUMN_NAME option
cursor = con.cursor()
cursor.execute("""
   COPY INTO TRIAL
FROM @~/
FILE_FORMAT = (TYPE='JSON' )
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
""")
print('-------------------------------------------------')
print("Result of COPY INTO query:")
print(cursor.fetchall())
cursor.close()


# # Load the Parquet file into Snowflake
# table = pq.read_parquet("data.parquet").to_pandas()
# cursor = con.cursor()
# cursor.execute("""
#    PUT file://data.parquet @~/data.parquet
# """)
# cursor.execute("""
#    COPY INTO TRIAL
#    FROM @~/data.parquet

#    FILE_FORMAT = (TYPE='PARQUET')
# """)
# cursor.close()

# con.close()
