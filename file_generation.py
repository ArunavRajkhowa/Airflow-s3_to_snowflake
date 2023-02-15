import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# Create a DataFrame with 10 rows of data
df = pd.DataFrame({'customer_name': ['John', 'Jane', 'Jim', 'Joan', 'Jake', 'Jill', 'Jack', 'Julie', 'Jade', 'Jasmine'],
                   'place': ['Mumbai', 'Delhi', 'Bangalore', 'Hyderabad', 'Chennai', 'Kolkata', 'Ahmedabad', 'Pune', 'Surat', 'Jaipur'],
                   'purchased_amount': [10000, 20000, 15000, 25000, 17500, 22000, 19500, 18000, 16500, 21000]})


# Write the DataFrame to a JSON file
df.to_json('data.json', orient='records')

# Convert the DataFrame to a Pyarrow table
table = pa.Table.from_pandas(df)

# Write the Pyarrow table to a Parquet file
pq.write_table(table, 'data.parquet')
