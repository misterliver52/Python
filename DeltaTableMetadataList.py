import pandas as pd
from delta import DeltaTable
import os

# Specify the root path of the data lake
root_path = "path_to_data_lake_root"

# Create an empty list to store the metadata for all Delta tables
metadata_list = []

# Loop through all the directories in the root path
for dirpath, dirnames, filenames in os.walk(root_path):
    for filename in filenames:
        if filename.endswith(".parquet") and "_delta_" in dirpath:
            # Initialize the DeltaTable object
            delta_table = DeltaTable.forPath(spark, dirpath)
            
            # Get the schema of the Delta table
            table_schema = delta_table.schema()

            # Get the column names, data types, and nullability of the Delta table
            column_names = table_schema.fieldNames()
            column_data_types = [str(field.dataType) for field in table_schema.fields]
            column_nullable = [str(field.nullable) for field in table_schema.fields]

            # Get the table comments
            table_comments = delta_table.properties()["comment"]

            # Get the column comments
            column_comments = [field.metadata["comment"] if "comment" in field.metadata else "" for field in table_schema.fields]

            # Combine the metadata into a list of tuples
            metadata_list.append((dirpath, filename, column_names, column_data_types, column_nullable, column_comments, table_comments))

# Create a Pandas DataFrame from the metadata list
metadata_df = pd.DataFrame(metadata_list, columns=["Directory Path", "File Name", "Column Names", "Data Types", "Nullable", "Column Comments", "Table Comments"])

# Print the DataFrame
print(metadata_df)
