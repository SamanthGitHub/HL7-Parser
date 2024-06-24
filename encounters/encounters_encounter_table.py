import xml.etree.ElementTree as ET
import pandas as pd
from datetime import datetime
from tabulate import tabulate
import pyodbc
import os

# Define the file path
file_path = 'G:/samanth473_drive/CDP/CDA-phcaserpt-1.3.0-CDA-phcaserpt-1.3.1/examples/samples/CDAR2_IG_PHCASERPT_R2_STU3.1_SAMPLE.xml'

# Extract the file name from the file path
file_name = os.path.basename(file_path)

# Load and parse the XML file
tree = ET.parse(file_path)
root = tree.getroot()

# Define the namespace
ns = {'n1': 'urn:hl7-org:v3'}

# Find the "Encounters" section
encounters_section = root.find(".//n1:section[n1:title='Encounters']", ns)

# Extract encounter data
encounter_data = []
source = "Encounters"
insert_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

# Function to extract text or return empty string if not found
def get_text_or_default(element, xpath, ns, default=""):
    found = element.find(xpath, ns)
    return found.text if found is not None else default

# Loop through each tr in the Encounters section
for tr in encounters_section.findall(".//n1:tr", ns):
    tds = tr.findall("n1:td", ns)
    if len(tds) == 3 and tds[2].find("n1:list", ns) is not None:
        encounter = get_text_or_default(tds[0], ".", ns)
        date = get_text_or_default(tds[1], ".", ns)
        location = get_text_or_default(tds[2], "n1:list/n1:item/n1:content", ns)
        encounter_data.append([encounter, date, location, source, file_name, insert_datetime])

# Create DataFrame
encounter_df = pd.DataFrame(encounter_data, columns=["ENCOUNTER", "ENCOUNTER_DATE", "ENCOUNTER_LOCATION", "SOURCE", "FILE_NAME", "INSERT_DATETIME"])

# Print DataFrame as table
print(tabulate(encounter_df, headers='keys', tablefmt='psql'))

# Define SQL Server connection details
server = 'Samanth'
database = 'ClinicalDocument'
schema = 'cdg'
table_name = 'Encounters'

# Establish connection to SQL Server
conn = pyodbc.connect(f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};Trusted_Connection=yes;')
cursor = conn.cursor()

# Function to get the current columns of a table
def get_current_columns(cursor, schema, table_name):
    cursor.execute(f"""
    SELECT COLUMN_NAME 
    FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE TABLE_SCHEMA = '{schema}' AND TABLE_NAME = '{table_name}'
    """)
    return [row.COLUMN_NAME for row in cursor.fetchall()]

# Function to add missing columns to the table
def add_missing_columns(cursor, schema, table_name, columns):
    existing_columns = get_current_columns(cursor, schema, table_name)
    for column in columns:
        if column not in existing_columns:
            cursor.execute(f"ALTER TABLE [{schema}].[{table_name}] ADD [{column}] NVARCHAR(MAX)")

# Check if table exists, create if not
cursor.execute(f"""
IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES 
               WHERE TABLE_SCHEMA = '{schema}' AND TABLE_NAME = '{table_name}')
BEGIN
    CREATE TABLE [{schema}].[{table_name}] (
        [ID] INT IDENTITY(1,1) PRIMARY KEY,
        [ENCOUNTER] NVARCHAR(MAX),
        [ENCOUNTER_DATE] NVARCHAR(MAX),
        [ENCOUNTER_LOCATION] NVARCHAR(MAX),
        [SOURCE] NVARCHAR(MAX),
        [FILE_NAME] NVARCHAR(MAX),
        [INSERT_DATETIME] NVARCHAR(MAX)
    )
END
""")

# Ensure all columns from DataFrame are present in the table
add_missing_columns(cursor, schema, table_name, encounter_df.columns)

# Truncate the table if it has rows
cursor.execute(f"""
IF EXISTS (SELECT 1 FROM [{schema}].[{table_name}])
BEGIN
    TRUNCATE TABLE [{schema}].[{table_name}]
END
""")

# Dynamically generate the insert statement based on DataFrame columns
columns_str = ', '.join(f'[{col}]' for col in encounter_df.columns)
values_str = ', '.join('?' for _ in encounter_df.columns)

insert_sql = f"""
INSERT INTO [{schema}].[{table_name}] ({columns_str})
VALUES ({values_str})
"""

# Insert DataFrame to SQL Server
for index, row in encounter_df.iterrows():
    cursor.execute(insert_sql, tuple(row))

# Commit the transaction
conn.commit()

# Close the connection
conn.close()

print('\nData written to SQL Server successfully.')
