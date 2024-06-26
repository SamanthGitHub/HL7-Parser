import xml.etree.ElementTree as ET
import pandas as pd
from datetime import datetime
import os
import pyodbc

# Define the file path
file_path = 'G:/samanth473_drive/CDP/CDA-phcaserpt-1.3.0-CDA-phcaserpt-1.3.1/examples/samples/CDAR2_IG_PHCASERPT_R2_STU3.1_SAMPLE.xml'

# Extract the file name from the file path
file_name = os.path.basename(file_path)

# Load and parse the XML file
tree = ET.parse(file_path)
root = tree.getroot()

# Define the namespace map
namespaces = {
    'voc': 'http://www.lantanagroup.com/voc',
    'hl7': 'urn:hl7-org:v3',
    'sdtc': 'urn:hl7-org:sdtc',
    'xsi': 'http://www.w3.org/2001/XMLSchema-instance'
}

# Extract the relatedDocument section
related_document = root.find('.//hl7:relatedDocument', namespaces)
type_code = related_document.attrib.get('typeCode') if related_document is not None else None

parent_document = related_document.find('.//hl7:parentDocument', namespaces)

doc_id = parent_document.find('.//hl7:id', namespaces)
doc_id_root = doc_id.attrib.get('root') if doc_id is not None else None
doc_id_extension = doc_id.attrib.get('extension') if doc_id is not None else None

set_id = parent_document.find('.//hl7:setId', namespaces)
set_id_root = set_id.attrib.get('root') if set_id is not None else None
set_id_extension = set_id.attrib.get('extension') if set_id is not None else None

version_number = parent_document.find('.//hl7:versionNumber', namespaces)
version_value = version_number.attrib.get('value') if version_number is not None else None

# Create a dictionary for the relatedDocument data
related_document_data = {
    "RELATED_DOC_TYPE_CODE": type_code,
    "RELATED_DOC_ID_ROOT": doc_id_root,
    "RELATED_DOC_ID_EXTENSION": doc_id_extension,
    "RELATED_DOC_SET_ID_ROOT": set_id_root,
    "RELATED_DOC_SET_ID_EXTENSION": set_id_extension,
    "RELATED_DOC_VERSION_NUMBER": version_value,
    "FILE_NAME": file_name,
    "INSERT_DATETIME": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
}

# Create a DataFrame for the relatedDocument section
df_related_document = pd.DataFrame([related_document_data])

# Print the DataFrame
print(df_related_document)

# Define SQL Server connection details
server = 'Samanth'
database = 'ClinicalDocument'
schema = 'cdg'
table_name = 'RelatedDocument'

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
        [RELATED_DOC_TYPE_CODE] NVARCHAR(MAX),
        [RELATED_DOC_ID_ROOT] NVARCHAR(MAX),
        [RELATED_DOC_ID_EXTENSION] NVARCHAR(MAX),
        [RELATED_DOC_SET_ID_ROOT] NVARCHAR(MAX),
        [RELATED_DOC_SET_ID_EXTENSION] NVARCHAR(MAX),
        [RELATED_DOC_VERSION_NUMBER] NVARCHAR(MAX),
        [FILE_NAME] NVARCHAR(MAX),
        [INSERT_DATETIME] NVARCHAR(MAX)
    )
END
""")

# Ensure all columns from DataFrame are present in the table
add_missing_columns(cursor, schema, table_name, df_related_document.columns)

# Truncate the table if it has rows
cursor.execute(f"""
IF EXISTS (SELECT 1 FROM [{schema}].[{table_name}])
BEGIN
    TRUNCATE TABLE [{schema}].[{table_name}]
END
""")

# Dynamically generate the insert statement based on DataFrame columns
columns_str = ', '.join(f'[{col}]' for col in df_related_document.columns)
values_str = ', '.join('?' for _ in df_related_document.columns)

insert_sql = f"""
INSERT INTO [{schema}].[{table_name}] ({columns_str})
VALUES ({values_str})
"""

# Insert DataFrame to SQL Server
for index, row in df_related_document.iterrows():
    cursor.execute(insert_sql, tuple(row))

# Commit the transaction
conn.commit()

# Close the connection
conn.close()

print('\nData written to SQL Server successfully.')
