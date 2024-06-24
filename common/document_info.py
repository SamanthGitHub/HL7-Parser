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

# Extract information
realm_code = root.find('.//hl7:realmCode', namespaces).attrib.get('code')
type_id = root.find('.//hl7:typeId', namespaces)
type_id_extension = type_id.attrib.get('extension')
type_id_root = type_id.attrib.get('root')
template_ids = root.findall('.//hl7:templateId', namespaces)
unique_document_id = root.find('.//hl7:id[@extension]', namespaces).attrib.get('extension')
unique_document_root = root.find('.//hl7:id[@root]', namespaces).attrib.get('root')
document_code = root.find('.//hl7:code', namespaces)
document_title = root.find('.//hl7:title', namespaces).text
effective_time = root.find('.//hl7:effectiveTime', namespaces).attrib.get('value')
confidentiality_code = root.find('.//hl7:confidentialityCode', namespaces)
language_code = root.find('.//hl7:languageCode', namespaces).attrib.get('code')
set_id = root.find('.//hl7:setId', namespaces)
version_number = root.find('.//hl7:versionNumber', namespaces).attrib.get('value')

# Get the current date and time
insert_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

# Create a dictionary for the data
data = {
    "REALM_CODE": realm_code,
    "TYPE_ID_EXTENSION": type_id_extension,
    "TYPE_ID_ROOT": type_id_root,
    "TEMPLATE_ID_ROOT_1": template_ids[0].attrib.get('root') if len(template_ids) > 0 else None,
    "TEMPLATE_ID_EXTENSION_1": template_ids[0].attrib.get('extension') if len(template_ids) > 0 else None,
    "TEMPLATE_ID_ROOT_2": template_ids[1].attrib.get('root') if len(template_ids) > 1 else None,
    "TEMPLATE_ID_EXTENSION_2": template_ids[1].attrib.get('extension') if len(template_ids) > 1 else None,
    "TEMPLATE_ID_ROOT_3": template_ids[2].attrib.get('root') if len(template_ids) > 2 else None,
    "TEMPLATE_ID_EXTENSION_3": template_ids[2].attrib.get('extension') if len(template_ids) > 2 else None,
    "UNIQUE_DOCUMENT_ID": unique_document_id,
    "UNIQUE_DOCUMENT_ROOT": unique_document_root,
    "DOCUMENT_CODE": document_code.attrib.get('code'),
    "DOCUMENT_CODE_SYSTEM": document_code.attrib.get('codeSystem'),
    "DOCUMENT_CODE_DISPLAY_NAME": document_code.attrib.get('displayName'),
    "DOCUMENT_CODE_SYSTEM_NAME": document_code.attrib.get('codeSystemName'),
    "DOCUMENT_TITLE": document_title,
    "EFFECTIVE_TIME": effective_time,
    "CONFIDENTIALITY_CODE": confidentiality_code.attrib.get('code'),
    "CONFIDENTIALITY_CODE_SYSTEM": confidentiality_code.attrib.get('codeSystem'),
    "CONFIDENTIALITY_CODE_DISPLAY_NAME": confidentiality_code.attrib.get('displayName'),
    "LANGUAGE_CODE": language_code,
    "SET_ID_ROOT": set_id.attrib.get('root'),
    "SET_ID_EXTENSION": set_id.attrib.get('extension'),
    "VERSION_NUMBER": version_number,
    "FILE_NAME": file_name,
    "INSERT_DATETIME": insert_datetime
}

# Create a DataFrame
df = pd.DataFrame([data])

# Print the DataFrame
print(df)

# Define SQL Server connection details
server = 'Samanth'
database = 'ClinicalDocument'
schema = 'cdg'
table_name = 'DocumentDetails'

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
        [REALM_CODE] NVARCHAR(MAX),
        [TYPE_ID_EXTENSION] NVARCHAR(MAX),
        [TYPE_ID_ROOT] NVARCHAR(MAX),
        [TEMPLATE_ID_ROOT_1] NVARCHAR(MAX),
        [TEMPLATE_ID_EXTENSION_1] NVARCHAR(MAX),
        [TEMPLATE_ID_ROOT_2] NVARCHAR(MAX),
        [TEMPLATE_ID_EXTENSION_2] NVARCHAR(MAX),
        [TEMPLATE_ID_ROOT_3] NVARCHAR(MAX),
        [TEMPLATE_ID_EXTENSION_3] NVARCHAR(MAX),
        [UNIQUE_DOCUMENT_ID] NVARCHAR(MAX),
        [UNIQUE_DOCUMENT_ROOT] NVARCHAR(MAX),
        [DOCUMENT_CODE] NVARCHAR(MAX),
        [DOCUMENT_CODE_SYSTEM] NVARCHAR(MAX),
        [DOCUMENT_CODE_DISPLAY_NAME] NVARCHAR(MAX),
        [DOCUMENT_CODE_SYSTEM_NAME] NVARCHAR(MAX),
        [DOCUMENT_TITLE] NVARCHAR(MAX),
        [EFFECTIVE_TIME] NVARCHAR(MAX),
        [CONFIDENTIALITY_CODE] NVARCHAR(MAX),
        [CONFIDENTIALITY_CODE_SYSTEM] NVARCHAR(MAX),
        [CONFIDENTIALITY_CODE_DISPLAY_NAME] NVARCHAR(MAX),
        [LANGUAGE_CODE] NVARCHAR(MAX),
        [SET_ID_ROOT] NVARCHAR(MAX),
        [SET_ID_EXTENSION] NVARCHAR(MAX),
        [VERSION_NUMBER] NVARCHAR(MAX),
        [FILE_NAME] NVARCHAR(MAX),
        [INSERT_DATETIME] NVARCHAR(MAX)
    )
END
""")

# Ensure all columns from DataFrame are present in the table
add_missing_columns(cursor, schema, table_name, df.columns)

# Truncate the table if it has rows
cursor.execute(f"""
IF EXISTS (SELECT 1 FROM [{schema}].[{table_name}])
BEGIN
    TRUNCATE TABLE [{schema}].[{table_name}]
END
""")

# Dynamically generate the insert statement based on DataFrame columns
columns_str = ', '.join(f'[{col}]' for col in df.columns)
values_str = ', '.join('?' for _ in df.columns)

insert_sql = f"""
INSERT INTO [{schema}].[{table_name}] ({columns_str})
VALUES ({values_str})
"""

# Insert DataFrame to SQL Server
for index, row in df.iterrows():
    cursor.execute(insert_sql, tuple(row))

# Commit the transaction
conn.commit()

# Close the connection
conn.close()

print('\nData written to SQL Server successfully.')
