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

# Extract the custodian section
custodian = root.find('.//hl7:custodian', namespaces)
assigned_custodian = custodian.find('.//hl7:assignedCustodian', namespaces)
represented_custodian_org = assigned_custodian.find('.//hl7:representedCustodianOrganization', namespaces)

custodian_id = represented_custodian_org.find('.//hl7:id', namespaces).attrib.get('extension') if represented_custodian_org.find('.//hl7:id', namespaces) is not None else None
custodian_root = represented_custodian_org.find('.//hl7:id', namespaces).attrib.get('root') if represented_custodian_org.find('.//hl7:id', namespaces) is not None else None
custodian_name = represented_custodian_org.find('.//hl7:name', namespaces).text if represented_custodian_org.find('.//hl7:name', namespaces) is not None else None

addr = represented_custodian_org.find('.//hl7:addr', namespaces)
street_address_line = addr.find('.//hl7:streetAddressLine', namespaces).text if addr.find('.//hl7:streetAddressLine', namespaces) is not None else None
city = addr.find('.//hl7:city', namespaces).text if addr.find('.//hl7:city', namespaces) is not None else None
state = addr.find('.//hl7:state', namespaces).text if addr.find('.//hl7:state', namespaces) is not None else None
postal_code = addr.find('.//hl7:postalCode', namespaces).text if addr.find('.//hl7:postalCode', namespaces) is not None else None
country = addr.find('.//hl7:country', namespaces).text if addr.find('.//hl7:country', namespaces) is not None else None

telecom = represented_custodian_org.find('.//hl7:telecom', namespaces)
telecom_use = telecom.attrib.get('use') if telecom is not None else None
telecom_value = telecom.attrib.get('value') if telecom is not None else None

# Get the current date and time
insert_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

# Create a dictionary for the custodian data
custodian_data = {
    "CUSTODIAN_ID": custodian_id,
    "CUSTODIAN_ROOT": custodian_root,
    "CUSTODIAN_NAME": custodian_name,
    "CUSTODIAN_STREET_ADDRESS_LINE": street_address_line,
    "CUSTODIAN_CITY": city,
    "CUSTODIAN_STATE": state,
    "CUSTODIAN_POSTAL_CODE": postal_code,
    "CUSTODIAN_COUNTRY": country,
    "CUSTODIAN_TELECOM_USE": telecom_use,
    "CUSTODIAN_TELECOM_VALUE": telecom_value,
    "FILE_NAME": file_name,
    "INSERT_DATETIME": insert_datetime
}

# Create a DataFrame for the custodian section
df_custodian = pd.DataFrame([custodian_data])

# Print the DataFrame for the custodian section
print(df_custodian)

# Define SQL Server connection details
server = 'Samanth'
database = 'ClinicalDocument'
schema = 'cdg'
table_name = 'Custodian'

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
        [CUSTODIAN_ID] NVARCHAR(MAX),
        [CUSTODIAN_ROOT] NVARCHAR(MAX),
        [CUSTODIAN_NAME] NVARCHAR(MAX),
        [CUSTODIAN_STREET_ADDRESS_LINE] NVARCHAR(MAX),
        [CUSTODIAN_CITY] NVARCHAR(MAX),
        [CUSTODIAN_STATE] NVARCHAR(MAX),
        [CUSTODIAN_POSTAL_CODE] NVARCHAR(MAX),
        [CUSTODIAN_COUNTRY] NVARCHAR(MAX),
        [CUSTODIAN_TELECOM_USE] NVARCHAR(MAX),
        [CUSTODIAN_TELECOM_VALUE] NVARCHAR(MAX),
        [FILE_NAME] NVARCHAR(MAX),
        [INSERT_DATETIME] NVARCHAR(MAX)
    )
END
""")

# Ensure all columns from DataFrame are present in the table
add_missing_columns(cursor, schema, table_name, df_custodian.columns)

# Truncate the table if it has rows
cursor.execute(f"""
IF EXISTS (SELECT 1 FROM [{schema}].[{table_name}])
BEGIN
    TRUNCATE TABLE [{schema}].[{table_name}]
END
""")

# Dynamically generate the insert statement based on DataFrame columns
columns_str = ', '.join(f'[{col}]' for col in df_custodian.columns)
values_str = ', '.join('?' for _ in df_custodian.columns)

insert_sql = f"""
INSERT INTO [{schema}].[{table_name}] ({columns_str})
VALUES ({values_str})
"""

# Insert DataFrame to SQL Server
for index, row in df_custodian.iterrows():
    cursor.execute(insert_sql, tuple(row))

# Commit the transaction
conn.commit()

# Close the connection
conn.close()

print('\nData written to SQL Server successfully.')
