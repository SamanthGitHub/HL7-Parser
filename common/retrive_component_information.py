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
    'hl7': 'urn:hl7-org:v3',
    'sdtc': 'urn:hl7-org:sdtc',
    'xsi': 'http://www.w3.org/2001/XMLSchema-instance'
}

# Extract all component sections
components = root.findall('.//hl7:structuredBody/hl7:component', namespaces)

# Function to extract component information
def extract_component_data(component):
    section = component.find('.//hl7:section', namespaces)
    if section is not None:
        template_ids = section.findall('.//hl7:templateId', namespaces)
        template_id_1 = template_ids[0].attrib.get('root') if len(template_ids) > 0 else None
        template_id_2 = template_ids[1].attrib.get('root') if len(template_ids) > 1 else None
        template_id_2_ext = template_ids[1].attrib.get('extension') if len(template_ids) > 1 and 'extension' in template_ids[1].attrib else None
        code = section.find('.//hl7:code', namespaces)
        title = section.find('.//hl7:title', namespaces)

        component_data = {
            "TEMPLATE_ID_1_ROOT": template_id_1,
            "TEMPLATE_ID_2_ROOT": template_id_2,
            "TEMPLATE_ID_2_EXTENSION": template_id_2_ext,
            "CODE_CODE": code.attrib.get('code') if code is not None else None,
            "CODE_CODE_SYSTEM": code.attrib.get('codeSystem') if code is not None else None,
            "CODE_CODE_SYSTEM_NAME": code.attrib.get('codeSystemName') if code is not None else None,
            "CODE_DISPLAY_NAME": code.attrib.get('displayName') if code is not None else None,
            "TITLE": title.text if title is not None else None
        }
        return component_data
    return {}

# Extract data for each component
component_data_list = [extract_component_data(component) for component in components]

# Get the current date and time
insert_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

# Add file name and insert date time to each component data
for data in component_data_list:
    data['FILE_NAME'] = file_name
    data['INSERT_DATETIME'] = insert_datetime

# Create a DataFrame for the component sections
df_component = pd.DataFrame(component_data_list)

# Print the DataFrame
print(df_component)

# Define SQL Server connection details
server = 'Samanth'
database = 'ClinicalDocument'
schema = 'cdg'
table_name = 'ComponentSections'

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
        [TEMPLATE_ID_1_ROOT] NVARCHAR(MAX),
        [TEMPLATE_ID_2_ROOT] NVARCHAR(MAX),
        [TEMPLATE_ID_2_EXTENSION] NVARCHAR(MAX),
        [CODE_CODE] NVARCHAR(MAX),
        [CODE_CODE_SYSTEM] NVARCHAR(MAX),
        [CODE_CODE_SYSTEM_NAME] NVARCHAR(MAX),
        [CODE_DISPLAY_NAME] NVARCHAR(MAX),
        [TITLE] NVARCHAR(MAX),
        [FILE_NAME] NVARCHAR(MAX),
        [INSERT_DATETIME] NVARCHAR(MAX)
    )
END
""")

# Ensure all columns from DataFrame are present in the table
add_missing_columns(cursor, schema, table_name, df_component.columns)

# Truncate the table if it has rows
cursor.execute(f"""
IF EXISTS (SELECT 1 FROM [{schema}].[{table_name}])
BEGIN
    TRUNCATE TABLE [{schema}].[{table_name}]
END
""")

# Dynamically generate the insert statement based on DataFrame columns
columns_str = ', '.join(f'[{col}]' for col in df_component.columns)
values_str = ', '.join('?' for _ in df_component.columns)

insert_sql = f"""
INSERT INTO [{schema}].[{table_name}] ({columns_str})
VALUES ({values_str})
"""

# Insert DataFrame to SQL Server
for index, row in df_component.iterrows():
    cursor.execute(insert_sql, tuple(row))

# Commit the transaction
conn.commit()

# Close the connection
conn.close()

print('\nData written to SQL Server successfully.')
