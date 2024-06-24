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

# Extract information for the recordTarget section
record_target = root.find('.//hl7:recordTarget', namespaces)
patient_role = record_target.find('.//hl7:patientRole', namespaces)
patient_ids = patient_role.findall('.//hl7:id', namespaces)
addr = patient_role.find('.//hl7:addr', namespaces)
telecoms = patient_role.findall('.//hl7:telecom', namespaces)
patient = patient_role.find('.//hl7:patient', namespaces)
guardian = patient.find('.//hl7:guardian', namespaces)
language_communication = patient.findall('.//hl7:languageCommunication', namespaces)

# Extract the useablePeriod element and its attributes
useable_period = addr.find('.//hl7:useablePeriod', namespaces)
useable_period_type = useable_period.attrib.get('{http://www.w3.org/2001/XMLSchema-instance}type') if useable_period is not None else None
low_value = useable_period.find('.//hl7:low', namespaces).attrib.get('value') if useable_period is not None else None

# Extract patient name elements
patient_name_data = {}

for name in patient.findall('hl7:name', namespaces):
    use = name.attrib.get('use')
    given_parts = []
    given_qualifiers = []
    for given in name.findall('hl7:given', namespaces):
        if given.text:
            given_parts.append(given.text)
        if 'qualifier' in given.attrib:
            given_qualifiers.append(given.attrib['qualifier'])
    
    given = " ".join(given_parts)
    given_qualifier_value = " ".join(given_qualifiers) if given_qualifiers else None
    family = name.find('hl7:family', namespaces).text if name.find('hl7:family', namespaces) is not None else None

    # Construct keys for the dictionary based on 'use'
    patient_name_data[f'PATIENT_NAME_{use}_GIVEN'] = given
    patient_name_data[f'PATIENT_NAME_{use}_FAMILY'] = family
    
    # Add qualifiers if present
    if given_qualifier_value:
        patient_name_data[f'PATIENT_NAME_{use}_GIVEN_QUALIFIER'] = given_qualifier_value

# Extract guardian name elements
guardian_name_data = {}

for guardian_person in guardian.findall('.//hl7:guardianPerson', namespaces):
    guardian_names = guardian_person.findall('.//hl7:name', namespaces)
    for guardian_name in guardian_names:
        use = guardian_name.attrib.get('use')
        given = " ".join([g.text for g in guardian_name.findall('.//hl7:given', namespaces) if g.text is not None])
        family = guardian_name.find('.//hl7:family', namespaces).text if guardian_name.find('.//hl7:family', namespaces) is not None else None
        
        # Extract given qualifier if present
        given_qualifier = guardian_name.find('.//hl7:given[@qualifier]', namespaces)
        given_qualifier_value = given_qualifier.attrib.get('qualifier') if given_qualifier is not None else None
        
        # Extract family qualifier if present
        family_qualifier = guardian_name.find('.//hl7:family[@qualifier]', namespaces)
        family_qualifier_value = family_qualifier.attrib.get('qualifier') if family_qualifier is not None else None
        
        # Construct keys for the dictionary based on 'use'
        guardian_name_data[f'GUARDIAN_NAME_{use}_GIVEN'] = given
        guardian_name_data[f'GUARDIAN_NAME_{use}_FAMILY'] = family
        
        # Add qualifiers if present
        if given_qualifier_value:
            guardian_name_data[f'GUARDIAN_NAME_{use}_GIVEN_QUALIFIER'] = given_qualifier_value
        
        if family_qualifier_value:
            guardian_name_data[f'GUARDIAN_NAME_{use}_FAMILY_QUALIFIER'] = family_qualifier_value

# Extract telecom elements for the patient
patient_telecom_data = {"TELECOM_HP": None, "TELECOM_WP": None}
for telecom in telecoms:
    use = telecom.attrib.get('use')
    value = telecom.attrib.get('value')
    if use == 'HP':
        patient_telecom_data["TELECOM_HP"] = value
    elif use == 'WP':
        patient_telecom_data["TELECOM_WP"] = value

# Extract telecom elements for the guardian
guardian_telecoms = guardian.findall('.//hl7:telecom', namespaces)
guardian_telecom_data = {"GUARDIAN_TELECOM_HP": None, "GUARDIAN_TELECOM_EMAIL": None}
for telecom in guardian_telecoms:
    use = telecom.attrib.get('use')
    value = telecom.attrib.get('value')
    if use == 'HP':
        guardian_telecom_data["GUARDIAN_TELECOM_HP"] = value
    elif "mailto:" in value:
        guardian_telecom_data["GUARDIAN_TELECOM_EMAIL"] = value

# Extract language communication data
language_data = {}

language_communication = patient.find('.//hl7:languageCommunication', namespaces)
if language_communication is not None:
    language_code = language_communication.find('.//hl7:languageCode', namespaces)
    mode_code = language_communication.find('.//hl7:modeCode', namespaces)
    proficiency_level_code = language_communication.find('.//hl7:proficiencyLevelCode', namespaces)
    preference_ind = language_communication.find('.//hl7:preferenceInd', namespaces)
    
    language_data['LANGUAGE_CODE'] = language_code.attrib.get('code') if language_code is not None else None
    language_data['MODE_CODE'] = mode_code.attrib.get('code') if mode_code is not None else None
    language_data['MODE_CODE_SYSTEM'] = mode_code.attrib.get('codeSystem') if mode_code is not None else None
    language_data['MODE_CODE_SYSTEM_NAME'] = mode_code.attrib.get('codeSystemName') if mode_code is not None else None
    language_data['MODE_DISPLAY_NAME'] = mode_code.attrib.get('displayName') if mode_code is not None else None
    language_data['PROFICIENCY_LEVEL_CODE'] = proficiency_level_code.attrib.get('code') if proficiency_level_code is not None else None
    language_data['PROFICIENCY_LEVEL_CODE_SYSTEM'] = proficiency_level_code.attrib.get('codeSystem') if proficiency_level_code is not None else None
    language_data['PROFICIENCY_LEVEL_CODE_SYSTEM_NAME'] = proficiency_level_code.attrib.get('codeSystemName') if proficiency_level_code is not None else None
    language_data['PROFICIENCY_DISPLAY_NAME'] = proficiency_level_code.attrib.get('displayName') if proficiency_level_code is not None else None
    language_data['PREFERENCE_IND'] = preference_ind.attrib.get('value') if preference_ind is not None else None

# Extract other relevant data
administrative_gender_code = patient.find('.//hl7:administrativeGenderCode', namespaces)
race_code = patient.find('.//hl7:raceCode', namespaces)
ethnic_group_code = patient.find('.//hl7:ethnicGroupCode', namespaces)

# Get the current date and time
insert_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

# Create a dictionary for the recordTarget section data
record_target_data = {
    # Patient IDs
    "PATIENT_ID_1_EXTENSION": patient_ids[0].attrib.get('extension') if len(patient_ids) > 0 else None,
    "PATIENT_ID_1_ROOT": patient_ids[0].attrib.get('root') if len(patient_ids) > 0 else None,
    "PATIENT_ID_2_EXTENSION": patient_ids[1].attrib.get('extension') if len(patient_ids) > 1 else None,
    "PATIENT_ID_2_ROOT": patient_ids[1].attrib.get('root') if len(patient_ids) > 1 else None,

    # Patient Address
    "ADDR_USE": addr.attrib.get('use') if addr is not None else None,
    "STREET_ADDRESS_LINE": addr.find('.//hl7:streetAddressLine', namespaces).text if addr.find('.//hl7:streetAddressLine', namespaces) is not None else None,
    "CITY": addr.find('.//hl7:city', namespaces).text if addr.find('.//hl7:city', namespaces) is not None else None,
    "STATE": addr.find('.//hl7:state', namespaces).text if addr.find('.//hl7:state', namespaces) is not None else None,
    "POSTAL_CODE": addr.find('.//hl7:postalCode', namespaces).text if addr.find('.//hl7:postalCode', namespaces) is not None else None,
    "COUNTY": addr.find('.//hl7:county', namespaces).text if addr.find('.//hl7:county', namespaces) is not None else None,
    "COUNTRY": addr.find('.//hl7:country', namespaces).text if addr.find('.//hl7:country', namespaces) is not None else None,
    "USEABLE_PERIOD_TYPE": useable_period_type,
    "USEABLE_PERIOD_LOW": low_value,

    # Patient Telecom
    "TELECOM_HP": patient_telecom_data["TELECOM_HP"],
    "TELECOM_WP": patient_telecom_data["TELECOM_WP"],

    # Patient Name
    "PATIENT_NAME_L_GIVEN": patient_name_data.get('PATIENT_NAME_L_GIVEN'),
    "PATIENT_NAME_L_GIVEN_QUALIFIER": patient_name_data.get('PATIENT_NAME_L_GIVEN_QUALIFIER'),
    "PATIENT_NAME_L_FAMILY": patient_name_data.get('PATIENT_NAME_L_FAMILY'),
    "PATIENT_NAME_L_FAMILY_QUALIFIER": patient_name_data.get('PATIENT_NAME_L_FAMILY_QUALIFIER'),
    "PATIENT_NAME_A_GIVEN": patient_name_data.get('PATIENT_NAME_A_GIVEN'),
    "PATIENT_NAME_A_GIVEN_QUALIFIER": patient_name_data.get('PATIENT_NAME_A_GIVEN_QUALIFIER'),
    "PATIENT_NAME_A_FAMILY": patient_name_data.get('PATIENT_NAME_A_FAMILY'),
    "PATIENT_NAME_A_FAMILY_QUALIFIER": patient_name_data.get('PATIENT_NAME_A_FAMILY_QUALIFIER'),
    
    # Patient Demographics
    "ADMINISTRATIVE_GENDER_CODE": administrative_gender_code.attrib.get('code') if administrative_gender_code is not None else None,
    "ADMINISTRATIVE_GENDER_CODE_SYSTEM": administrative_gender_code.attrib.get('codeSystem') if administrative_gender_code is not None else None,
    "BIRTH_TIME": patient.find('.//hl7:birthTime', namespaces).attrib.get('value') if patient.find('.//hl7:birthTime', namespaces) is not None else None,
    "DECEASED_IND": patient.find('.//sdtc:deceasedInd', namespaces).attrib.get('value') if patient.find('.//sdtc:deceasedInd', namespaces) is not None else None,
    "RACE_CODE": race_code.attrib.get('code') if race_code is not None else None,
    "RACE_CODE_SYSTEM": race_code.attrib.get('codeSystem') if race_code is not None else None,
    "RACE_CODE_SYSTEM_NAME": race_code.attrib.get('codeSystemName') if race_code is not None else None,
    "RACE_DISPLAY_NAME": race_code.attrib.get('displayName') if race_code is not None else None,
    "ETHNIC_GROUP_CODE": ethnic_group_code.attrib.get('code') if ethnic_group_code is not None else None,
    "ETHNIC_GROUP_CODE_SYSTEM": ethnic_group_code.attrib.get('codeSystem') if ethnic_group_code is not None else None,
    "ETHNIC_GROUP_CODE_SYSTEM_NAME": ethnic_group_code.attrib.get('codeSystemName') if ethnic_group_code is not None else None,
    "ETHNIC_GROUP_DISPLAY_NAME": ethnic_group_code.attrib.get('displayName') if ethnic_group_code is not None else None,
    
    # Guardian Address
    "GUARDIAN_ADDR_USE": guardian.find('.//hl7:addr', namespaces).attrib.get('use') if guardian.find('.//hl7:addr', namespaces) is not None else None,
    "GUARDIAN_STREET_ADDRESS_LINE": guardian.find('.//hl7:addr//hl7:streetAddressLine', namespaces).text if guardian.find('.//hl7:addr//hl7:streetAddressLine', namespaces) is not None else None,
    "GUARDIAN_CITY": guardian.find('.//hl7:addr//hl7:city', namespaces).text if guardian.find('.//hl7:addr//hl7:city', namespaces) is not None else None,
    "GUARDIAN_STATE": guardian.find('.//hl7:addr//hl7:state', namespaces).text if guardian.find('.//hl7:addr//hl7:state', namespaces) is not None else None,
    "GUARDIAN_POSTAL_CODE": guardian.find('.//hl7:addr//hl7:postalCode', namespaces).text if guardian.find('.//hl7:addr//hl7:postalCode', namespaces) is not None else None,
    "GUARDIAN_COUNTRY": guardian.find('.//hl7:addr//hl7:country', namespaces).text if guardian.find('.//hl7:addr//hl7:country', namespaces) is not None else None,
    
    # Guardian Telecom
    "GUARDIAN_TELECOM_HP": guardian_telecom_data["GUARDIAN_TELECOM_HP"],
    "GUARDIAN_TELECOM_EMAIL": guardian_telecom_data["GUARDIAN_TELECOM_EMAIL"],
    
    # Guardian Name
    "GUARDIAN_PERSON_NAME_L_GIVEN": guardian_name_data.get('GUARDIAN_NAME_L_GIVEN'),
    "GUARDIAN_PERSON_NAME_L_GIVEN_QUALIFIER": guardian_name_data.get('GUARDIAN_NAME_L_GIVEN_QUALIFIER'),
    "GUARDIAN_PERSON_NAME_L_FAMILY": guardian_name_data.get('GUARDIAN_NAME_L_FAMILY'),
    "GUARDIAN_PERSON_NAME_L_FAMILY_QUALIFIER": guardian_name_data.get('GUARDIAN_NAME_L_FAMILY_QUALIFIER'),
    
    # Language Communication Data
    "LANGUAGE_CODE": language_data.get('LANGUAGE_CODE'),
    "MODE_CODE": language_data.get('MODE_CODE'),
    "MODE_CODE_SYSTEM": language_data.get('MODE_CODE_SYSTEM'),
    "MODE_CODE_SYSTEM_NAME": language_data.get('MODE_CODE_SYSTEM_NAME'),
    "MODE_DISPLAY_NAME": language_data.get('MODE_DISPLAY_NAME'),
    "PROFICIENCY_LEVEL_CODE": language_data.get('PROFICIENCY_LEVEL_CODE'),
    "PROFICIENCY_LEVEL_CODE_SYSTEM": language_data.get('PROFICIENCY_LEVEL_CODE_SYSTEM'),
    "PROFICIENCY_LEVEL_CODE_SYSTEM_NAME": language_data.get('PROFICIENCY_LEVEL_CODE_SYSTEM_NAME'),
    "PROFICIENCY_DISPLAY_NAME": language_data.get('PROFICIENCY_DISPLAY_NAME'),
    "PREFERENCE_IND": language_data.get('PREFERENCE_IND'),
    "FILE_NAME": file_name,
    "INSERT_DATETIME": insert_datetime
}

# Create a DataFrame for the recordTarget section
df_record_target = pd.DataFrame([record_target_data])

# Print the DataFrame for the recordTarget section
print(df_record_target)

# Define SQL Server connection details
server = 'Samanth'
database = 'ClinicalDocument'
schema = 'cdg'
table_name = 'RecordTarget'

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
        [PATIENT_ID_1_EXTENSION] NVARCHAR(MAX),
        [PATIENT_ID_1_ROOT] NVARCHAR(MAX),
        [PATIENT_ID_2_EXTENSION] NVARCHAR(MAX),
        [PATIENT_ID_2_ROOT] NVARCHAR(MAX),
        [ADDR_USE] NVARCHAR(MAX),
        [STREET_ADDRESS_LINE] NVARCHAR(MAX),
        [CITY] NVARCHAR(MAX),
        [STATE] NVARCHAR(MAX),
        [POSTAL_CODE] NVARCHAR(MAX),
        [COUNTY] NVARCHAR(MAX),
        [COUNTRY] NVARCHAR(MAX),
        [USEABLE_PERIOD_TYPE] NVARCHAR(MAX),
        [USEABLE_PERIOD_LOW] NVARCHAR(MAX),
        [TELECOM_HP] NVARCHAR(MAX),
        [TELECOM_WP] NVARCHAR(MAX),
        [PATIENT_NAME_L_GIVEN] NVARCHAR(MAX),
        [PATIENT_NAME_L_GIVEN_QUALIFIER] NVARCHAR(MAX),
        [PATIENT_NAME_L_FAMILY] NVARCHAR(MAX),
        [PATIENT_NAME_L_FAMILY_QUALIFIER] NVARCHAR(MAX),
        [PATIENT_NAME_A_GIVEN] NVARCHAR(MAX),
        [PATIENT_NAME_A_GIVEN_QUALIFIER] NVARCHAR(MAX),
        [PATIENT_NAME_A_FAMILY] NVARCHAR(MAX),
        [PATIENT_NAME_A_FAMILY_QUALIFIER] NVARCHAR(MAX),
        [ADMINISTRATIVE_GENDER_CODE] NVARCHAR(MAX),
        [ADMINISTRATIVE_GENDER_CODE_SYSTEM] NVARCHAR(MAX),
        [BIRTH_TIME] NVARCHAR(MAX),
        [DECEASED_IND] NVARCHAR(MAX),
        [RACE_CODE] NVARCHAR(MAX),
        [RACE_CODE_SYSTEM] NVARCHAR(MAX),
        [RACE_CODE_SYSTEM_NAME] NVARCHAR(MAX),
        [RACE_DISPLAY_NAME] NVARCHAR(MAX),
        [ETHNIC_GROUP_CODE] NVARCHAR(MAX),
        [ETHNIC_GROUP_CODE_SYSTEM] NVARCHAR(MAX),
        [ETHNIC_GROUP_CODE_SYSTEM_NAME] NVARCHAR(MAX),
        [ETHNIC_GROUP_DISPLAY_NAME] NVARCHAR(MAX),
        [GUARDIAN_ADDR_USE] NVARCHAR(MAX),
        [GUARDIAN_STREET_ADDRESS_LINE] NVARCHAR(MAX),
        [GUARDIAN_CITY] NVARCHAR(MAX),
        [GUARDIAN_STATE] NVARCHAR(MAX),
        [GUARDIAN_POSTAL_CODE] NVARCHAR(MAX),
        [GUARDIAN_COUNTRY] NVARCHAR(MAX),
        [GUARDIAN_TELECOM_HP] NVARCHAR(MAX),
        [GUARDIAN_TELECOM_EMAIL] NVARCHAR(MAX),
        [GUARDIAN_PERSON_NAME_L_GIVEN] NVARCHAR(MAX),
        [GUARDIAN_PERSON_NAME_L_GIVEN_QUALIFIER] NVARCHAR(MAX),
        [GUARDIAN_PERSON_NAME_L_FAMILY] NVARCHAR(MAX),
        [GUARDIAN_PERSON_NAME_L_FAMILY_QUALIFIER] NVARCHAR(MAX),
        [LANGUAGE_CODE] NVARCHAR(MAX),
        [MODE_CODE] NVARCHAR(MAX),
        [MODE_CODE_SYSTEM] NVARCHAR(MAX),
        [MODE_CODE_SYSTEM_NAME] NVARCHAR(MAX),
        [MODE_DISPLAY_NAME] NVARCHAR(MAX),
        [PROFICIENCY_LEVEL_CODE] NVARCHAR(MAX),
        [PROFICIENCY_LEVEL_CODE_SYSTEM] NVARCHAR(MAX),
        [PROFICIENCY_LEVEL_CODE_SYSTEM_NAME] NVARCHAR(MAX),
        [PROFICIENCY_DISPLAY_NAME] NVARCHAR(MAX),
        [PREFERENCE_IND] NVARCHAR(MAX),
        [FILE_NAME] NVARCHAR(MAX),
        [INSERT_DATETIME] NVARCHAR(MAX)
    )
END
""")

# Ensure all columns from DataFrame are present in the table
add_missing_columns(cursor, schema, table_name, df_record_target.columns)

# Truncate the table if it has rows
cursor.execute(f"""
IF EXISTS (SELECT 1 FROM [{schema}].[{table_name}])
BEGIN
    TRUNCATE TABLE [{schema}].[{table_name}]
END
""")

# Dynamically generate the insert statement based on DataFrame columns
columns_str = ', '.join(f'[{col}]' for col in df_record_target.columns)
values_str = ', '.join('?' for _ in df_record_target.columns)

insert_sql = f"""
INSERT INTO [{schema}].[{table_name}] ({columns_str})
VALUES ({values_str})
"""

# Insert DataFrame to SQL Server
for index, row in df_record_target.iterrows():
    cursor.execute(insert_sql, tuple(row))

# Commit the transaction
conn.commit()

# Close the connection
conn.close()

print('\nData written to SQL Server successfully.')
