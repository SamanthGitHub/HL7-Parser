import xml.etree.ElementTree as ET
import pandas as pd
from datetime import datetime

# Load the XML from the specified file path
tree = ET.parse('G:/samanth473_drive/CDP/CDA-phcaserpt-1.3.0-CDA-phcaserpt-1.3.1/examples/samples/CDAR2_IG_PHCASERPT_R2_STU3.1_SAMPLE.xml')
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

# Create a dictionary for the recordTarget section data
record_target_data = {
    ## Patient IDs
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
    "PREFERENCE_IND": language_data.get('PREFERENCE_IND')
 }

# Create a DataFrame for the recordTarget section
df_record_target = pd.DataFrame([record_target_data])

# # Get the current date and time
current_time = datetime.now().strftime("%Y%m%d%H%M%S")

# # Define the file path for the recordTarget section with the timestamp
csv_file_path_record_target = f'G:/samanth473_drive/Parse/Result/CDA_phcaserpt_{current_time}.csv'

# # Save the DataFrame for the recordTarget section as a CSV file
df_record_target.to_csv(csv_file_path_record_target, index=False)

#Display the DataFrame for the recordTarget section
print(df_record_target)
