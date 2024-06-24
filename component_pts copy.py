import xml.etree.ElementTree as ET
import pandas as pd
from datetime import datetime

# Define the file path
file_path = 'G:/samanth473_drive/CDP/CDA-phcaserpt-1.3.0-CDA-phcaserpt-1.3.1/examples/samples/CDAR2_IG_PHCASERPT_R2_STU3.1_SAMPLE.xml'

# Load and parse the XML file
tree = ET.parse(file_path)
root = tree.getroot()

# Define the namespace
ns = {'n1': 'urn:hl7-org:v3'}

# Find the "Encounters" section
encounters_section = root.find(".//n1:section[n1:title='Encounters']", ns)

# Extract encounter data
encounter_data = []
diagnosis_data = []
additional_diagnosis_data = []

for tr in encounters_section.findall(".//n1:tr", ns):
    if 'ID' in tr.attrib and 'id_2a620155-9d11-439e-92b3-5d9815ff4de8_ref' in tr.attrib['ID']:
        encounter = tr.find('n1:td[1]', ns).text
        date = tr.find('n1:td[2]', ns).text
        location = tr.find('n1:td[3]/n1:list/n1:item/n1:content', ns).text
        encounter_data.append([encounter, date, location])
    
    for diag_tr in tr.findall(".//n1:tr", ns):
        if 'ID' in diag_tr.attrib and 'id_db734647-fc99-424c-a864-7e3cda82e705_ref' in diag_tr.attrib['ID']:
            diagnosis_type = diag_tr.find('n1:td[1]', ns).text
            problem = diag_tr.find('n1:td[2]', ns).text
            trigger_code = diag_tr.find('n1:td[3]', ns).text
            trigger_code_system = diag_tr.find('n1:td[4]', ns).text
            rctc_oid = diag_tr.find('n1:td[5]', ns).text
            rctc_version = diag_tr.find('n1:td[6]', ns).text
            dates = diag_tr.find('n1:td[7]', ns).text
            diagnosis_data.append([diagnosis_type, problem, trigger_code, trigger_code_system, rctc_oid, rctc_version, dates])

        if 'ID' in diag_tr.attrib and 'id_db734647-fc99-424c-a864-7e3cda82e704_ref' in diag_tr.attrib['ID']:
            problem_type = diag_tr.find('n1:td[1]', ns).text
            problem = diag_tr.find('n1:td[2]', ns).text
            dates = diag_tr.find('n1:td[3]', ns).text
            additional_diagnosis_data.append([problem_type, problem, dates])

# Create DataFrames
encounter_df = pd.DataFrame(encounter_data, columns=["Encounter", "Date(s)", "Location"])
diagnosis_df = pd.DataFrame(diagnosis_data, columns=["Encounter Diagnosis Type", "Problem", "Trigger Code", "Trigger Code Code System", "RCTC OID", "RCTC Version", "Date(s)"])
additional_diagnosis_df = pd.DataFrame(additional_diagnosis_data, columns=["Problem Type", "Problem", "Date(s)"])

# Generate current time string
current_time = datetime.now().strftime("%Y%m%d_%H%M%S")

# Define file paths
encounter_file_path = f'G:/samanth473_drive/Parse/Result/CDA_Encounter_{current_time}.csv'
diagnosis_file_path = f'G:/samanth473_drive/Parse/Result/CDA_Diagnosis_{current_time}.csv'
additional_diagnosis_file_path = f'G:/samanth473_drive/Parse/Result/CDA_Additional_Diagnosis_{current_time}.csv'

# Save DataFrames to CSV
encounter_df.to_csv(encounter_file_path, index=False)
diagnosis_df.to_csv(diagnosis_file_path, index=False)
additional_diagnosis_df.to_csv(additional_diagnosis_file_path, index=False)

print(f'Files saved successfully:\n{encounter_file_path}\n{diagnosis_file_path}\n{additional_diagnosis_file_path}')
