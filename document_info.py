import xml.etree.ElementTree as ET
import pandas as pd
from datetime import datetime

# Load the XML from a file
tree = ET.parse('G:/samanth473_drive/CDP/CDA-phcaserpt-1.3.0-CDA-phcaserpt-1.3.1/examples/samples/CDAR2_IG_PHCASERPT_R2_STU3.1_SAMPLE.xml')
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
    "VERSION_NUMBER": version_number
}

# Create a DataFrame
df = pd.DataFrame([data])

# Get the current date and time
current_time = datetime.now().strftime("%m%d%Y%H%M%S")

# Define the file path with the timestamp
csv_file_path = f'G:/samanth473_drive/Parse/Result/CDA_phcaserpt_{current_time}.csv'

# Save the DataFrame as a CSV file
df.to_csv(csv_file_path, index=False)

# Display the DataFrame
print(df)


