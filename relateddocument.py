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
    "RELATED_DOC_VERSION_NUMBER": version_value
}

# Create a DataFrame for the relatedDocument section
df_related_document = pd.DataFrame([related_document_data])

# Get the current date and time
current_time = datetime.now().strftime("%Y%m%d%H%M%S")

# Define the file path for the relatedDocument section with the timestamp
csv_file_path_related_document = f'G:/samanth473_drive/Parse/Result/CDA_related_document_{current_time}.csv'

# Save the DataFrame for the relatedDocument section as a CSV file
df_related_document.to_csv(csv_file_path_related_document, index=False)

# Display the DataFrame for the relatedDocument section
print(df_related_document)
