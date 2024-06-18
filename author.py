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

# Extract the author section
author = root.find('.//hl7:author', namespaces)
author_time = author.find('.//hl7:time', namespaces).attrib.get('value') if author.find('.//hl7:time', namespaces) is not None else None

assigned_author = author.find('.//hl7:assignedAuthor', namespaces)
author_id = assigned_author.find('.//hl7:id', namespaces).attrib.get('root') if assigned_author.find('.//hl7:id', namespaces) is not None else None

addr = assigned_author.find('.//hl7:addr', namespaces)
street_address_line = addr.find('.//hl7:streetAddressLine', namespaces).text if addr.find('.//hl7:streetAddressLine', namespaces) is not None else None
city = addr.find('.//hl7:city', namespaces).text if addr.find('.//hl7:city', namespaces) is not None else None
state = addr.find('.//hl7:state', namespaces).text if addr.find('.//hl7:state', namespaces) is not None else None
postal_code = addr.find('.//hl7:postalCode', namespaces).text if addr.find('.//hl7:postalCode', namespaces) is not None else None
country = addr.find('.//hl7:country', namespaces).text if addr.find('.//hl7:country', namespaces) is not None else None

telecom = assigned_author.find('.//hl7:telecom', namespaces)
telecom_use = telecom.attrib.get('use') if telecom is not None else None
telecom_value = telecom.attrib.get('value') if telecom is not None else None

assigned_authoring_device = assigned_author.find('.//hl7:assignedAuthoringDevice', namespaces)
manufacturer_model_name = assigned_authoring_device.find('.//hl7:manufacturerModelName', namespaces).attrib.get('displayName') if assigned_authoring_device.find('.//hl7:manufacturerModelName', namespaces) is not None else None
software_name = assigned_authoring_device.find('.//hl7:softwareName', namespaces).attrib.get('displayName') if assigned_authoring_device.find('.//hl7:softwareName', namespaces) is not None else None

# Create a dictionary for the author data
author_data = {
    "AUTHOR_TIME": author_time,
    "AUTHOR_ID": author_id,
    "AUTHOR_STREET_ADDRESS_LINE": street_address_line,
    "AUTHOR_CITY": city,
    "AUTHOR_STATE": state,
    "AUTHOR_POSTAL_CODE": postal_code,
    "AUTHOR_COUNTRY": country,
    "AUTHOR_TELECOM_USE": telecom_use,
    "AUTHOR_TELECOM_VALUE": telecom_value,
    "AUTHOR_MANUFACTURER_MODEL_NAME": manufacturer_model_name,
    "AUTHOR_SOFTWARE_NAME": software_name
}

# Create a DataFrame for the author section
df_author = pd.DataFrame([author_data])

# Get the current date and time
current_time = datetime.now().strftime("%Y%m%d%H%M%S")

# Define the file path for the author section with the timestamp
csv_file_path_author = f'G:/samanth473_drive/Parse/Result/CDA_author_{current_time}.csv'

# Save the DataFrame for the author section as a CSV file
df_author.to_csv(csv_file_path_author, index=False)

# Display the DataFrame for the author section
print(df_author)
