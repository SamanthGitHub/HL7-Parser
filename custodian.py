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
    "CUSTODIAN_TELECOM_VALUE": telecom_value
}

# Create a DataFrame for the custodian section
df_custodian = pd.DataFrame([custodian_data])

# Get the current date and time
current_time = datetime.now().strftime("%Y%m%d%H%M%S")

# Define the file path for the custodian section with the timestamp
csv_file_path_custodian = f'G:/samanth473_drive/Parse/Result/CDA_custodian_{current_time}.csv'

# Save the DataFrame for the custodian section as a CSV file
df_custodian.to_csv(csv_file_path_custodian, index=False)

# Display the DataFrame for the custodian section
print(df_custodian)
