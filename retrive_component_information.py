import xml.etree.ElementTree as ET
import pandas as pd

# Load the XML from the specified file path
tree = ET.parse('G:/samanth473_drive/CDP/CDA-phcaserpt-1.3.0-CDA-phcaserpt-1.3.1/examples/samples/CDAR2_IG_PHCASERPT_R2_STU3.1_SAMPLE.xml')
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

# Create a DataFrame for the component sections
df_component = pd.DataFrame(component_data_list)

# Save the DataFrame as a CSV file
csv_file_path_component = 'G:/samanth473_drive/Parse/Result/CDA_components.csv'
df_component.to_csv(csv_file_path_component, index=False)

# Display the DataFrame
print(df_component)
