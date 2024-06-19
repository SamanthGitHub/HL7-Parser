import xml.etree.ElementTree as ET
import pandas as pd
from datetime import datetime

# Load the XML from the specified file path
tree = ET.parse('G:/samanth473_drive/CDP/CDA-phcaserpt-1.3.0-CDA-phcaserpt-1.3.1/examples/samples/CDAR2_IG_PHCASERPT_R2_STU3.1_SAMPLE.xml')
root = tree.getroot()

# Define the namespace map
namespaces = {
    'hl7': 'urn:hl7-org:v3',
    'sdtc': 'urn:hl7-org:sdtc',
    'xsi': 'http://www.w3.org/2001/XMLSchema-instance'
}

# Function to extract all attributes of an element
def extract_attributes(element):
    attributes = {}
    if element is not None:
        for attr, value in element.attrib.items():
            attributes[f"{element.tag.split('}')[-1].upper()}_{attr.upper()}"] = value
    return attributes

# Function to extract all relevant data before the title
def extract_component_data(section):
    component_data = {}
    for child in section:
        if child.tag.endswith('title'):
            break
        tag_base = child.tag.split('}')[-1].upper()
        if tag_base == 'TEMPLATEID':
            index = 1
            while f'{tag_base}_ROOT_{index}' in component_data:
                index += 1
            for attr, value in child.attrib.items():
                component_data[f"{tag_base}_{attr.upper()}_{index}"] = value
        else:
            component_data.update(extract_attributes(child))
    return component_data

# Extract all component sections
components = root.findall('.//hl7:structuredBody/hl7:component', namespaces)

# List to store component data
component_data_list = []

# Process each component section
for component in components:
    section = component.find('.//hl7:section', namespaces)
    if section is not None:
        # Extract component data
        component_data = extract_component_data(section)
        
        # Extract title
        title = section.find('.//hl7:title', namespaces)
        if title is not None:
            component_data["TITLE"] = title.text
        
        component_data_list.append(component_data)

# Create a DataFrame for component sections
df_components = pd.DataFrame(component_data_list)

# Get the current date and time
current_time = datetime.now().strftime("%Y%m%d%H%M%S")

# Define the file path for the component section with the timestamp
csv_file_path_component = f'G:/samanth473_drive/Parse/Result/CDA_components_{current_time}.csv'

# Save the DataFrame as a CSV file
df_components.to_csv(csv_file_path_component, index=False)

# Display the DataFrame
print(df_components)
