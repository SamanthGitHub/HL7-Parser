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

# Function to extract the full text content, handling <br> tags
def get_full_text(element):
    text = ''
    if element.text:
        text += element.text
    for sub_element in element:
        if sub_element.tag == '{urn:hl7-org:v3}br':
            text += '\n'
        if sub_element.tail:
            text += sub_element.tail
    return text

# Extract the component section
component_section = root.find('.//hl7:structuredBody/hl7:component/hl7:section', namespaces)

# Extract the title for the identifier
title_element = component_section.find('.//hl7:title', namespaces)
table_identifier = title_element.text.replace(' ', '_').upper() if title_element is not None else "UNKNOWN_TABLE"

# Extract the HTML table data
html_tables = component_section.findall('.//hl7:text/hl7:table', namespaces)
table_data_list = []

for html_table in html_tables:
    table_data = []
    headers = [get_full_text(th) for th in html_table.findall('.//hl7:thead/hl7:tr/hl7:th', namespaces)]
    for row in html_table.findall('.//hl7:tbody/hl7:tr', namespaces):
        row_data = [get_full_text(td) for td in row.findall('hl7:td', namespaces)]
        row_data.append(table_identifier)  # Add identifier to link with main table
        table_data.append(row_data)
    df_html_table = pd.DataFrame(table_data, columns=headers + ['TABLE_IDENTIFIER'])
    table_data_list.append(df_html_table)

# Combine all HTML table DataFrames into one
df_html_combined = pd.concat(table_data_list, ignore_index=True)

# Extract other relevant data from the component section
component_data = {
    "TEMPLATE_ID_1_ROOT": component_section.find('.//hl7:templateId', namespaces).attrib.get('root'),
    "TEMPLATE_ID_2_ROOT": component_section.find('.//hl7:templateId[@extension]', namespaces).attrib.get('root'),
    "TEMPLATE_ID_2_EXTENSION": component_section.find('.//hl7:templateId[@extension]', namespaces).attrib.get('extension'),
    "CODE_CODE": component_section.find('.//hl7:code', namespaces).attrib.get('code'),
    "CODE_CODE_SYSTEM": component_section.find('.//hl7:code', namespaces).attrib.get('codeSystem'),
    "CODE_CODE_SYSTEM_NAME": component_section.find('.//hl7:code', namespaces).attrib.get('codeSystemName'),
    "CODE_DISPLAY_NAME": component_section.find('.//hl7:code', namespaces).attrib.get('displayName'),
    "TITLE": component_section.find('.//hl7:title', namespaces).text,
    "TABLE_IDENTIFIER": table_identifier  # Add identifier to link with HTML tables
}

# Create a DataFrame for the component section
df_component = pd.DataFrame([component_data])

# Extract entries within the component section
entries = []
for entry in component_section.findall('.//hl7:entry', namespaces):
    observation = entry.find('.//hl7:observation', namespaces)
    entry_data = {"ENTRY_TYPE_CODE": entry.attrib.get('typeCode')}

    if observation is not None:
        entry_data.update({
            "OBSERVATION_CLASS_CODE": observation.attrib.get('classCode'),
            "OBSERVATION_MOOD_CODE": observation.attrib.get('moodCode'),
            "OBSERVATION_TEMPLATE_ID_ROOT": observation.find('.//hl7:templateId', namespaces).attrib.get('root'),
            "OBSERVATION_TEMPLATE_ID_EXTENSION": observation.find('.//hl7:templateId[@extension]', namespaces).attrib.get('extension'),
            "OBSERVATION_ID_ROOT": observation.find('.//hl7:id', namespaces).attrib.get('root'),
            "OBSERVATION_ID_EXTENSION": observation.find('.//hl7:id', namespaces).attrib.get('extension'),
            "OBSERVATION_CODE_CODE": observation.find('.//hl7:code', namespaces).attrib.get('code'),
            "OBSERVATION_CODE_CODE_SYSTEM": observation.find('.//hl7:code', namespaces).attrib.get('codeSystem'),
            "OBSERVATION_CODE_CODE_SYSTEM_NAME": observation.find('.//hl7:code', namespaces).attrib.get('codeSystemName'),
            "OBSERVATION_CODE_DISPLAY_NAME": observation.find('.//hl7:code', namespaces).attrib.get('displayName'),
            "OBSERVATION_STATUS_CODE": observation.find('.//hl7:statusCode', namespaces).attrib.get('code'),
            "OBSERVATION_EFFECTIVE_TIME": observation.find('.//hl7:effectiveTime', namespaces).attrib.get('value')
        })
    else:
        procedure = entry.find('.//hl7:procedure', namespaces)
        if procedure is not None:
            entry_data.update({
                "PROCEDURE_CLASS_CODE": procedure.attrib.get('classCode'),
                "PROCEDURE_MOOD_CODE": procedure.attrib.get('moodCode'),
                "PROCEDURE_TEMPLATE_ID_ROOT": procedure.find('.//hl7:templateId', namespaces).attrib.get('root'),
                "PROCEDURE_TEMPLATE_ID_EXTENSION": procedure.find('.//hl7:templateId[@extension]', namespaces).attrib.get('extension'),
                "PROCEDURE_ID_ROOT": procedure.find('.//hl7:id', namespaces).attrib.get('root'),
                "PROCEDURE_ID_EXTENSION": procedure.find('.//hl7:id', namespaces).attrib.get('extension'),
                "PROCEDURE_CODE_CODE": procedure.find('.//hl7:code', namespaces).attrib.get('code'),
                "PROCEDURE_CODE_CODE_SYSTEM": procedure.find('.//hl7:code', namespaces).attrib.get('codeSystem'),
                "PROCEDURE_CODE_CODE_SYSTEM_NAME": procedure.find('.//hl7:code', namespaces).attrib.get('codeSystemName'),
                "PROCEDURE_CODE_DISPLAY_NAME": procedure.find('.//hl7:code', namespaces).attrib.get('displayName'),
                "PROCEDURE_STATUS_CODE": procedure.find('.//hl7:statusCode', namespaces).attrib.get('code'),
                "PROCEDURE_EFFECTIVE_TIME": procedure.find('.//hl7:effectiveTime', namespaces).attrib.get('value')
            })
        else:
            act = entry.find('.//hl7:act', namespaces)
            if act is not None:
                entry_data.update({
                    "ACT_CLASS_CODE": act.attrib.get('classCode'),
                    "ACT_MOOD_CODE": act.attrib.get('moodCode'),
                    "ACT_TEMPLATE_ID_ROOT": act.find('.//hl7:templateId', namespaces).attrib.get('root'),
                    "ACT_TEMPLATE_ID_EXTENSION": act.find('.//hl7:templateId[@extension]', namespaces).attrib.get('extension'),
                    "ACT_ID_ROOT": act.find('.//hl7:id', namespaces).attrib.get('root'),
                    "ACT_ID_EXTENSION": act.find('.//hl7:id', namespaces).attrib.get('extension'),
                    "ACT_CODE_CODE": act.find('.//hl7:code', namespaces).attrib.get('code'),
                    "ACT_CODE_CODE_SYSTEM": act.find('.//hl7:code', namespaces).attrib.get('codeSystem'),
                    "ACT_CODE_CODE_SYSTEM_NAME": act.find('.//hl7:code', namespaces).attrib.get('codeSystemName'),
                    "ACT_CODE_DISPLAY_NAME": act.find('.//hl7:code', namespaces).attrib.get('displayName'),
                    "ACT_STATUS_CODE": act.find('.//hl7:statusCode', namespaces).attrib.get('code'),
                    "ACT_EFFECTIVE_TIME": act.find('.//hl7:effectiveTime', namespaces).attrib.get('value')
                })

    entry_data["TABLE_IDENTIFIER"] = table_identifier  # Add identifier to link with HTML tables
    entries.append(entry_data)

# Ensure all columns are present by combining all entry DataFrames with the main component DataFrame
df_entries = pd.DataFrame(entries)
combined_columns = df_component.columns.union(df_entries.columns)
df_combined = pd.concat([df_component, df_entries], axis=0, ignore_index=True).reindex(columns=combined_columns)

# Get the current date and time
current_time = datetime.now().strftime("%Y%m%d%H%M%S")

# Define the file paths for the component section and HTML table with the timestamp
csv_file_path_component = f'G:/samanth473_drive/Parse/Result/CDA_component_{current_time}.csv'
csv_file_path_html = f'G:/samanth473_drive/Parse/Result/CDA_html_table_{current_time}.csv'

# Save the DataFrames as CSV files
df_combined.to_csv(csv_file_path_component, index=False)
df_html_combined.to_csv(csv_file_path_html, index=False)

# Display the DataFrames
print(df_combined)
print(df_html_combined)
