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

# Extract the componentOf section
component_of = root.find('.//hl7:componentOf', namespaces)
encompassing_encounter = component_of.find('.//hl7:encompassingEncounter', namespaces)

# Extract encompassingEncounter details
encounter_id = encompassing_encounter.find('.//hl7:id', namespaces)
encounter_id_root = encounter_id.attrib.get('root') if encounter_id is not None else None
encounter_id_extension = encounter_id.attrib.get('extension') if encounter_id is not None else None

encounter_code = encompassing_encounter.find('.//hl7:code', namespaces)
encounter_code_code = encounter_code.attrib.get('code') if encounter_code is not None else None
encounter_code_codeSystem = encounter_code.attrib.get('codeSystem') if encounter_code is not None else None
encounter_code_codeSystemName = encounter_code.attrib.get('codeSystemName') if encounter_code is not None else None
encounter_code_displayName = encounter_code.attrib.get('displayName') if encounter_code is not None else None

effective_time = encompassing_encounter.find('.//hl7:effectiveTime', namespaces)
effective_time_low = effective_time.find('.//hl7:low', namespaces).attrib.get('value') if effective_time.find('.//hl7:low', namespaces) is not None else None
effective_time_high = effective_time.find('.//hl7:high', namespaces).attrib.get('value') if effective_time.find('.//hl7:high', namespaces) is not None else None

# Extract responsibleParty details
responsible_party = encompassing_encounter.find('.//hl7:responsibleParty', namespaces)
assigned_entity = responsible_party.find('.//hl7:assignedEntity', namespaces)

provider_id = assigned_entity.find('.//hl7:id', namespaces)
provider_id_extension = provider_id.attrib.get('extension') if provider_id is not None else None
provider_id_root = provider_id.attrib.get('root') if provider_id is not None else None

provider_addr = assigned_entity.find('.//hl7:addr', namespaces)
provider_street_address_line = provider_addr.find('.//hl7:streetAddressLine', namespaces).text if provider_addr.find('.//hl7:streetAddressLine', namespaces) is not None else None
provider_city = provider_addr.find('.//hl7:city', namespaces).text if provider_addr.find('.//hl7:city', namespaces) is not None else None
provider_state = provider_addr.find('.//hl7:state', namespaces).text if provider_addr.find('.//hl7:state', namespaces) is not None else None
provider_postal_code = provider_addr.find('.//hl7:postalCode', namespaces).text if provider_addr.find('.//hl7:postalCode', namespaces) is not None else None
provider_country = provider_addr.find('.//hl7:country', namespaces).text if provider_addr.find('.//hl7:country', namespaces) is not None else None

# Extract telecom details
provider_telecoms = assigned_entity.findall('.//hl7:telecom', namespaces)
provider_telecom_data = {
    'PROVIDER_TELECOM_WP_PHONE': None,
    'PROVIDER_TELECOM_WP_FAX': None,
    'PROVIDER_TELECOM_WP_EMAIL': None,
    'PROVIDER_TELECOM_HP': None
}
for telecom in provider_telecoms:
    use = telecom.attrib.get('use')
    value = telecom.attrib.get('value')
    if use == 'WP':
        if value.startswith('tel:'):
            provider_telecom_data['PROVIDER_TELECOM_WP_PHONE'] = value
        elif value.startswith('fax:'):
            provider_telecom_data['PROVIDER_TELECOM_WP_FAX'] = value
        elif value.startswith('mailto:'):
            provider_telecom_data['PROVIDER_TELECOM_WP_EMAIL'] = value
    elif use == 'HP':
        provider_telecom_data['PROVIDER_TELECOM_HP'] = value

assigned_person = assigned_entity.find('.//hl7:assignedPerson', namespaces)
provider_name = assigned_person.find('.//hl7:name', namespaces)
provider_given = " ".join([g.text for g in provider_name.findall('.//hl7:given', namespaces) if g.text is not None])
provider_family = provider_name.find('.//hl7:family', namespaces).text if provider_name.find('.//hl7:family', namespaces) is not None else None
provider_suffix = provider_name.find('.//hl7:suffix', namespaces).text if provider_name.find('.//hl7:suffix', namespaces) is not None else None
provider_suffix_qualifier = provider_name.find('.//hl7:suffix', namespaces).attrib.get('qualifier') if provider_name.find('.//hl7:suffix', namespaces) is not None else None

represented_org = assigned_entity.find('.//hl7:representedOrganization', namespaces)
org_name = represented_org.find('.//hl7:name', namespaces).text if represented_org.find('.//hl7:name', namespaces) is not None else None

org_addr = represented_org.find('.//hl7:addr', namespaces)
org_street_address_line = org_addr.find('.//hl7:streetAddressLine', namespaces).text if org_addr.find('.//hl7:streetAddressLine', namespaces) is not None else None
org_city = org_addr.find('.//hl7:city', namespaces).text if org_addr.find('.//hl7:city', namespaces) is not None else None
org_state = org_addr.find('.//hl7:state', namespaces).text if org_addr.find('.//hl7:state', namespaces) is not None else None
org_postal_code = org_addr.find('.//hl7:postalCode', namespaces).text if org_addr.find('.//hl7:postalCode', namespaces) is not None else None
org_country = org_addr.find('.//hl7:country', namespaces).text if org_addr.find('.//hl7:country', namespaces) is not None else None

# Extract location details
location = encompassing_encounter.find('.//hl7:location', namespaces)
health_care_facility = location.find('.//hl7:healthCareFacility', namespaces)

facility_id = health_care_facility.find('.//hl7:id', namespaces)
facility_id_extension = facility_id.attrib.get('extension') if facility_id is not None else None
facility_id_root = facility_id.attrib.get('root') if facility_id is not None else None

facility_code = health_care_facility.find('.//hl7:code', namespaces)
facility_code_code = facility_code.attrib.get('code') if facility_code is not None else None
facility_code_codeSystem = facility_code.attrib.get('codeSystem') if facility_code is not None else None
facility_code_displayName = facility_code.attrib.get('displayName') if facility_code is not None else None

facility_location = health_care_facility.find('.//hl7:location', namespaces)
facility_addr = facility_location.find('.//hl7:addr', namespaces)
facility_street_address_line = facility_addr.find('.//hl7:streetAddressLine', namespaces).text if facility_addr.find('.//hl7:streetAddressLine', namespaces) is not None else None
facility_city = facility_addr.find('.//hl7:city', namespaces).text if facility_addr.find('.//hl7:city', namespaces) is not None else None
facility_state = facility_addr.find('.//hl7:state', namespaces).text if facility_addr.find('.//hl7:state', namespaces) is not None else None
facility_postal_code = facility_addr.find('.//hl7:postalCode', namespaces).text if facility_addr.find('.//hl7:postalCode', namespaces) is not None else None
facility_country = facility_addr.find('.//hl7:country', namespaces).text if facility_addr.find('.//hl7:country', namespaces) is not None else None

service_provider_org = health_care_facility.find('.//hl7:serviceProviderOrganization', namespaces)
service_provider_name = service_provider_org.find('.//hl7:name', namespaces).text if service_provider_org.find('.//hl7:name', namespaces) is not None else None

service_provider_telecoms = service_provider_org.findall('.//hl7:telecom', namespaces)
service_provider_telecom_data = {
    'SERVICE_PROVIDER_TELECOM_WP_PHONE': None,
    'SERVICE_PROVIDER_TELECOM_WP_FAX': None,
    'SERVICE_PROVIDER_TELECOM_WP_EMAIL': None
}
for telecom in service_provider_telecoms:
    use = telecom.attrib.get('use')
    value = telecom.attrib.get('value')
    if use == 'WP':
        if value.startswith('tel:'):
            service_provider_telecom_data['SERVICE_PROVIDER_TELECOM_WP_PHONE'] = value
        elif value.startswith('fax:'):
            service_provider_telecom_data['SERVICE_PROVIDER_TELECOM_WP_FAX'] = value
        elif value.startswith('mailto:'):
            service_provider_telecom_data['SERVICE_PROVIDER_TELECOM_WP_EMAIL'] = value

service_provider_addr = service_provider_org.find('.//hl7:addr', namespaces)
service_provider_street_address_line = service_provider_addr.find('.//hl7:streetAddressLine', namespaces).text if service_provider_addr.find('.//hl7:streetAddressLine', namespaces) is not None else None
service_provider_city = service_provider_addr.find('.//hl7:city', namespaces).text if service_provider_addr.find('.//hl7:city', namespaces) is not None else None
service_provider_state = service_provider_addr.find('.//hl7:state', namespaces).text if service_provider_addr.find('.//hl7:state', namespaces) is not None else None
service_provider_postal_code = service_provider_addr.find('.//hl7:postalCode', namespaces).text if service_provider_addr.find('.//hl7:postalCode', namespaces) is not None else None
service_provider_country = service_provider_addr.find('.//hl7:country', namespaces).text if service_provider_addr.find('.//hl7:country', namespaces) is not None else None

# Create a dictionary for the componentOf data
component_of_data = {
    # EncompassingEncounter block
    "ENCOUNTER_ID_EXTENSION": encounter_id_extension,
    "ENCOUNTER_ID_ROOT": encounter_id_root,
    "ENCOUNTER_CODE_CODE": encounter_code_code,
    "ENCOUNTER_CODE_CODE_SYSTEM": encounter_code_codeSystem,
    "ENCOUNTER_CODE_CODE_SYSTEM_NAME": encounter_code_codeSystemName,
    "ENCOUNTER_CODE_DISPLAY_NAME": encounter_code_displayName,
    "EFFECTIVE_TIME_LOW": effective_time_low,
    "EFFECTIVE_TIME_HIGH": effective_time_high,

    # ResponsibleParty block
    "PROVIDER_ID_EXTENSION": provider_id_extension,
    "PROVIDER_ID_ROOT": provider_id_root,
    "PROVIDER_STREET_ADDRESS_LINE": provider_street_address_line,
    "PROVIDER_CITY": provider_city,
    "PROVIDER_STATE": provider_state,
    "PROVIDER_POSTAL_CODE": provider_postal_code,
    "PROVIDER_COUNTRY": provider_country,
    "PROVIDER_TELECOM_WP_PHONE": provider_telecom_data['PROVIDER_TELECOM_WP_PHONE'],
    "PROVIDER_TELECOM_WP_FAX": provider_telecom_data['PROVIDER_TELECOM_WP_FAX'],
    "PROVIDER_TELECOM_WP_EMAIL": provider_telecom_data['PROVIDER_TELECOM_WP_EMAIL'],
    "PROVIDER_TELECOM_HP": provider_telecom_data['PROVIDER_TELECOM_HP'],
    "PROVIDER_GIVEN": provider_given,
    "PROVIDER_FAMILY": provider_family,
    "PROVIDER_SUFFIX": provider_suffix,
    "PROVIDER_SUFFIX_QUALIFIER": provider_suffix_qualifier,
    "ORG_NAME": org_name,
    "ORG_STREET_ADDRESS_LINE": org_street_address_line,
    "ORG_CITY": org_city,
    "ORG_STATE": org_state,
    "ORG_POSTAL_CODE": org_postal_code,
    "ORG_COUNTRY": org_country,

    # Location block
    "FACILITY_ID_EXTENSION": facility_id_extension,
    "FACILITY_ID_ROOT": facility_id_root,
    "FACILITY_CODE_CODE": facility_code_code,
    "FACILITY_CODE_CODE_SYSTEM": facility_code_codeSystem,
    "FACILITY_CODE_DISPLAY_NAME": facility_code_displayName,
    "FACILITY_STREET_ADDRESS_LINE": facility_street_address_line,
    "FACILITY_CITY": facility_city,
    "FACILITY_STATE": facility_state,
    "FACILITY_POSTAL_CODE": facility_postal_code,
    "FACILITY_COUNTRY": facility_country,
    "SERVICE_PROVIDER_NAME": service_provider_name,
    "SERVICE_PROVIDER_TELECOM_WP_PHONE": service_provider_telecom_data['SERVICE_PROVIDER_TELECOM_WP_PHONE'],
    "SERVICE_PROVIDER_TELECOM_WP_FAX": service_provider_telecom_data['SERVICE_PROVIDER_TELECOM_WP_FAX'],
    "SERVICE_PROVIDER_TELECOM_WP_EMAIL": service_provider_telecom_data['SERVICE_PROVIDER_TELECOM_WP_EMAIL'],
    "SERVICE_PROVIDER_STREET_ADDRESS_LINE": service_provider_street_address_line,
    "SERVICE_PROVIDER_CITY": service_provider_city,
    "SERVICE_PROVIDER_STATE": service_provider_state,
    "SERVICE_PROVIDER_POSTAL_CODE": service_provider_postal_code,
    "SERVICE_PROVIDER_COUNTRY": service_provider_country
}

# Create a DataFrame for the componentOf section
df_component_of = pd.DataFrame([component_of_data])

# Get the current date and time
current_time = datetime.now().strftime("%Y%m%d%H%M%S")

# Define the file path for the componentOf section with the timestamp
csv_file_path_component_of = f'G:/samanth473_drive/Parse/Result/CDA_component_of_{current_time}.csv'

# Save the DataFrame for the componentOf section as a CSV file
df_component_of.to_csv(csv_file_path_component_of, index=False)

# Display the DataFrame for the componentOf section
print(df_component_of)
