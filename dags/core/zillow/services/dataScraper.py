import logging
import json

# Configure logging
logging.basicConfig(
    level=logging.INFO,  # You can change to logging.DEBUG for more verbose output
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s [%(funcName)s]',
    datefmt='%Y-%m-%d %H:%M:%S'
)

def clean_data_phone(json_data):
    for item in json_data:
        # Check if 'phone' exists in the item
        if "phone" in item:
            # If 'phone' is an empty string, set it to None
            if item["phone"] == "":
                item["phone"] = None
    return json_data


def extract_base_data(json_data):
    if not isinstance(json_data, dict):
        logging.error(f"Expected a dictionary but got: {type(json_data)}")
        return []

    try:
        # Dynamically build a flat record of all top-level key-value pairs
        flat_record = {key: json_data.get(key) for key in json_data}

        # Handle nested 'address' separately if needed
        address = json_data.get('address', {})
        if isinstance(address, dict):
            flat_record['streetAddress'] = address.get('streetAddress', None)
            flat_record['city'] = address.get('city', None)
            flat_record['state'] = address.get('state', None)
            flat_record['zipcode'] = address.get('zipcode', None)
            flat_record['neighborhood'] = address.get('neighborhood', None)
            flat_record['subdivision'] = address.get('subdivision', None)
        else:
            logging.warning(f"Expected 'address' to be a dictionary but got: {type(address)}")

        return [flat_record]
    except Exception as e:
        logging.error(f"Unexpected error while extracting property details: {e}")
        return []
    

def extract_property_details(data):
    """
    Flattens the top-level key-value pairs from the provided data and
    handles specific nested structures like 'listingOffices' and 'listingAgents'.
    Excludes the 'priceHistory' and 'resoFacts.daysOnZillow' keys from the extraction process.
    """
    flattened_records = []

    if not isinstance(data, dict):
        logging.error(f"Expected a dictionary but got: {type(data)}")
        return flattened_records

    # Create a base flat record from top-level key-value pairs, excluding specific keys
    flat_record = {
        key: value for key, value in data.items() 
        if not isinstance(value, (list, dict)) and key not in {'listingSubType', 'attributionInfo', 'address', 'openHouseSchedule', 'priceHistory', 'resoFacts.daysOnZillow', 'taxHistory', 'schools'}
    }

    # Add the flat record to the list of flattened records
    flattened_records.append(flat_record)

    return flattened_records
    

def extract_climate_data(json_data):
    """
    Flattens the 'climate' data from a given dictionary containing property data.
    If the 'climate' key is missing, it will return an empty list.
    """
    flattened_records = []

    if not isinstance(json_data, dict):
        logging.error(f"Expected a dictionary but got: {type(json_data)}")
        return flattened_records

    # Check if 'climate' key exists
    climate_data = json_data.get('climate')
    if not climate_data:
        logging.warning("No 'climate' key found in the data.")
        return flattened_records

    if not isinstance(climate_data, dict):
        logging.error(f"Expected 'climate' to be a dictionary but got: {type(climate_data)}")
        return flattened_records

    for source_type, source_content in climate_data.items():
        if isinstance(source_content, dict):
            primary_data = source_content.get('primary')
            if primary_data and isinstance(primary_data, dict):
                flat_record = {
                    "zpid": json_data.get('zpid'),
                    'sourceType': source_type,
                    'insuranceRecommendation': primary_data.get('insuranceRecommendation'),
                    'riskScoreLabel': primary_data.get('riskScore', {}).get('label') if isinstance(primary_data.get('riskScore'), dict) else None,
                    'riskScoreMax': primary_data.get('riskScore', {}).get('max') if isinstance(primary_data.get('riskScore'), dict) else None,
                    'riskScoreValue': primary_data.get('riskScore', {}).get('value') if isinstance(primary_data.get('riskScore'), dict) else None,
                    'insuranceSeparatePolicy': primary_data.get('insuranceSeparatePolicy'),
                    'historicCountAll': primary_data.get('historicCountAll'),
                    'historicCountPropertyAll': primary_data.get('historicCountPropertyAll'),
                    'sourceURL': primary_data.get('source', {}).get('url') if isinstance(primary_data.get('source'), dict) else None,
                }

                # Handle probability data if present
                if 'probability' in primary_data and isinstance(primary_data['probability'], list):
                    flat_record['probability'] = [
                        {
                            'relativeYear': prob.get('relativeYear'),
                            'probability': prob.get('probability')
                        }
                        for prob in primary_data['probability']
                    ]

                # Handle other nested lists if present
                if 'hotDays' in primary_data and isinstance(primary_data['hotDays'], list):
                    flat_record['hotDays'] = [
                        {
                            'relativeYear': day.get('relativeYear'),
                            'dayCount': day.get('dayCount')
                        }
                        for day in primary_data['hotDays']
                    ]
                if 'badAirDays' in primary_data and isinstance(primary_data['badAirDays'], list):
                    flat_record['badAirDays'] = [
                        {
                            'relativeYear': day.get('relativeYear'),
                            'dayCount': day.get('dayCount')
                        }
                        for day in primary_data['badAirDays']
                    ]

                # Handle percentile temperature if present
                flat_record['percentile98Temp'] = primary_data.get('percentile98Temp')

                flattened_records.append(flat_record)
            else:
                logging.warning(f"'primary' is either None or not a dictionary for source type {source_type}.")
        else:
            logging.warning(f"Expected 'primary' to be a dictionary but got: {type(source_content)}")

    return flattened_records

def extract_tax_history(data):
    """
    Flattens the 'taxHistory' data from a given dictionary.
    If the 'taxHistory' key does not exist or is not a list, an empty list is returned.
    """
    flattened_records = []

    if not isinstance(data, dict):
        logging.error(f"Expected a dictionary but got: {type(data)}")
        return flattened_records

    tax_history = data.get('taxHistory')
    if not tax_history:
        logging.warning("No 'taxHistory' key found or it is empty.")
        return flattened_records

    if not isinstance(tax_history, list):
        logging.error(f"Expected 'taxHistory' to be a list but got: {type(tax_history)}")
        return flattened_records

    for record in tax_history:
        if isinstance(record, dict):
            flat_record = {
                "zpid": data.get('zpid'),
                'time': record.get('time'),
                'valueIncreaseRate': record.get('valueIncreaseRate'),
                'taxIncreaseRate': record.get('taxIncreaseRate'),
                'taxPaid': record.get('taxPaid'),
                'value': record.get('value')
            }
            flattened_records.append(flat_record)
        else:
            logging.warning(f"Expected each entry in 'taxHistory' to be a dictionary but got: {type(record)}")

    return flattened_records

def extract_nearby_homes(data):
    """
    Flattens the 'nearbyHomes' data from a given dictionary.
    If the 'nearbyHomes' key does not exist or is not a list, an empty list is returned.
    """
    flattened_records = []

    if not isinstance(data, dict):
        logging.error(f"Expected a dictionary but got: {type(data)}")
        return flattened_records

    nearby_homes = data.get('nearbyHomes')
    if not nearby_homes:
        logging.warning("No 'nearbyHomes' key found or it is empty.")
        return flattened_records

    # if not isinstance(nearby_homes, list):
    #     logging.error(f"Expected 'nearbyHomes' to be a list but got: {type(nearby_homes)}")
    #     return flattened_records

    for home in nearby_homes:
        if isinstance(home, dict):
            flat_record = {
                'zpid': data.get('zpid'),
                'zpidNearByProperty': home.get('zpid'),
                'livingArea': home.get('livingArea'),
                'livingAreaValue': home.get('livingAreaValue'),
                'lotAreaUnits': home.get('lotAreaUnits'),
                'lotAreaValue': home.get('lotAreaValue'),
                'lotSize': home.get('lotSize'),
                'miniCardPhotoUrls': [photo.get('url') for photo in home.get('miniCardPhotos', []) if isinstance(photo, dict)],
                'longitude': home.get('longitude'),
                'latitude': home.get('latitude'),
                'livingAreaUnits': home.get('livingAreaUnits'),
                'price': home.get('price'),
                'homeType': home.get('homeType'),
                'homeStatus': home.get('homeStatus'),
                'currency': home.get('currency'),
                'address_city': home.get('address', {}).get('city'),
                'address_state': home.get('address', {}).get('state'),
                'address_streetAddress': home.get('address', {}).get('streetAddress'),
                'address_zipcode': home.get('address', {}).get('zipcode')
            }
            flattened_records.append(flat_record)
        else:
            logging.warning(f"Expected each entry in 'nearbyHomes' to be a dictionary but got: {type(home)}")

    return flattened_records

def extract_price_history(data):
    """
    Flattens the 'priceHistory' data from a given dictionary.
    If the 'priceHistory' key does not exist or is not a list, an empty list is returned.
    """
    flattened_records = []

    if not isinstance(data, dict):
        logging.error(f"Expected a dictionary but got: {type(data)}")
        return flattened_records

    price_history = data.get('priceHistory')
    if not price_history:
        logging.warning("No 'priceHistory' key found or it is empty.")
        return flattened_records

    if not isinstance(price_history, list):
        logging.error(f"Expected 'priceHistory' to be a list but got: {type(price_history)}")
        return flattened_records

    for record in price_history:
        if isinstance(record, dict):
            flat_record = {
                "zpid": data.get('zpid'),
                'priceChangeRate': record.get('priceChangeRate'),
                'date': record.get('date'),
                'source': record.get('source'),
                'postingIsRental': record.get('postingIsRental'),
                'time': record.get('time'),
                'attributeSource_infoString2': record.get('attributeSource', {}).get('infoString2'),
                'pricePerSquareFoot': record.get('pricePerSquareFoot'),
                'event': record.get('event'),
                'price': record.get('price')

                # 'attributeSource_infoString3': record.get('attributeSource', {}).get('infoString3'),
                # 'attributeSource_infoString1': record.get('attributeSource', {}).get('infoString1'),
                # 'buyerAgent_name': record.get('buyerAgent', {}).get('name') if record.get('buyerAgent') else None,
                # 'buyerAgent_photo': record.get('buyerAgent', {}).get('photo') if record.get('buyerAgent') else None,
                # 'buyerAgent_profileUrl': record.get('buyerAgent', {}).get('profileUrl') if record.get('buyerAgent') else None,
                # 'sellerAgent_name': record.get('sellerAgent', {}).get('name') if record.get('sellerAgent') else None,
                # 'sellerAgent_photo': record.get('sellerAgent', {}).get('photo') if record.get('sellerAgent') else None,
                # 'sellerAgent_profileUrl': record.get('sellerAgent', {}).get('profileUrl') if record.get('sellerAgent') else None,
                # 'showCountyLink': record.get('showCountyLink'),
            }
            flattened_records.append(flat_record)

        else:
            logging.warning(f"Expected each entry in 'priceHistory' to be a dictionary but got: {type(record)}")

    return flattened_records

def extract_schools_nearby(data):
    """
    Flattens the 'schools' data from a given dictionary.
    If the 'schools' key does not exist or is not a list, an empty list is returned.
    """
    flattened_records = []

    if not isinstance(data, dict):
        logging.error(f"Expected a dictionary but got: {type(data)}")
        return flattened_records

    schools = data.get('schools')
    if not schools:
        logging.warning("No 'schools' key found or it is empty.")
        return flattened_records

    if not isinstance(schools, list):
        logging.error(f"Expected 'schools' to be a list but got: {type(schools)}")
        return flattened_records

    for school in schools:
        if isinstance(school, dict):
            flat_record = {
                'zpid': data.get('zpid'),
                'link': school.get('link'),
                'rating': school.get('rating'),
                'totalCount': school.get('totalCount'),
                'distance': school.get('distance'),
                'assigned': school.get('assigned'),
                'name': school.get('name'),
                'studentsPerTeacher': school.get('studentsPerTeacher'),
                'isAssigned': school.get('isAssigned'),
                'size': school.get('size'),
                'level': school.get('level'),
                'grades': school.get('grades'),
                'type': school.get('type')
            }
            flattened_records.append(flat_record)
        else:
            logging.warning(f"Expected each entry in 'schools' to be a dictionary but got: {type(school)}")

    return flattened_records

def extract_attribution_info(data):
    """
    Flattens the 'attributionInfo' data from a given dictionary.
    If the 'attributionInfo' key does not exist or is not a dictionary, an empty list is returned.
    Nested lists like 'listingOffices' and 'listingAgents' are handled in a structured way.
    """
    flattened_records = []

    if not isinstance(data, dict):
        logging.error(f"Expected a dictionary but got: {type(data)}")
        return flattened_records

    attribution_info = data.get('attributionInfo')
    if not attribution_info or not isinstance(attribution_info, dict):
        logging.warning("No 'attributionInfo' key found or it is not a dictionary.")
        return flattened_records

    # Create a base flat record for non-nested fields
    flat_record = {
        'zpid': data.get('zpid'),
        'buyerAgentName': attribution_info.get('buyerAgentName'),
        'mlsName': attribution_info.get('mlsName'),
        'coAgentLicenseNumber': attribution_info.get('coAgentLicenseNumber'),
        'lastUpdated': attribution_info.get('lastUpdated'),
        'buyerAgentMemberStateLicense': attribution_info.get('buyerAgentMemberStateLicense'),
        'brokerName': attribution_info.get('brokerName'),
        'listingAgreement': attribution_info.get('listingAgreement'),
        'infoString10': attribution_info.get('infoString10'),
        'trueStatus': attribution_info.get('trueStatus'),
        'infoString3': attribution_info.get('infoString3'),
        'agentEmail': attribution_info.get('agentEmail'),
        'agentName': attribution_info.get('agentName'),
        'attributionTitle': attribution_info.get('attributionTitle'),
        'mlsId': attribution_info.get('mlsId'),
        'coAgentName': attribution_info.get('coAgentName'),
        'coAgentNumber': attribution_info.get('coAgentNumber'),
        'infoString5': attribution_info.get('infoString5'),
        'agentPhoneNumber': attribution_info.get('agentPhoneNumber'),
        'agentLicenseNumber': attribution_info.get('agentLicenseNumber'),
        'providerLogo': attribution_info.get('providerLogo'),
        'infoString16': attribution_info.get('infoString16'),
        'buyerBrokerageName': attribution_info.get('buyerBrokerageName'),
        'mlsDisclaimer': attribution_info.get('mlsDisclaimer'),
        'brokerPhoneNumber': attribution_info.get('brokerPhoneNumber'),
        'lastChecked': attribution_info.get('lastChecked')
    }

    # Handle 'listingOffices' nested list if present
    if 'listingOffices' in attribution_info:
        flat_record['listingOffices'] = [
            {
                'associatedOfficeType': office.get('associatedOfficeType'),
                'officeName': office.get('officeName')
            }
            for office in attribution_info['listingOffices']
            if isinstance(office, dict)
        ]

    # Handle 'listingAgents' nested list if present
    if 'listingAgents' in attribution_info:
        flat_record['listingAgents'] = [
            {
                'memberStateLicense': agent.get('memberStateLicense'),
                'memberFullName': agent.get('memberFullName'),
                'associatedAgentType': agent.get('associatedAgentType')
            }
            for agent in attribution_info['listingAgents']
            if isinstance(agent, dict)
        ]

    # Add the flat record to the list of flattened records
    flattened_records.append(flat_record)

    return flattened_records

def extract_reso_facts(data):
    """
    Flattens the 'resoFacts' data from a given dictionary while excluding fields
    that contain lists of strings or lists of dictionaries.
    """
    flattened_records = []

    if not isinstance(data, dict):
        logging.error(f"Expected a dictionary but got: {type(data)}")
        return flattened_records

    # Access 'resoFacts' key and validate it
    reso_facts = data.get('resoFacts')
    if not reso_facts or not isinstance(reso_facts, dict):
        logging.warning("No 'resoFacts' key found or it is not a dictionary.")
        return flattened_records

    # Create a base flat record while skipping fields that are lists of strings or lists of dictionaries
    flat_record = {
        key: value
        for key, value in reso_facts.items()
        if key != 'rooms' and not (
            isinstance(value, list) and (all(isinstance(item, str) for item in value) or all(isinstance(item, dict) for item in value))
        ) and not isinstance(value, dict)
    }

    # Add 'zpid' separately if needed
    flat_record['zpid'] = data.get('zpid')

    # Add the flat record to the list of flattened records
    flattened_records.append(flat_record)

    return flattened_records

def extract_reso_nested(data):
    """
    Builds a list of dictionaries that include fields from 'resoFacts' that are either 
    lists of strings or lists of dictionaries, excluding certain keys.
    """
    structured_records = []

    if not isinstance(data, dict):
        logging.error(f"Expected a dictionary but got: {type(data)}")
        return structured_records

    # Access 'resoFacts' key and validate it
    reso_facts = data.get('resoFacts')
    if not reso_facts or not isinstance(reso_facts, dict):
        logging.warning("No 'resoFacts' key found or it is not a dictionary.")
        return structured_records

    # Extract fields that are lists of strings or lists of dictionaries, excluding 'rooms' and 'associationAmenities'
    structured_record = {
        key: value
        for key, value in reso_facts.items()
        if key not in {'rooms', 'associationAmenities'} and (
            isinstance(value, list) and (all(isinstance(item, str) for item in value) or all(isinstance(item, dict) for item in value))
        )
    }

    # Add 'zpid' separately if needed
    structured_record['zpid'] = data.get('zpid')

    # Add the structured record to the list
    structured_records.append(structured_record)

    return structured_records

def extract_room_details(data):
    """
    Extracts and processes the 'rooms' data from 'resoFacts'.
    """
    rooms_records = []

    if not isinstance(data, dict):
        logging.error(f"Expected a dictionary but got: {type(data)}")
        return rooms_records

    reso_facts = data.get('resoFacts')
    if not reso_facts or not isinstance(reso_facts, dict):
        logging.warning("No 'resoFacts' key found or it is not a dictionary.")
        return rooms_records

    # Extract 'rooms' if present
    rooms = reso_facts.get('rooms')
    if rooms and isinstance(rooms, list):
        for room in rooms:
            if isinstance(room, dict):
                room_record = {
                    'level': room.get('level'),
                    'dimensions': room.get('dimensions'),
                    'features': room.get('features'),
                    'roomAreaUnits': room.get('roomAreaUnits'),
                    'roomArea': room.get('roomArea'),
                    'roomAreaSource': room.get('roomAreaSource'),
                    'roomType': room.get('roomType'),
                    'roomDescription': room.get('roomDescription'),
                    'roomWidth': room.get('roomWidth'),
                    'roomLevel': room.get('roomLevel'),
                    'description': room.get('description'),
                    'roomLengthWidthUnits': room.get('roomLengthWidthUnits'),
                    'roomLength': room.get('roomLength'),
                    'roomFeatures': room.get('roomFeatures'),
                    'roomDimensions': room.get('roomDimensions'),
                    'roomLengthWidthSource': room.get('roomLengthWidthSource'),
                    'area': room.get('area')
                }
                # Optionally, you can add 'zpid' here if needed for reference
                room_record['zpid'] = data.get('zpid')
                rooms_records.append(room_record)

    return rooms_records

def extract_listing_subtype(data):
    """
    Extracts and structures data from the 'listingSubType' key of the input dictionary.
    Captures boolean values as a structured record and appends additional fields like 'zpid'.
    """
    structured_records = []

    # Validate that data is a dictionary
    if not isinstance(data, dict):
        logging.error(f"Expected a dictionary but got: {type(data)}")
        return structured_records

    # Access 'listingSubType' and validate it as a dictionary
    listing_subtype = data.get('listingSubType')
    if not listing_subtype or not isinstance(listing_subtype, dict):
        logging.warning("No 'listingSubType' key found or it is not a dictionary.")
        return structured_records

    # Create a structured record for the boolean values in 'listingSubType'
    structured_record = {
        key: value
        for key, value in listing_subtype.items()
        if isinstance(value, bool)
    }

    # Add 'zpid' separately if needed
    structured_record['zpid'] = data.get('zpid')

    # Add the structured record to the list
    structured_records.append(structured_record)

    return structured_records

def extract_listed_by(data):
    """
    Extracts and structures data from the 'listed_by' key of the input dictionary.
    Captures key-value pairs as a structured record and appends additional fields like 'zpid'.
    """
    structured_records = []

    # Validate that data is a dictionary
    if not isinstance(data, dict):
        logging.error(f"Expected a dictionary but got: {type(data)}")
        return structured_records

    # Access 'listed_by' and validate it as a dictionary
    listed_by = data.get('listed_by')
    if not listed_by or not isinstance(listed_by, dict):
        logging.warning("No 'listed_by' key found or it is not a dictionary.")
        return structured_records

    # Create a structured record for key-value pairs in 'listed_by'
    structured_record = {
        key: value
        for key, value in listed_by.items()
    }

    # Add 'zpid' separately if needed
    structured_record['zpid'] = data.get('zpid')

    # Add the structured record to the list
    structured_records.append(structured_record)

    structured_records=clean_data_phone(structured_records)

    return structured_records

def extract_mortgage_info(data):
    """
    Extracts and structures data from the 'mortgageZHLRates' key of the input dictionary.
    Captures nested rate data as structured records and appends additional fields like 'zpid'.
    """
    structured_records = []

    # Validate that data is a dictionary
    if not isinstance(data, dict):
        logging.error(f"Expected a dictionary but got: {type(data)}")
        return structured_records

    # Access 'mortgageZHLRates' and validate it as a dictionary
    mortgage_rates = data.get('mortgageZHLRates')
    if not mortgage_rates or not isinstance(mortgage_rates, dict):
        logging.warning("No 'mortgageZHLRates' key found or it is not a dictionary.")
        return structured_records

    # Iterate over each rate bucket in 'mortgageZHLRates'
    for bucket_name, bucket_data in mortgage_rates.items():
        if isinstance(bucket_data, dict):
            structured_record = {
                'zpid': data.get('zpid'),
                'bucketType': bucket_name,
                'rate': bucket_data.get('rate'),
                'rateSource': bucket_data.get('rateSource'),
                'lastUpdated': bucket_data.get('lastUpdated')
            }
            structured_records.append(structured_record)
        else:
            logging.warning(f"Expected a dictionary for bucket '{bucket_name}' but got: {type(bucket_data)}")

    return structured_records