import logging
import json

def extract_property_details(json_data):
    if not isinstance(json_data, dict):
        logging.error(f"Expected a dictionary but got: {type(json_data)}")
        return []

    try:
        address = json_data.get('address', {})
        if not isinstance(address, dict):
            logging.warning(f"Expected 'address' to be a dictionary but got: {type(address)}")
            address = {}

        listing_sub_type = json_data.get('listingSubType', {})
        if not isinstance(listing_sub_type, dict):
            logging.warning(f"Expected 'listingSubType' to be a dictionary but got: {type(listing_sub_type)}")
            listing_sub_type = {}

        flat_record = {
            'zpid': json_data.get('zpid', None),
            'address': address.get('streetAddress', None),
            'city': address.get('city', None),
            'state': address.get('state', None),
            'zipcode': address.get('zipcode', None),
            'neighborhood': address.get('neighborhood', None),
            'subdivision': address.get('subdivision', None),
            'price': json_data.get('price', None),
            'bedrooms': json_data.get('bedrooms', None),
            'bathrooms': json_data.get('bathrooms', None),
            'livingArea': json_data.get('livingArea', None),
            'homeType': json_data.get('homeType', None),
            'homeStatus': json_data.get('homeStatus', None),
            'zestimate': json_data.get('zestimate', None),
            'latitude': json_data.get('latitude', None),
            'longitude': json_data.get('longitude', None),
            'imgSrc': json_data.get('imgSrc', None),
            'url': json_data.get('url', None),
            'yearBuilt': json_data.get('yearBuilt', None),
            'listingStatus': listing_sub_type.get('is_FSBA', None),
            'favoriteCount': json_data.get('favoriteCount', None),
            'timeOnZillow': json_data.get('timeOnZillow', None),
            'county': json_data.get('county', None),
            'propertyTaxRate': json_data.get('propertyTaxRate', None),
            'annualHomeownersInsurance': json_data.get('annualHomeownersInsurance', None),
        }

        return [flat_record]
    except Exception as e:
        logging.error(f"Unexpected error while extracting property details: {e}")
        return []

def extract_price_history(json_data):
    if not isinstance(json_data, dict):
        logging.error(f"Expected a dictionary but got: {type(json_data)}")
        return []

    price_history_records = []
    zpid = json_data.get('zpid')

    try:
        price_history_list = json_data.get('priceHistory', [])
        if isinstance(price_history_list, dict):
            logging.warning(f"Expected 'priceHistory' to be a list but got: {type(price_history_list)}")
            price_history_list = [price_history_list]  # Convert to list for consistent processing

        if not isinstance(price_history_list, list):
            logging.error(f"Failed to process 'priceHistory'. Expected list but got: {type(price_history_list)}")
            return []

        for record in price_history_list:
            if not isinstance(record, dict):
                logging.warning(f"Expected each record in 'priceHistory' to be a dictionary but got: {type(record)}")
                continue  # Skip malformed records

            price_history_record = {
                'zpid': zpid,
                'date': record.get('date', None),
                'event': record.get('event', None),
                'price': record.get('price', None),
                'priceChangeRate': record.get('priceChangeRate', None),
                'pricePerSquareFoot': record.get('pricePerSquareFoot', None),
                'postingIsRental': record.get('postingIsRental', None),
                'source': record.get('source', None),
                'time': record.get('time', None),
            }
            price_history_records.append(price_history_record)

    except Exception as e:
        logging.error(f"Unexpected error while extracting price history: {e}")

    return price_history_records


def extract_school_info(json_data):
    if not isinstance(json_data, dict):
        logging.error(f"Expected a dictionary but got: {type(json_data)}")
        return []

    # Initialize list to hold extracted school information records
    school_info_records = []

    # Get zpid for relationship
    zpid = json_data.get('zpid')

    try:
        # Extract school data if available
        schools_list = json_data.get('schools', [])
        if not isinstance(schools_list, list):
            logging.warning(f"Expected 'schools' to be a list but got: {type(schools_list)}")
            return []

        for school in schools_list:
            if not isinstance(school, dict):
                logging.warning(f"Expected each school record to be a dictionary but got: {type(school)}")
                continue  # Skip invalid school records

            # Flatten and extract relevant data from each school record
            school_record = {
                'zpid': zpid,
                'schoolName': school.get('name', None),
                'level': school.get('level', None),
                'grades': school.get('grades', None),
                'distance': school.get('distance', None),
                'rating': school.get('rating', None),
                'link': school.get('link', None),
                'type': school.get('type', None),
                'isAssigned': school.get('isAssigned', None),
                'size': school.get('size', None),
                'studentsPerTeacher': school.get('studentsPerTeacher', None),
                'totalCount': school.get('totalCount', None)
            }
            # Add the record to the list
            school_info_records.append(school_record)

    except Exception as e:
        logging.error(f"Unexpected error while extracting school information: {e}")

    return school_info_records

def extract_attribution_info(json_data):
    if not isinstance(json_data, dict):
        logging.error(f"Expected a dictionary but got: {type(json_data)}")
        return {}

    # Get zpid for relationship
    zpid = json_data.get('zpid')

    try:
        # Extract attribution info if available
        attribution_info = json_data.get('attributionInfo', {})
        if not isinstance(attribution_info, dict):
            logging.warning(f"Expected 'attributionInfo' to be a dictionary but got: {type(attribution_info)}")
            attribution_info = {}  # Fallback to an empty dictionary

        # Create a record with relevant fields
        attribution_record = {
            'zpid': zpid,
            'agentEmail': attribution_info.get('agentEmail', None),
            'agentLicenseNumber': attribution_info.get('agentLicenseNumber', None),
            'agentName': attribution_info.get('agentName', None),
            'agentPhoneNumber': attribution_info.get('agentPhoneNumber', None),
            'brokerName': attribution_info.get('brokerName', None),
            'brokerPhoneNumber': attribution_info.get('brokerPhoneNumber', None),
            'attributionTitle': attribution_info.get('attributionTitle', None),
            'buyerAgentMemberStateLicense': attribution_info.get('buyerAgentMemberStateLicense', None),
            'buyerAgentName': attribution_info.get('buyerAgentName', None),
            'buyerBrokerageName': attribution_info.get('buyerBrokerageName', None),
            'coAgentLicenseNumber': attribution_info.get('coAgentLicenseNumber', None),
            'coAgentName': attribution_info.get('coAgentName', None),
            'coAgentNumber': attribution_info.get('coAgentNumber', None),
            'lastChecked': attribution_info.get('lastChecked', None),
            'lastUpdated': attribution_info.get('lastUpdated', None),
            'listingAgreement': attribution_info.get('listingAgreement', None),
            'mlsId': attribution_info.get('mlsId', None),
            'mlsName': attribution_info.get('mlsName', None),
            'trueStatus': attribution_info.get('trueStatus', None)
        }

        return attribution_record
    except Exception as e:
        logging.error(f"Unexpected error while extracting attribution information: {e}")
        return {}

def extract_tax_history(tax_history_data):
    if not isinstance(tax_history_data, dict):
        logging.error(f"Expected a dictionary but got: {type(tax_history_data)}")
        return []

    flat_data = []
    zpid = tax_history_data.get('zpid')

    try:
        tax_history = tax_history_data.get('taxHistory', [])
        if isinstance(tax_history, dict):
            logging.warning(f"Expected 'taxHistory' to be a list but got: {type(tax_history)}")
            tax_history = [tax_history]  # Convert to list for consistent processing

        if not isinstance(tax_history, list):
            logging.error(f"Failed to process 'taxHistory'. Expected list but got: {type(tax_history)}")
            return []
        
        for record in tax_history:
            if not isinstance(record, dict):
                logging.warning(f"Expected each tax history record to be a dictionary but got: {type(record)}")
                continue  # Skip invalid records

            flat_record = {
                'zpid': zpid,
                'taxPaid': record.get('taxPaid', None),
                'value': record.get('value', None),
                'taxIncreaseRate': record.get('taxIncreaseRate', None),
                'time': record.get('time', None)
            }
            flat_data.append(flat_record)

    except Exception as e:
        logging.error(f"Unexpected error while flattening tax history data: {e}")

    return flat_data

def extract_reso_facts(reso_facts_data):
    if not isinstance(reso_facts_data, dict):
        logging.error(f"Expected a dictionary but got: {type(reso_facts_data)}")
        return []

    flat_record = []
    zpid = reso_facts_data.get('zpid')

    try:
        reso_facts = reso_facts_data.get('resoFacts', {})
        if not isinstance(reso_facts, dict):
            logging.warning(f"Expected 'resoFacts' to be a dictionary but got: {type(reso_facts)}")
            reso_facts = {}  # Fallback to an empty dictionary

        flat_reso_facts = {
            'zpid': zpid,
            'aboveGradeFinishedArea': reso_facts.get('aboveGradeFinishedArea', None),
            'appliances': ', '.join(reso_facts.get('appliances', [])) if isinstance(reso_facts.get('appliances'), list) else reso_facts.get('appliances', None),
            'architecturalStyle': reso_facts.get('architecturalStyle', None),
            'associationFee': reso_facts.get('associationFee', None),
            'associationFeeIncludes': ', '.join(reso_facts.get('associationFeeIncludes', [])) if isinstance(reso_facts.get('associationFeeIncludes'), list) else reso_facts.get('associationFeeIncludes', None),
            'bathrooms': reso_facts.get('bathrooms', None),
            'bedrooms': reso_facts.get('bedrooms', None),
            'buildingArea': reso_facts.get('buildingArea', None),
            'cooling': ', '.join(reso_facts.get('cooling', [])) if isinstance(reso_facts.get('cooling'), list) else reso_facts.get('cooling', None),
            'heating': ', '.join(reso_facts.get('heating', [])) if isinstance(reso_facts.get('heating'), list) else reso_facts.get('heating', None),
            'interiorFeatures': ', '.join(reso_facts.get('interiorFeatures', [])) if isinstance(reso_facts.get('interiorFeatures'), list) else reso_facts.get('interiorFeatures', None),
            'lotSize': reso_facts.get('lotSize', None),
            'parkingFeatures': ', '.join(reso_facts.get('parkingFeatures', [])) if isinstance(reso_facts.get('parkingFeatures'), list) else reso_facts.get('parkingFeatures', None),
            'propertyCondition': reso_facts.get('propertyCondition', None),
            'propertyTypeDimension': reso_facts.get('propertyTypeDimension', None),
            'roofType': reso_facts.get('roofType', None),
            'securityFeatures': ', '.join(reso_facts.get('securityFeatures', [])) if isinstance(reso_facts.get('securityFeatures'), list) else reso_facts.get('securityFeatures', None),
            'stories': reso_facts.get('stories', None),
            'yearBuilt': reso_facts.get('yearBuilt', None)
        }

        flat_record.append(flat_reso_facts)

    except Exception as e:
        logging.error(f"Unexpected error while flattening reso facts: {e}")

    return flat_record

def preprocess_json_file(input_gcs_uri, output_local_path):
        from google.cloud import storage
        import tempfile

        # Initialize a GCS client
        storage_client = storage.Client()
        bucket_name = input_gcs_uri.split('/')[2]
        blob_name = '/'.join(input_gcs_uri.split('/')[3:])
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(blob_name)

        # Download the file locally for processing
        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            blob.download_to_filename(temp_file.name)

            # Read and preprocess the data
            cleaned_data = []
            with open(temp_file.name, 'r') as f:
                for line in f:
                    record = json.loads(line)
                    # Replace empty object with an empty string
                    if isinstance(record.get("parkingFeatures"), dict) and not record["parkingFeatures"]:
                        record["parkingFeatures"] = ""
                    cleaned_data.append(record)

            # Write the cleaned data to a new file
            with open(output_local_path, 'w') as f:
                for record in cleaned_data:
                    f.write(json.dumps(record) + '\n')

        # Upload the cleaned file back to GCS
        output_blob = bucket.blob('path/to/cleaned_reso_facts.json')
        output_blob.upload_from_filename(output_local_path)