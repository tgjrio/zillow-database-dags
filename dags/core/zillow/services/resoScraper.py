import logging

logging.basicConfig(
    level=logging.INFO,  # You can change to logging.DEBUG for more verbose output
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s [%(funcName)s]',
    datefmt='%Y-%m-%d %H:%M:%S'
)

def extract_property_basic_info(data):
    """
    Extracts data for the 'property_basic_info' table from a given input dictionary.

    Parameters:
    - data (dict): Input dictionary representing a single record.

    Returns:
    - dict: Extracted data containing fields relevant to 'property_basic_info'.
    """
    if not isinstance(data, dict):
        logging.error(f"Expected a dictionary but got: {type(data)}")
        return {}

    extracted_data = {
        "zpid": data.get("zpid"),
        "listingId": data.get("listingId"),
        "yearBuilt": data.get("yearBuilt"),
        "yearBuiltEffective": data.get("yearBuiltEffective"),
        "parcelNumber": data.get("parcelNumber"),
        "propertyCondition": data.get("propertyCondition"),
        "municipality": data.get("municipality"),
        "buildingName": data.get("buildingName"),
        "subdivisionName": data.get("subdivisionName"),
        "homeType": data.get("homeType"),
        "ownershipType": data.get("ownershipType")
    }

    return extracted_data

def extract_location_area_info(data):
    """
    Extracts data for the 'location_area_info' table from a given input dictionary.

    Parameters:
    - data (dict): Input dictionary representing a single record.

    Returns:
    - dict: Extracted data containing fields relevant to 'location_area_info'.
    """
    if not isinstance(data, dict):
        logging.error(f"Expected a dictionary but got: {type(data)}")
        return {}

    extracted_data = {
        "zpid": data.get("zpid"),
        "lotSize": data.get("lotSize"),
        "lotSizeDimensions": data.get("lotSizeDimensions"),
        "livingArea": data.get("livingArea"),
        "livingAreaRange": data.get("livingAreaRange"),
        "buildingArea": data.get("buildingArea"),
        "buildingAreaSource": data.get("buildingAreaSource"),
        "aboveGradeFinishedArea": data.get("aboveGradeFinishedArea"),
        "belowGradeFinishedArea": data.get("belowGradeFinishedArea"),
        "elevation": data.get("elevation"),
        "elevationUnits": data.get("elevationUnits"),
        "topography": data.get("topography")
    }

    return extracted_data

def extract_associations_info(data):
    """
    Extracts data for the 'associations_fees' table from a given input dictionary.

    Parameters:
    - data (dict): Input dictionary representing a single record.

    Returns:
    - dict: Extracted data containing fields relevant to 'associations_fees'.
    """
    if not isinstance(data, dict):
        logging.error(f"Expected a dictionary but got: {type(data)}")
        return {}

    extracted_data = {
        "zpid": data.get("zpid"),
        "hasAssociation": data.get("hasAssociation"),
        "associationName": data.get("associationName"),
        "associationName2": data.get("associationName2"),
        "associationPhone": data.get("associationPhone"),
        "associationPhone2": data.get("associationPhone2"),
        "associationFee": data.get("associationFee"),
        "associationFee2": data.get("associationFee2"),
        "associationFeeIncludes": data.get("associationFeeIncludes"),
        "hoaFee": data.get("hoaFee"),
        "hoaFeeTotal": data.get("hoaFeeTotal"),
        "associationAmenities": data.get("associationAmenities")
    }

    return extracted_data

def extract_utilities_info(data):
    """
    Extracts data for the 'utility_energy_features' table from a given input dictionary.

    Parameters:
    - data (dict): Input dictionary representing a single record.

    Returns:
    - dict: Extracted data containing fields relevant to 'utility_energy_features'.
    """
    if not isinstance(data, dict):
        logging.error(f"Expected a dictionary but got: {type(data)}")
        return {}

    extracted_data = {
        "zpid": data.get("zpid"),
        "utilities": data.get("utilities"),
        "waterSource": data.get("waterSource"),
        "sewer": data.get("sewer"),
        "gas": data.get("gas"),
        "hasElectricOnProperty": data.get("hasElectricOnProperty"),
        "greenWaterConservation": data.get("greenWaterConservation"),
        "greenEnergyGeneration": data.get("greenEnergyGeneration"),
        "greenIndoorAirQuality": data.get("greenIndoorAirQuality"),
        "greenSustainability": data.get("greenSustainability"),
        "greenBuildingVerificationType": data.get("greenBuildingVerificationType")
    }

    return extracted_data

def extract_building_interior_info(data):
    """
    Extracts data for the 'building_interior_features' table from a given input dictionary.

    Parameters:
    - data (dict): Input dictionary representing a single record.

    Returns:
    - dict: Extracted data containing fields relevant to 'building_interior_features'.
    """
    if not isinstance(data, dict):
        logging.error(f"Expected a dictionary but got: {type(data)}")
        return {}

    extracted_data = {
        "zpid": data.get("zpid"),
        "stories": data.get("stories"),
        "storiesTotal": data.get("storiesTotal"),
        "storiesDecimal": data.get("storiesDecimal"),
        "levels": data.get("levels"),
        "hasAttachedProperty": data.get("hasAttachedProperty"),
        "hasAttachedGarage": data.get("hasAttachedGarage"),
        "hasCarport": data.get("hasCarport"),
        "hasGarage": data.get("hasGarage"),
        "carportParkingCapacity": data.get("carportParkingCapacity"),
        "garageParkingCapacity": data.get("garageParkingCapacity"),
        "coveredParkingCapacity": data.get("coveredParkingCapacity"),
        "openParkingCapacity": data.get("openParkingCapacity")
    }

    return extracted_data


def extract_amenities_info(data):
    """
    Extracts data for the 'amenities_community_features' table from a given input dictionary.

    Parameters:
    - data (dict): Input dictionary representing a single record.

    Returns:
    - dict: Extracted data containing fields relevant to 'amenities_community_features'.
    """
    if not isinstance(data, dict):
        logging.error(f"Expected a dictionary but got: {type(data)}")
        return {}

    extracted_data = {
        "zpid": data.get("zpid"),
        "hasPrivatePool": data.get("hasPrivatePool"),
        "hasSpa": data.get("hasSpa"),
        "spaFeatures": data.get("spaFeatures"),
        "poolFeatures": data.get("poolFeatures"),
        "communityFeatures": data.get("communityFeatures"),
        "accessibilityFeatures": data.get("accessibilityFeatures"),
        "horseAmenities": data.get("horseAmenities"),
        "canRaiseHorses": data.get("canRaiseHorses"),
        "horseYN": data.get("horseYN")
    }

    return extracted_data

def extract_market_pricing_info(data):
    """
    Extracts data for the 'market_pricing_info' table from a given input dictionary.

    Parameters:
    - data (dict): Input dictionary representing a single record.

    Returns:
    - dict: Extracted data containing fields relevant to 'market_pricing_info'.
    """
    if not isinstance(data, dict):
        logging.error(f"Expected a dictionary but got: {type(data)}")
        return {}

    extracted_data = {
        "zpid": data.get("zpid"),
        "pricePerSquareFoot": data.get("pricePerSquareFoot"),
        "taxAnnualAmount": data.get("taxAnnualAmount"),
        "taxAssessedValue": data.get("taxAssessedValue"),
        "cumulativeDaysOnMarket": data.get("cumulativeDaysOnMarket"),
        "daysOnZillow": data.get("daysOnZillow"),
        "onMarketDate": data.get("onMarketDate"),
        "offerReviewDate": data.get("offerReviewDate"),
        "listingTerms": data.get("listingTerms"),
        "specialListingConditions": data.get("specialListingConditions"),
        "contingency": data.get("contingency")
    }

    return extracted_data

def extract_miscellaneous_info(data):
    """
    Extracts data for the 'miscellaneous_features' table from a given input dictionary.

    Parameters:
    - data (dict): Input dictionary representing a single record.

    Returns:
    - dict: Extracted data containing fields relevant to 'miscellaneous_features'.
    """
    if not isinstance(data, dict):
        logging.error(f"Expected a dictionary but got: {type(data)}")
        return {}

    extracted_data = {
        "zpid": data.get("zpid"),
        "roomTypes": data.get("roomTypes"),
        "flooring": data.get("flooring"),
        "roofType": data.get("roofType"),
        "attic": data.get("attic"),
        "fencing": data.get("fencing"),
        "fireplaceFeatures": data.get("fireplaceFeatures"),
        "inclusions": data.get("inclusions"),
        "exclusions": data.get("exclusions"),
        "appliances": data.get("appliances"),
        "furnished": data.get("furnished"),
        "otherStructures": data.get("otherStructures"),
        "exteriorFeatures": data.get("exteriorFeatures")
    }

    return extracted_data