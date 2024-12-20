import logging
from core.zillow.configurations import settings
from core.zillow.services.api_requests import make_request_with_retries

def fetch_zpids_for_sale(location, **kwargs):
        headers = {
            "X-RapidAPI-Key": settings.ZILLOW_TOKEN,
            "X-RapidAPI-Host": "zillow-com1.p.rapidapi.com"
        }
        days_on_market = kwargs.get('days_on_market', '1')
        status_type = kwargs.get('status_type', 'ForSale')

        querystring = {
            "location": location,
            "daysOn": days_on_market,
            "status_type": status_type,
            "page": 1
        }
        data = make_request_with_retries(settings.SEARCH_URL, querystring, headers, 5)
        zpid_list = []

        if data and isinstance(data, dict):
            total_pages = data.get('totalPages', 1)
            for page in range(1, total_pages + 1):
                querystring['page'] = page
                page_data = make_request_with_retries(settings.SEARCH_URL, querystring, headers, 5)
                if page_data and isinstance(page_data, dict):
                    properties = page_data.get('props', [])
                    zpid_list.extend([prop.get('zpid') for prop in properties if prop.get('zpid')])
                    logging.info(f"Fetched page {page} of {total_pages} for location '{location}'")
                else:
                    logging.warning(f"Skipping page {page} due to repeated failures or invalid data structure.")
        else:
            logging.error(f"Initial request for location '{location}' failed.")

        logging.info(f"Total zpids fetched for location '{location}': {len(zpid_list)}")
        return zpid_list


def fetch_zpids_sold(location, **kwargs):
        headers = {
            "X-RapidAPI-Key": settings.ZILLOW_TOKEN,
            "X-RapidAPI-Host": "zillow-com1.p.rapidapi.com"
        }
        # soldInLast = kwargs.get('soldInLast', '1')
        # status_type = kwargs.get('status_type', 'RecentlySold')

        querystring = {
            "location": location,
            'status_type': 'RecentlySold',
            "soldInLast": '1',
            "page": 1
        }
        data = make_request_with_retries(settings.SEARCH_URL, querystring, headers, 5)
        zpid_list = []

        if data and isinstance(data, dict):
            total_pages = data.get('totalPages', 1)
            for page in range(1, total_pages + 1):
                querystring['page'] = page
                page_data = make_request_with_retries(settings.SEARCH_URL, querystring, headers, 5)
                if page_data and isinstance(page_data, dict):
                    properties = page_data.get('props', [])
                    zpid_list.extend([prop.get('zpid') for prop in properties if prop.get('zpid')])
                    logging.info(f"Fetched page {page} of {total_pages} for location '{location}'")
                else:
                    logging.warning(f"Skipping page {page} due to repeated failures or invalid data structure.")
        else:
            logging.error(f"Initial request for location '{location}' failed.")

        logging.info(f"Total zpids fetched for location '{location}': {len(zpid_list)}")
        return zpid_list