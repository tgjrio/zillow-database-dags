import requests
import time
import logging
from core import settings


def make_request_with_retries(url, querystring, headers=None, max_retries=5):
    if headers is None:
        headers = {
            "X-RapidAPI-Key": settings.ZILLOW_TOKEN,
            "X-RapidAPI-Host": "zillow-com1.p.rapidapi.com"
        }

    retry_count = 0
    while retry_count < max_retries:
        response = requests.get(url, headers=headers, params=querystring)
        if response.status_code == 200:
            return response.json()
        elif response.status_code == 429:
            retry_count += 1
            wait_time = 2 ** retry_count  # Exponential backoff
            logging.warning(f"Rate limit hit. Retrying in {wait_time} seconds...")
            time.sleep(wait_time)
        else:
            logging.error(f"Failed to fetch data: {response.status_code}")
            return None
    logging.error("Max retries exceeded")
    return None