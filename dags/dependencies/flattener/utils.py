import logging
import subprocess
import sys
from typing import Any, Optional

import dependencies.flattener.constants as constants
import requests  # type: ignore

"""
Set up a logging instance that will write to stdout (and therefore show up in Google Cloud logs)
"""
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
# Create the logger at module level so its settings are applied throughout code base
logger = logging.getLogger(__name__)

def check_service_health(base_url: str) -> dict:
    """
    Call the heartbeat endpoint to check service health.
    """
    logger.info("Trying to get API health status")
    try:        
        # Make authenticated request
        response = requests.get(
            f"{base_url}/heartbeat",
            headers=get_auth_header()
        )
        response.raise_for_status()
        return response.json()
        
    except subprocess.CalledProcessError as e:
        raise Exception(f"Error getting authentication token: {e}") from e
    except requests.exceptions.RequestException as e:
        raise Exception(f"Error checking service health: {e}") from e

def get_gcloud_token() -> str:
    """
    Get identity token from gcloud CLI
    """
    try:
        # subprocess runs a system command; using it to obtain and then return token
        token = subprocess.check_output(
            ['gcloud', 'auth', 'print-identity-token'],
            universal_newlines=True
        ).strip()
        return token
    except subprocess.CalledProcessError as e:
        logger.error(f"Failed to get gcloud token: {e}")
        raise Exception(f"Failed to get gcloud token: {e}") from e

def get_auth_header() -> dict[str, str]:
    """
    Set up headers with bearer token
    """
    headers = {'Authorization': f'Bearer {get_gcloud_token()}'}
    return headers

def make_api_call(endpoint: str, method: str = "post", 
                 params: Optional[dict[str, str]] = None, 
                 json_data: Optional[dict[str, Any]] = None, 
                 timeout: Optional[tuple] = None) -> Optional[Any]:
    """
    Makes an API call to the processor endpoint with standardized error handling.
    """
    url = f"{constants.PROCESSOR_ENDPOINT}/{endpoint}"

    # pipeline_log calls are made often and clutter the logs, don't display this message
    if endpoint != "pipeline_log":
        logger.info(f"Making {method.upper()} request to {url}")
    
    try:
        if method.lower() == "get":
            response = requests.get(
                url,
                headers=get_auth_header(),
                params=params,
                timeout=timeout
            )
        else:  # POST
            response = requests.post(
                url,
                headers=get_auth_header(),
                json=json_data,
                timeout=timeout
            )
        
        # Check if the request was successful
        if response.status_code != 200:
            error_message = f"API Error {response.status_code} from {endpoint}: {response.text}"
            logger.error(error_message)
            raise Exception(error_message)
        
        # Try to parse as JSON, but handle non-JSON responses
        if response.content:
            try:
                return response.json()
            except ValueError:
                # Not JSON, return text instead
                return response.text
        return None
        
    except requests.exceptions.JSONDecodeError as e:
        # This shouldn't normally be reached with the try/except above,
        # but keeping it as a fallback
        logger.warning(f"Response from {endpoint} was not valid JSON: {response.text}")
        return response.text
    except subprocess.CalledProcessError as e:
        error_msg = f"Error getting authentication token for {endpoint}: {str(e)}"
        logger.error(error_msg)
        raise Exception(error_msg) from e
    except requests.exceptions.RequestException as e:
        error_msg = f"Network error when calling {endpoint}: {str(e)}"
        logger.error(error_msg)
        raise Exception(error_msg) from e