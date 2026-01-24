import requests

def load_metadata(metadata_url: str) -> dict:
    """
    Load metadata dynamically from GitHub
    """
    response = requests.get(metadata_url, timeout=30)
    response.raise_for_status()
    return response.json()
