"""Domo authentication module."""

import os
import logging
from domo_utils.auth import DeveloperTokenAuth, ClientCredentialsAuth
from domo_utils.api import get_dataset_api

logger = logging.getLogger(__name__)


class DomoAuth:
    """Simple Domo authentication handler."""
    
    def __init__(self):
        self.dataset_api = None
    
    def authenticate(self):
        """Authenticate with Domo using environment variables."""
        # Try Developer Token first
        dev_token = os.getenv("DOMO_DEVELOPER_TOKEN")
        instance = os.getenv("DOMO_INSTANCE")
        
        if dev_token and instance:
            auth_client = DeveloperTokenAuth(token=dev_token, instance_id=instance)
            auth_client.connect()
            self.dataset_api = get_dataset_api(auth_client)
            logger.info("✅ Authenticated with Developer Token")
            return
        
        # Try Client Credentials
        client_id = os.getenv("DOMO_CLIENT_ID")
        client_secret = os.getenv("DOMO_CLIENT_SECRET")
        
        if client_id and client_secret and instance:
            auth_client = ClientCredentialsAuth(
                client_id=client_id,
                client_secret=client_secret,
                api_host=f"{instance}.domo.com"
            )
            auth_client.connect()
            self.dataset_api = get_dataset_api(auth_client)
            logger.info("✅ Authenticated with Client Credentials")
            return
        
        raise ValueError("❌ No valid Domo credentials found in environment")
    
    @property
    def is_authenticated(self) -> bool:
        return self.dataset_api is not None
