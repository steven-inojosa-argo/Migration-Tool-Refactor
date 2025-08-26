import os.path
import logging
from typing import Any, List, Optional, Dict, TypedDict, Union, cast, TYPE_CHECKING
from os import getenv

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google.oauth2 import service_account
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build, Resource
from googleapiclient.errors import HttpError

import polars as pl
import pandas as pd  # Still needed for compatibility

# Import types from stubs only for type checking
if TYPE_CHECKING:
    from googleapiclient._apis.sheets.v4 import SheetsResource
    from googleapiclient._apis.sheets.v4.schemas import (
        ValueRange,
        BatchUpdateSpreadsheetRequest,
        UpdateValuesResponse,
        BatchUpdateSpreadsheetResponse,
        Spreadsheet,
    )

# Default scopes for read-only access, can be expanded as needed
DEFAULT_SCOPES = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
# For read and write access
READ_WRITE_SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]


class GoogleSheets:

    # Use string annotation to avoid runtime errors
    sheet: "SheetsResource.SpreadsheetsResource"

    def __init__(
        self,
        credentials_path: str = "credentials.json",
        scopes: List[str] = DEFAULT_SCOPES,
    ):
        """
        Initialize the GoogleSheets client

        Args:
            credentials_path (str): Path to the service account JSON file
            scopes (Optional[List[str]]): List of scopes to request
        """
        self.credentials_path = credentials_path
        self.scopes = scopes
        self.logger = logging.getLogger("GSheets")

        # Initialize the service
        self._authenticate()

    def _authenticate(self):
        """Authenticate with Google Sheets API"""
        try:
            # Initialize credentials from the service account file
            creds = service_account.Credentials.from_service_account_file(
                self.credentials_path, scopes=self.scopes
            )

            # Use cast() instead of type comment
            service = cast("SheetsResource", build("sheets", "v4", credentials=creds))
            self.sheet = service.spreadsheets()
            self.logger.info(
                "Successfully authenticated with Google Sheets API using service account"
            )
        except Exception as err:
            self.logger.error(f"Failed to authenticate with Google Sheets API: {err}")
            raise

    def read_range(self, spreadsheet_id: str, range_name: str) -> List[List[Any]]:
        """
        Read data from a specific range in a spreadsheet

        Args:
            spreadsheet_id (str): The ID of the spreadsheet
            range_name (str): The range to read (e.g., 'Sheet1!A1:D10')

        Returns:
            List[List[Any]]: The values from the specified range
        """
        try:
            result = (
                self.sheet.values()
                .get(spreadsheetId=spreadsheet_id, range=range_name)
                .execute()
            )
            values = result.get("values", [])
            return values
        except HttpError as err:
            self.logger.error(f"Error reading range: {err}")
            return []

    def read_to_dataframe(
        self, spreadsheet_id: str, range_name: str, header: bool = True
    ) -> pd.DataFrame:
        """
        Read data from a specific range in a spreadsheet into a pandas DataFrame

        Args:
            spreadsheet_id (str): The ID of the spreadsheet
            range_name (str): The range to read (e.g., 'Sheet1!A1:D10')
            header (bool): Whether to treat the first row as column names

        Returns:
            pd.DataFrame: The data as a pandas DataFrame
        """
        values = self.read_range(spreadsheet_id, range_name)

        if not values:
            return pd.DataFrame()

        if header and len(values) > 0:
            # Get the headers from the first row
            headers = values[0]

            # Get the data rows
            data_rows = values[1:] if len(values) > 1 else []

            # Create pandas DataFrame
            pandas_df = pd.DataFrame(data_rows, columns=headers)
            return pandas_df
        else:
            # For non-header data, create pandas DataFrame
            pandas_df = pd.DataFrame(values)
            return pandas_df

    def write_range(
        self, spreadsheet_id: str, range_name: str, values: List[List[Any]]
    ) -> "UpdateValuesResponse":
        """
        Write data to a specific range in a spreadsheet

        Args:
            spreadsheet_id (str): The ID of the spreadsheet
            range_name (str): The range to write to (e.g., 'Sheet1!A1:D10')
            values (List[List[Any]]): The values to write

        Returns:
            UpdateValuesResponse: The response from the API
        """
        if "https://www.googleapis.com/auth/spreadsheets" not in self.scopes:
            self.logger.error(
                "Write operation requires read-write scope. Please initialize with READ_WRITE_SCOPES"
            )
            raise PermissionError(
                "Write operation requires read-write scope. Please initialize with READ_WRITE_SCOPES"
            )

        try:
            # Create a ValueRange object that the API expects
            body: ValueRange = {"values": values}
            result = (
                self.sheet.values()
                .update(
                    spreadsheetId=spreadsheet_id,
                    range=range_name,
                    valueInputOption="USER_ENTERED",
                    body=body,
                )
                .execute()
            )
            self.logger.info(f"Updated {result.get('updatedCells')} cells")
            return result
        except HttpError as err:
            self.logger.error(f"Error writing to range: {err}")
            raise

    def write_dataframe(
        self,
        df: pd.DataFrame,
        spreadsheet_id: str,
        range_name: str,
        include_header: bool = True,
    ) -> "UpdateValuesResponse":
        """
        Write a pandas DataFrame to a specific range in a spreadsheet

        Args:
            df (pd.DataFrame): The DataFrame to write
            spreadsheet_id (str): The ID of the spreadsheet
            range_name (str): The range to write to (e.g., 'Sheet1!A1')
            include_header (bool): Whether to include column names as the first row

        Returns:
            UpdateValuesResponse: The response from the API
        """
        if "https://www.googleapis.com/auth/spreadsheets" not in self.scopes:
            self.logger.error(
                "Write operation requires read-write scope. Please initialize with READ_WRITE_SCOPES"
            )
            raise PermissionError(
                "Write operation requires read-write scope. Please initialize with READ_WRITE_SCOPES"
            )

        try:
            # DataFrame is already pandas, no conversion needed
            pandas_df = df

            # Convert DataFrame to list of lists
            if include_header:
                values = [pandas_df.columns.tolist()] + pandas_df.values.tolist()
            else:
                values = pandas_df.values.tolist()

            return self.write_range(spreadsheet_id, range_name, values)
        except Exception as err:
            self.logger.error(f"Error writing DataFrame to range: {err}")
            raise

    def update_cell(
        self, spreadsheet_id: str, range_name: str, value: Any
    ) -> "UpdateValuesResponse":
        """
        Update a single cell in a spreadsheet

        Args:
            spreadsheet_id (str): The ID of the spreadsheet
            range_name (str): The cell range (e.g., 'Sheet1!A1')
            value (Any): The value to write to the cell

        Returns:
            UpdateValuesResponse: The response from the API
        """
        if "https://www.googleapis.com/auth/spreadsheets" not in self.scopes:
            self.logger.error(
                "Write operation requires read-write scope. Please initialize with READ_WRITE_SCOPES"
            )
            raise PermissionError(
                "Write operation requires read-write scope. Please initialize with READ_WRITE_SCOPES"
            )

        try:
            # Create a ValueRange object with a single value
            body: ValueRange = {"values": [[value]]}
            result = (
                self.sheet.values()
                .update(
                    spreadsheetId=spreadsheet_id,
                    range=range_name,
                    valueInputOption="USER_ENTERED",
                    body=body,
                )
                .execute()
            )
            self.logger.info(f"Updated cell {range_name} with value: {value}")
            return result
        except HttpError as err:
            self.logger.error(f"Error updating cell {range_name}: {err}")
            raise

    def create_sheet(
        self, spreadsheet_id: str, title: str
    ) -> "BatchUpdateSpreadsheetResponse":
        """
        Add a new sheet to an existing spreadsheet

        Args:
            spreadsheet_id (str): The ID of the spreadsheet
            title (str): The title of the new sheet

        Returns:
            BatchUpdateSpreadsheetResponse: The response from the API
        """
        if "https://www.googleapis.com/auth/spreadsheets" not in self.scopes:
            self.logger.error(
                "Sheet creation requires read-write scope. Please initialize with READ_WRITE_SCOPES"
            )
            raise PermissionError(
                "Sheet creation requires read-write scope. Please initialize with READ_WRITE_SCOPES"
            )

        try:
            # Create a BatchUpdateSpreadsheetRequest object
            body: BatchUpdateSpreadsheetRequest = {
                "requests": [{"addSheet": {"properties": {"title": title}}}]
            }
            result = self.sheet.batchUpdate(
                spreadsheetId=spreadsheet_id, body=body
            ).execute()
            self.logger.info(f"Created sheet: {title}")
            return result
        except HttpError as err:
            self.logger.error(f"Error creating sheet: {err}")
            raise

    def get_sheet_properties(self, spreadsheet_id: str) -> "Spreadsheet":
        """
        Get properties of all sheets in a spreadsheet

        Args:
            spreadsheet_id (str): The ID of the spreadsheet

        Returns:
            Spreadsheet: The properties of the spreadsheet
        """
        try:
            result = self.sheet.get(spreadsheetId=spreadsheet_id).execute()
            return result
        except HttpError as err:
            self.logger.error(f"Error getting sheet properties: {err}")
            raise