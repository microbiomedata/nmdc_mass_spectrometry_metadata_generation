# -*- coding: utf-8 -*-
import requests
import json
import logging
from dotenv import load_dotenv
import os
from pathlib import Path
import oauthlib
import requests_oauthlib
import requests

load_dotenv()

BIO_API_KEY = os.getenv("BIO_API_KEY")
from dotenv import load_dotenv

load_dotenv()
import os

# set the cwd to /src/
if "src" not in Path.cwd().name:
    os.chdir(Path.cwd() / "src")
CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")


class NMDCAPIInterface:
    """
    A generic interface for the NMDC runtime API.

    Attributes
    ----------
    base_url : str
        The base URL for the NMDC runtime API.

    Methods
    -------
    validate_json(json_path: str) -> None:
        Validates a json file using the NMDC json validate endpoint.
    """

    def __init__(self):
        self.base_url = "https://api.microbiomedata.org"

    def validate_json(self, json_path) -> None:
        """
        Validates a json file using the NMDC json validate endpoint.

        If the validation passes, the method returns without any side effects.

        Parameters
        ----------
        json_path : str
            The path to the json file to be validated.

        Raises
        ------
        Exception
            If the validation fails.
        """
        with open(json_path, "r") as f:
            data = json.load(f)

        # Check that the term "placeholder" is not present anywhere in the json
        if "placeholder" in json.dumps(data):
            raise Exception("Placeholder values found in json!")

        url = f"{self.base_url}/metadata/json:validate"
        headers = {"accept": "application/json", "Content-Type": "application/json"}
        response = requests.post(url, headers=headers, json=data)
        if response.text != '{"result":"All Okay!"}' or response.status_code != 200:
            logging.error(f"Request failed with response {response.text}")
            raise Exception("Validation failed")

    def mint_nmdc_id(self, nmdc_type: str) -> list[str]:
        """
        Mint new NMDC IDs of the specified type using the NMDC ID minting API.

        Parameters
        ----------
        nmdc_type : str
            The type of NMDC ID to mint (e.g., 'nmdc:MassSpectrometry',
            'nmdc:DataObject').

        Returns
        -------
        list[str]
            A list containing one newly minted NMDC ID.

        Raises
        ------
        requests.exceptions.RequestException
            If there is an error during the API request.

        Notes
        -----
        This method relies on a YAML configuration file for authentication
        details. The file should contain 'client_id' and 'client_secret' keys.

        """
        client = oauthlib.oauth2.BackendApplicationClient(client_id=CLIENT_ID)
        oauth = requests_oauthlib.OAuth2Session(client=client)

        token = oauth.fetch_token(
            token_url=f"{self.base_url}/token",
            client_id=CLIENT_ID,
            client_secret=CLIENT_SECRET,
        )

        nmdc_mint_url = f"{self.base_url}/pids/mint"

        payload = {"schema_class": {"id": nmdc_type}, "how_many": 1}

        response = oauth.post(nmdc_mint_url, data=json.dumps(payload))
        list_ids = response.json()

        return list_ids


class ApiInfoRetriever(NMDCAPIInterface):
    """
    A class to retrieve API information from a specified collection.

    This class provides functionality to query an API and retrieve information
    from a specified collection based on a name field value.

    Attributes
    ----------
    collection_name : str
        The name of the collection from which to retrieve information.

    Methods
    -------
    get_id_by_name_from_collection(name_field_value: str) -> str:
        Retrieves the ID of an entry from the collection based on the given name field value.
    """

    def __init__(self, collection_name: str):
        """
        Initialize the ApiInfoRetriever with the specified collection name.

        Parameters
        ----------
        collection_name : str
            The name of the collection to be used for API queries.
        """
        super().__init__()
        self.collection_name = collection_name

    def get_id_by_name_from_collection(self, name_field_value: str) -> str:
        """
        Retrieve the ID of an entry from the collection using the name field value.

        This method constructs a query to the API to filter the collection based on the
        given name field value, retrieves the response, and extracts the ID of the first
        entry in the response.

        Parameters
        ----------
        name_field_value : str
            The value of the name field to filter the collection.

        Returns
        -------
        str
            The ID of the entry retrieved from the collection.

        Raises
        ------
        IndexError
            If no matching entry is found in the collection.
        requests.RequestException
            If there's an error in making the API request.
        """
        # Trim trailing white spaces
        name_field_value = name_field_value.strip()

        filter_param = f'{{"name": "{name_field_value}"}}'
        field = "id"

        og_url = f"{self.base_url}/nmdcschema/{self.collection_name}?&filter={filter_param}&projection={field}"

        try:
            resp = requests.get(og_url)
            resp.raise_for_status()  # Raises an HTTPError for bad responses
            data = resp.json()
            identifier = data["resources"][0]["id"]
            return identifier
        except requests.RequestException as e:
            raise requests.RequestException(f"Error making API request: {e}")
        except (KeyError, IndexError) as e:
            raise IndexError(f"No matching entry found for '{name_field_value}': {e}")

    def check_if_ids_exist(self, ids: list) -> bool:
        """
        Check if the IDs exist in the collection.

        This method constructs a query to the API to filter the collection based on the given IDs, and checks if all IDs exist in the collection.

        Parameters
        ----------
        ids : list
            A list of IDs to check if they exist in the collection.

        Returns
        -------
        bool
            True if all IDs exist in the collection, False otherwise.

        Raises
        ------
        requests.RequestException
            If there's an error in making the API request.
        """
        ids_test = list(set(ids))
        for id in ids_test:
            filter_param = f'{{"id": "{id}"}}'
            field = "id"

            og_url = f"{self.base_url}/nmdcschema/{self.collection_name}?&filter={filter_param}&projection={field}"

            try:
                resp = requests.get(og_url)
                resp.raise_for_status()  # Raises an HTTPError for bad responses
                data = resp.json()
                if len(data["resources"]) == 0:
                    print(f"ID {id} not found")
                    return False
            except requests.RequestException as e:
                raise requests.RequestException(f"Error making API request: {e}")

        return True

    def get_id_by_slot_from_collection(self, slot_name: str, slot_field_value: str):
        """
        Retrieve the NMDC identifier from a specified collection based on a slot name and field value.

        Parameters
        ----------
        slot_name : str
            The name of the slot to filter by.
        slot_field_value : str
            The value of the slot field to filter for. Trailing whitespace will be removed.

        Returns
        -------
        str
            The identifier corresponding to the specified slot name and field value.

        Raises
        ------
        ValueError
            If the request to the API fails or if no resources are found for the given slot name and value.
        """
        # trim trailing white spaces
        slot_field_value = slot_field_value.rstrip()

        filter = f'{{"{slot_name}": "{slot_field_value}"}}'
        field = "id"

        og_url = f"https://api.microbiomedata.org/nmdcschema/{self.collection_name}?&filter={filter}&projection={field}"
        resp = requests.get(og_url)

        # Check if the response status is 200
        if resp.status_code != 200:
            raise ValueError(
                f"Failed to retrieve data from {self.collection_name}, response code: {resp.status_code}"
            )

        data = resp.json()

        # Ensure there is at least one resource in the response
        if not data["resources"]:
            raise ValueError(
                f"No resources in Mongo found for '{slot_name}' slot in {self.collection_name} with value {slot_field_value}"
            )

        identifier = data["resources"][0]["id"]

        return identifier


class BioOntologyInfoRetriever:
    """
    Client for retrieving ENVO term information from BioPortal API.

    A class to handle authentication and retrieval of Environmental Ontology (ENVO)
    terms using the BioPortal REST API service.

    Parameters
    ----------

    Notes
    -----
    The configuration file should contain an 'api_key' field with a valid
    BioPortal API key.

    Examples
    --------
    >>> retriever = BioOntologyInfoRetriever('config.yaml')
    >>> envo_terms = retriever.get_envo_terms('ENVO:00002042')
    >>> print(envo_terms)
    {'ENVO:00002042': 'surface water'}
    """

    def __init__(self):
        pass

    def get_envo_terms(self, envo_id: dict):
        """
        Look up an ENVO term label using BioPortal API.

        Parameters
        ----------
        envo_id : str
            The ENVO identifier to look up (e.g., 'ENVO:00002042')

        Returns
        -------
        dict
            Dictionary with envo_id as key and term label as value
            Example: {'ENVO:00002042': 'surface water'}

        Raises
        ------
        requests.exceptions.RequestException
            If the API request fails
        KeyError
            If the response doesn't contain expected data format
        yaml.YAMLError
            If the config file cannot be parsed
        FileNotFoundError
            If the config file is not found

        Notes
        -----
        Makes an authenticated request to BioPortal API to retrieve the
        preferred label (prefLabel) for the given ENVO term.
        """

        url = f"http://data.bioontology.org/ontologies/ENVO/classes/{envo_id}"
        headers = {"Authorization": f"apikey token={BIO_API_KEY}"}

        response = requests.get(url, headers=headers)
        response.raise_for_status()

        data = response.json()
        return {envo_id: data["prefLabel"]}
