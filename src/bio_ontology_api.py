# -*- coding: utf-8 -*-
import requests
from dotenv import load_dotenv
import os
from pathlib import Path
import requests

load_dotenv()

BIO_API_KEY = os.getenv("BIO_API_KEY")
from dotenv import load_dotenv

load_dotenv()
import os

# set the cwd to /src/
if "src" not in Path.cwd().name:
    os.chdir(Path.cwd() / "src")


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
