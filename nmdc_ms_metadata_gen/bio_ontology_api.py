# -*- coding: utf-8 -*-
import requests


class BioOntologyInfoRetriever:
    """
    Client for retrieving ENVO term information from BioPortal API.

    A class to handle authentication and retrieval of Environmental Ontology (ENVO)
    terms using the BioPortal REST API service.

    Parameters
    ----------
    bio_api_key : str
        The BioPortal BioOntology API key for authentication.

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

    def __init__(self, bio_api_key: str):
        self.BIO_API_KEY = bio_api_key

    def get_envo_terms(self, envo_id: dict) -> dict:
        """
        Look up an ENVO term label using BioPortal API.

        Parameters
        ----------
        envo_id : dict
            The ENVO identifier to look up (e.g., 'ENVO:00002042')

        Returns
        -------
        dict
            Dictionary with envo_id as key and term label as value
            Example: {'ENVO:00002042': 'surface water'}

        Notes
        -----
        Makes an authenticated request to BioPortal API to retrieve the
        preferred label (prefLabel) for the given ENVO term.

        """

        url = f"http://data.bioontology.org/ontologies/ENVO/classes/{envo_id}"
        headers = {"Authorization": f"apikey token={self.BIO_API_KEY}"}

        response = requests.get(url, headers=headers)
        response.raise_for_status()

        data = response.json()
        return {envo_id: data["prefLabel"]}
