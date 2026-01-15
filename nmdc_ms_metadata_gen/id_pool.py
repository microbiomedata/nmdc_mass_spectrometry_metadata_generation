import os
import random
import string
from collections import defaultdict

from dotenv import load_dotenv
from nmdc_api_utilities.minter import Minter

from nmdc_ms_metadata_gen.data_classes import NmdcTypes

load_dotenv()
ENV = os.getenv("NMDC_ENV", "prod")

id_prefixes = {
    NmdcTypes.Biosample: "bsm",
    NmdcTypes.MassSpectrometry: "dgms",
    NmdcTypes.MetabolomicsAnalysis: "wfmb",
    NmdcTypes.DataObject: "dobj",
    NmdcTypes.CalibrationInformation: "calib",
    NmdcTypes.NomAnalysis: "wfnom",
    NmdcTypes.MassSpectrometryConfiguration: "mscon",
    NmdcTypes.Instrument: "inst",
    NmdcTypes.Manifest: "manif",
    NmdcTypes.ChemicalConversionProcess: "chcpr",
    NmdcTypes.ChromatographyConfiguration: "chrcon",
    NmdcTypes.Pooling: "poolp",
    NmdcTypes.SubSamplingProcess: "subspr",
    NmdcTypes.Extraction: "extrp",
    NmdcTypes.ProcessedSample: "procsm",
    NmdcTypes.DissolvingProcess: "dispro",
    NmdcTypes.FiltrationProcess: "filtpr",
    NmdcTypes.ChromatographicSeparationProcess: "cspro",
    NmdcTypes.MixingProcess: "mixpro",
}


class IDPool:
    """
    Manages a pool of pre-generated NMDC IDs for efficient ID allocation.
    """

    def __init__(
        self, pool_size: int = 100, refill_threshold: int = 10, test: bool = False
    ):
        self.pool_size = pool_size
        self.refill_threshold = refill_threshold
        self.pools = defaultdict(list)
        self.test = test

    def get_id(self, nmdc_type: str, client_id: str, client_secret: str) -> str:
        """
        Get an ID from the pool, refilling if necessary.

        Parameters
        ----------
        nmdc_type : str
            The type of NMDC entity to get an ID for.
        client_id : str
            The client ID for the NMDC API.
        client_secret : str
            The client secret for the NMDC API.

        Returns
        -------
        str
            A single NMDC ID.
        """
        # Check if we need to refill the pool
        if len(self.pools[nmdc_type]) <= self.refill_threshold:
            self._refill_pool(nmdc_type, client_id, client_secret)

        # Ensure the pool is not empty before popping
        if not self.pools[nmdc_type]:
            raise RuntimeError(
                f"ID pool for type '{nmdc_type}' is empty after refill attempt."
            )

        # Return an ID from the pool
        return self.pools[nmdc_type].pop()

    def _refill_pool(
        self, nmdc_type: str, client_id: str, client_secret: str, retries: int = 3
    ) -> None:
        """
        Refill the pool for a specific NMDC type.

        Parameters
        ----------
        nmdc_type : str
            The type of NMDC entity to refill the pool for.
        client_id : str
            The client ID for the NMDC API.
        client_secret : str
            The client secret for the NMDC API.
        retries : int
            Number of retry attempts in case of failure.

        Returns
        -------
        None
        """
        if self.test:
            # In test mode, generate dummy IDs
            try:
                dummy_ids = [
                    f"nmdc:{id_prefixes[nmdc_type]}-13-{''.join(random.choices(string.ascii_lowercase + string.digits, k=random.randint(8, 8)))}"
                    for _ in range(self.pool_size - len(self.pools[nmdc_type]))
                ]
            except KeyError:
                raise ValueError(
                    f"NMDC type {nmdc_type} not found in id_prefixes mapping."
                )
            self.pools[nmdc_type].extend(dummy_ids)
        else:
            attempt = 0
            while attempt < retries:
                try:
                    minter = Minter(env=ENV)
                    new_ids = minter.mint(
                        nmdc_type=nmdc_type,
                        count=self.pool_size,
                        client_id=client_id,
                        client_secret=client_secret,
                    )
                    self.pools[nmdc_type].extend(new_ids)
                    return
                except Exception as e:
                    attempt += 1
                    if attempt >= retries:
                        raise RuntimeError(
                            f"Failed to refill ID pool for type '{nmdc_type}' after {retries} attempts: {e}"
                        )
