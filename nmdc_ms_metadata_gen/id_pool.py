import os
from collections import defaultdict

from dotenv import load_dotenv
from nmdc_api_utilities.minter import Minter

load_dotenv()
ENV = os.getenv("NMDC_ENV", "prod")


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
