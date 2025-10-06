import pandas as pd


class ChangeSheetGenerator:
    """
    A class to assist in prgrammatically creating change sheets. More documentation can be found here https://docs.microbiomedata.org/runtime/howto-guides/author-changesheets/
    """

    @staticmethod
    def initialize_empty_df() -> pd.DataFrame:
        """
        Create an empty DataFrame with required columns.
        Required columns are: id, action, attribute, value

        Parameters
        ----------
        None

        Returns
        -------
        pd.DataFrame
            The initialized empty DataFrame
        """
        df = pd.DataFrame(columns=["id", "action", "attribute", "value"])
        return df

    @staticmethod
    def add_row(
        df: pd.DataFrame, id: str, action: str, attribute: str, value: str
    ) -> pd.DataFrame:
        """
        Add a new row to the DataFrame.

        Parameters
        ----------
        df : pd.DataFrame
            The DataFrame to add the row to
        id : str
            The identifier
        action : str
            The action being performed
        attribute : str
            The attribute being modified
        value : str
            The value to set

        Returns
        -------
        pd.DataFrame
            The updated DataFrame
        """
        new_row = {"id": id, "action": action, "attribute": attribute, "value": value}

        # Changed self.df to df since we're using static method
        return pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)


class WorkflowSheetGenerator:
    """
    The workflow sheet provides the last processed sample id and the raw file name it will be associated with.
    This provides a reference for creating the associated data generation records.

    """

    @staticmethod
    def initialize_empty_df() -> pd.DataFrame:
        """
        Create an empty DataFrame with required columns

        Parameters
        ----------
        None

        Returns
        -------
        pd.DataFrame
            The initialized empty DataFrame
        """
        df = pd.DataFrame(
            columns=["biosample_id", "raw_data_identifier", "last_processed_sample"]
        )
        return df

    @staticmethod
    def add_row(
        df: pd.DataFrame,
        biosample_id: str,
        raw_data_identifier: str,
        last_processed_sample: str,
    ) -> pd.DataFrame:
        """
        Add a new row to the DataFrame.

        Parameters
        ----------
        df : pd.DataFrame
            The DataFrame to add the row to
        biosample_id : str
            The biosample identifier
        raw_data_identifier : str
            The raw file identifier
        last_processed_sample : str
            The last processed sample made during material processing, to be used as input to data generation record

        Returns
        -------
        pd.DataFrame
            The updated DataFrame
        """
        new_row = {
            "biosample_id": biosample_id,
            "raw_data_identifier": raw_data_identifier,
            "last_processed_sample": last_processed_sample,
        }

        # Changed self.df to df since we're using static method
        return pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)
