import csv
from typing import Any, Dict, List, Self, Union

import pandas as pd


class DataFrameInput:
    """
    A container for a Pandas DataFrame and it's associated information.
    """

    def __init__(
        self,
        file_loc: str,
        name: str = "",
        dataframe: Union[pd.DataFrame, None] = None,
        general_description: Union[str, None] = None,
        column_descriptions: Union[Dict[str, str], None] = None,
    ) -> None:
        self.name = name
        self.file_loc = file_loc
        self.dataframe = dataframe
        self.general_description = general_description
        self.column_descriptions = column_descriptions

    @classmethod
    def from_csv(
        cls,
        csv_path: str,
        general_description: Union[str, None] = None,
        column_descriptions: Union[Dict[str, str], None] = None,
        name: str = "",
        read_csv_config: Union[Dict[str, Any], None] = None,
    ) -> Self:
        """
        Construct a DataFrameInput object from a csv_path and provided descriptions.

        Parameters
        ----------
        csv_path : str
            The full CSV path.
        general_description : Union[str, None], optional
            A general description of the data in the CSV, by default None
        column_descriptions : Union[Dict[str, str], None], optional
            A dictionary with columns as keys and their associated descriptions as values.
            If this arg is passed, then only the columns identified will be loaded, by default None
        name : str, optional
            The CSV name. Inferred by the csv_path arg, by default ""
        read_csv_config : Union[Dict[str, Any], None], optional
            pd.read_csv() method keyword arguments to pass when loading the CSV file, by default None

        Returns
        -------
        Self
            The DataFrameInput object.
        """

        if column_descriptions:
            for k in column_descriptions.keys():
                if k not in _get_csv_columns(
                    csv_path,
                    (
                        read_csv_config["sep"]
                        if read_csv_config and "sep" in read_csv_config
                        else ","
                    ),
                ):
                    raise ValueError(
                        f"column_descriptions key {k} is not in the DataFrame columns!"
                    )
            usecols = list(column_descriptions.keys())
        else:
            usecols = None

        if "/" in csv_path:
            pieces = csv_path.split("/")
            if not name:
                name = pieces.pop()
            else:
                pieces.pop()
            file_loc = "/".join(pieces)

        else:
            file_loc = ""

        if not read_csv_config and not usecols:
            dataframe = pd.read_csv(csv_path)
        elif not read_csv_config and usecols:
            dataframe = pd.read_csv(csv_path, usecols=usecols)
        elif usecols:
            dataframe = pd.read_csv(csv_path, usecols=usecols, **read_csv_config)
        else:
            dataframe = pd.read_csv(csv_path, **read_csv_config)

        return cls(
            file_loc=file_loc,
            name=name,
            general_description=general_description,
            column_descriptions=column_descriptions,
            dataframe=dataframe,
        )


class DataFramePackage:
    """
    A collection of DataFrameInput objects.
    """

    data: List[DataFrameInput]

    def __init__(self, data: Dict[str, pd.DataFrame]) -> None:
        self.data = data

    @classmethod
    def from_csv_list(cls, csv_location_list: List[str]) -> Self:
        """
        Construct a DataFrameInput object from a list of CSV file addresses.

        Parameters
        ----------
        csv_location_list : List[str]
            A list of CSV file locations.

        Returns
        -------
        Self
            A DataFramePackage object.
        """

        data = dict()
        for csv_loc in csv_location_list:
            assert csv_loc.endswith(
                ".csv"
            ), f"Invalid file in csv_location_list: {csv_loc}"

            data[csv_loc] = pd.read_csv(csv_loc)

        return cls(data=data)


def _get_csv_columns(file_path: str, sep: str = ",") -> List[str]:

    with open(file_path) as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=sep)

        for row in csv_reader:
            return row[1:]
