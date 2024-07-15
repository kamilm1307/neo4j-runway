import csv
import io
import json
from typing import Any, Callable, Dict, List, Optional, Union

import pandas as pd


class DataFramePackage:
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

        self._generate_dataframe_summaries()

    @classmethod
    def from_file(
        cls,
        file_path: str,
        general_description: Union[str, None] = None,
        column_descriptions: Union[Dict[str, str], None] = None,
        name: str = "",
        read_file_config: Union[Dict[str, Any], None] = None,
    ) -> "DataFramePackage":
        """
        Construct a DataFramePackage object from a csv_path and provided descriptions.

        Parameters
        ----------
        file_path : str
            The full file path. Either .csv or .json file
        general_description : Union[str, None], optional
            A general description of the data in the file, by default None
        column_descriptions : Union[Dict[str, str], None], optional
            A dictionary with columns as keys and their associated descriptions as values.
            If this arg is passed, then only the columns identified will be loaded, by default None
        name : str, optional
            The file name. Inferred by the file_path arg, by default ""
        read_file_config : Union[Dict[str, Any], None], optional
            pd.read_*() method keyword arguments to pass when loading the file, by default None

        Returns
        -------
        Self
            The DataFramePackage object.
        """

        if not read_file_config: read_file_config = dict()

        file_type = _get_file_type(file_path)

        if column_descriptions:
            for k in column_descriptions.keys():
                if k not in _get_columns(
                    file_path,
                    (
                        read_file_config["sep"]
                        if read_file_config and "sep" in read_file_config
                        else ","
                    ),
                ):
                    raise ValueError(
                        f"column_descriptions key {k} is not in the DataFrame columns!"
                    )
            read_file_config["usecols"] = list(column_descriptions.keys())
        # else:
        #     read_file_config["usecols"] = None

        if "/" in file_path:
            pieces = file_path.split("/")
            if not name:
                name = pieces.pop()
            else:
                pieces.pop()
            file_loc = "/".join(pieces)

        else:
            file_loc = ""

        dataframe = _load_file(
            file_path=file_path,
            method=_get_pandas_load_method(file_type),
            read_file_config=read_file_config,
            # usecols=usecols,
        )

        return cls(
            file_loc=file_loc,
            name=name,
            general_description=general_description,
            column_descriptions=column_descriptions,
            dataframe=dataframe,
        )

    def _generate_dataframe_summaries(self) -> None:
        """
        Generate the data summaries.
        """
        buffer = io.StringIO()
        self.dataframe.info(buf=buffer)

        df_info = buffer.getvalue()
        desc_numeric = self.dataframe.describe(
            percentiles=[0.1, 0.25, 0.5, 0.75, 0.9, 0.95, 0.99]
        )
        desc_categorical = self.dataframe.describe(include="object")

        self.df_info = df_info
        self.numeric_data_description = desc_numeric
        self.categorical_data_description = desc_categorical


class DataFramePackageCollection:
    """
    A collection of DataFrameInput objects.
    """

    data: List[DataFramePackage]

    def __init__(self, data: Dict[str, pd.DataFrame]) -> None:
        self.data = data

    @classmethod
    def from_csv_list(
        cls, csv_location_list: List[str]
    ) -> "DataFramePackageCollection":
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


def _get_columns(file_path: str, sep: str = ",") -> List[str]:

    if file_path.endswith(".csv"):
        with open(file_path) as f:
            csv_reader = csv.reader(f, delimiter=sep)

            for row in csv_reader:
                return row[1:]
    elif file_path.endswith(".json"):
        with open(file_path) as f:
            json_reader: Dict[str, List[Any]] = json.load(f)
            return list(json_reader.keys())

    else:
        raise ValueError(f"Unable to parse file type from file path {file_path}")


def _get_file_type(file_path: str) -> str:
    if file_path.endswith(".csv"):
        return "csv"
    elif file_path.endswith(".json"):
        return "json"
    else:
        raise ValueError(f"Unable to parse file type from file path {file_path}")


def _load_file(
    file_path: str,
    method: Callable,
    read_file_config: Optional[Dict[str, Any]],
    # usecols: Optional[List[str]] = None,
) -> pd.DataFrame:

    assert method in [
        pd.read_csv,
        pd.read_json,
    ], f"Unsupported pandas read_* method provided: {method}"

    if not read_file_config:
        return method(file_path)
    # elif not read_file_config and usecols:
    #     return method(file_path, usecols=usecols)
    # elif usecols:
    #     return method(file_path, usecols=usecols, **read_file_config)
    else:
        if method == pd.read_json:
            # read_json doesn't have a usecols arg parameter
            cols = read_file_config.pop("usecols")
            return method(file_path, **read_file_config)[cols]
        else:
            return method(file_path, **read_file_config)


def _get_pandas_load_method(file_type: str) -> Callable:
    if file_type == "csv":
        return pd.read_csv
    elif file_type == "json":
        return pd.read_json
    else:
        raise ValueError(
            f"Unable to provide pandas read_* method for file type {file_type}"
        )
