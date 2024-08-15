import os
from typing import Dict, List, Optional

import pandas as pd

from ...discovery.discovery_content import DiscoveryContent


class Table:
    """
    A container for a Pandas DataFrame and its associated information.

    Attributes
    ----------
    name : str
        The name of the data file.
    file_path : str
        The full file path to the file.
    data : pd.DataFrame
        The data in Pandas DataFrame format.
    general_description : str
        A general description of the data.
    data_dictionary : Dict[str, str], optional
        A description of each column that is available for data modeling. Only columns identified here will be considered for inclusion in the data model.
    use_cases : Optional[List[str]], optional
        Any use cases that the graph data model should address.
    discovery_content : Optional[DiscoveryContent], optional
        Any insights gathered about the data. This is contained within the DiscoveryContent class.
    discovery : str
        The discovery generated by the Discovery module and contained in the `discovery` attribute of `discovery_content`.
    """

    name: str
    file_path: str
    data: pd.DataFrame
    general_description: str = ""
    data_dictionary: Dict[str, str] = dict()
    use_cases: Optional[List[str]] = None
    discovery_content: Optional[DiscoveryContent] = None

    def __init__(
        self,
        name: str,
        file_path: str,
        data: pd.DataFrame,
        general_description: str = "",
        data_dictionary: Dict[str, str] = dict(),
        use_cases: Optional[List[str]] = None,
        discovery_content: Optional[DiscoveryContent] = None,
    ) -> None:
        """
        A container for a Pandas DataFrame and its associated information.

        Parameters
        ----------
        name : str
            The name of the data file.
        file_path : str
            The full file path to the file.
        data : pd.DataFrame
            The data in Pandas DataFrame format.
        general_description : str
            A general description of the data, by default None
        data_dictionary : Dict[str, str], optional
            A description of each column that is available for data modeling, by default dict()
        use_cases : Optional[List[str]], optional
            Any use cases that the graph data model should address, by default None
        discovery_content : Optional[DiscoveryContent], optional
        Any insights gathered about the data. This is contained within the DiscoveryContent class. By default None
        """

        if not isinstance(data, pd.DataFrame):
            raise ValueError("table argument 'data' should be a Pandas DataFrame.")

        self.name = name
        self.file_path = file_path
        self.data = data
        self.general_description = general_description
        self.data_dictionary = data_dictionary
        self.use_cases = use_cases
        self.discovery_content = discovery_content

    @property
    def discovery(self) -> str:
        """
        The discovery generated by the Discovery module. Does not contain Pandas summaries.

        Returns
        -------
        str
            Discovery.
        """

        if self.discovery_content is None:
            return ""
        else:
            return self.discovery_content.discovery

    @discovery.setter
    def discovery(self, value: str) -> None:
        assert self.discovery_content is not None

        self.discovery_content.discovery = value
