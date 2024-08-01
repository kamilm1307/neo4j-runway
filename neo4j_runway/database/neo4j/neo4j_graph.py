from typing import Any, Dict, List, Optional, Union
import warnings

from neo4j import GraphDatabase

from ..base import BaseGraph
from ...utils.read_env import read_environment
from ...exceptions import APOCNotInstalledError


class Neo4jGraph(BaseGraph):
    """
    Handler for Neo4j graph interactions.

    Attributes
    ----------
    apoc_version : Union[str, None]
        The APOC version present in the database.
    database : Union[str, None]
        The database name to run queries against in the Neo4j instance.
    database_edition : str
        The edition of the Neo4j instance.
    database_version : str
        The Neo4j version of the Neo4j instance.
    driver : GraphDatabase.Driver
        The driver used to communicate with Neo4j. Constructed from credentials provided to the constructor.
    schema : Union[Dict[str, Any], None]
        The database schema gathered from APOC.meta.schema
    """

    def __init__(
        self,
        username: Optional[str] = None,
        password: Optional[str] = None,
        uri: Optional[str] = None,
        database: Optional[str] = None,
        driver_config: Dict[str, Any] = dict(),
    ) -> None:
        """
        Constructor for the Neo4jGraph.

        Parameters
        ----------
        username : Optional[str], optional
            Neo4j username. If not provided, will check NEO4J_USERNAME env variable. By default None
        password : Optional[str], optional
            Neo4j password. If not provided, will check NEO4J_PASSWORD env variable. By default None
        uri : Optional[str], optional
            Neo4j uri. If not provided, will check NEO4J_URI env variable. By default None
        database : Optional[str], optional
            Neo4j database to connect to. If not provided, will check NEO4J_DATABASE env variable. By default None
        driver_config : Dict[str, Any], optional
            Any additional configuration to provide the driver, by default dict()
        """
        self.driver = GraphDatabase.driver(
            uri=uri or read_environment("NEO4J_URI"),
            auth=(
                username or read_environment("NEO4J_USERNAME"),
                password or read_environment("NEO4J_PASSWORD"),
            ),
            **driver_config,
        )
        self.database = database or read_environment("NEO4J_DATABASE") or "neo4j"
        self.apoc_version = self._get_apoc_version()
        version, edition = self._get_database_version()
        self.database_edition = edition
        self._schema = None

        super().__init__(driver=self.driver, version=version)

    @property
    def schema(self) -> Dict[str, Any]:
        if self._schema is None:
            self.refresh_schema()
        return self._schema

    @schema.setter
    def schema(self, new_schema: Dict[str, Any]) -> None:
        self._schema = new_schema

    def verify(self) -> Dict[str, Any]:
        """
        Verify connection and authentication.

        Returns
        -------
        Dict[str, Any]
            Whether connection is successful and any messages.
        """

        try:

            self.driver.verify_connectivity()
            self.driver.verify_authentication()
        except Exception as e:
            return {
                "valid": False,
                "message": f"""
                            Are your credentials correct?
                            Connection Error: {e}
                            """,
            }
        self.driver.close()
        return {"valid": True, "message": "Connection and Auth Verified!"}

    def _get_database_version(self) -> List[str]:
        """
        Retrieve the Neo4j version and edition of the database.

        Returns
        -------
        List[str]
            The Neo4j version and edition.
        """
        try:
            with self.driver.session(database=self.database) as session:
                response = (
                    session.run(
                        """CALL dbms.components()
    YIELD versions, edition
    RETURN versions[0] as version, edition"""
                    )
                    .single()
                    .values()
                )
            self.database_version, self.database_edition = response
        except Exception as e:
            self.driver.close()

        return response

    def _get_apoc_version(self) -> Union[str, None]:
        """
        Retrieve the APOC version running in the database.

        Returns
        -------
        str
            The APOC version or None if APOC not present on database.
        """
        try:
            with self.driver.session(database=self.database) as session:
                return session.run("RETURN apoc.version()").single().value()
        except Exception as e:
            warnings.warn(
                "APOC is not found in the database. Some features such as schema retrieval depend on APOC."
            )
            return None

    def refresh_schema(self) -> Dict[str, Any]:
        """
        Refresh the graph schema via APOC from the database.

        Raises
        ------
        APOCNotInstalledError
            If APOC is not installed on the Neo4j instance.

        Returns
        -------
        Dict[str, Any]
            The schema in APOC format, if APOC is present on database
        """
        try:
            with self.driver.session() as session:
                response = session.run(
                    """CALL apoc.meta.schema()
YIELD value
RETURN value as dataModel"""
                ).value()[0]

            self.schema = response
            return response
        except Exception as e:
            raise APOCNotInstalledError(
                "APOC must be installed to perform `refresh_schema` operation."
            )
