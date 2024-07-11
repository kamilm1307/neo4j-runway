import unittest

import pandas as pd

from neo4j_runway.utils import DataFrameInput


class TestDataFrameInputObject(unittest.TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        pass

    def test_init_csv_path_only(self) -> None:
        truth = pd.read_csv("tests/resources/pets-arrows-2.csv")
        data = DataFrameInput.from_csv(csv_path="tests/resources/pets-arrows-2.csv")

        self.assertTrue(truth.equals(data.dataframe))
        self.assertEqual(data.name, "pets-arrows-2.csv")

    def test_init_csv_path_and_good_descriptions(self) -> None:
        truth = pd.read_csv(
            "tests/resources/pets-arrows-2.csv", usecols=["name", "pet_name"]
        )
        data = DataFrameInput.from_csv(
            csv_path="tests/resources/pets-arrows-2.csv",
            general_description="This is a test general.",
            column_descriptions={
                "name": "A person's name.",
                "pet_name": "A pet's name.",
            },
        )

        self.assertTrue(truth.equals(data.dataframe))
        self.assertEqual(data.name, "pets-arrows-2.csv")
        self.assertEqual({"name", "pet_name"}, set(data.column_descriptions.keys()))

    def test_init_csv_path_and_bad_descriptions(self) -> None:

        with self.assertRaises(ValueError):
            DataFrameInput.from_csv(
                csv_path="tests/resources/pets-arrows-2.csv",
                general_description="This is a test general.",
                column_descriptions={
                    "name": "A person's name.",
                    "wrong_column": "A pet's name.",
                },
            )

    def test_init_with_read_csv_config(self) -> None:
        truth = pd.read_csv(
            "tests/resources/pets-arrows-2-pipe.csv",
            sep="|",
            skipfooter=2,
            header=0,
            engine="python",
            usecols=["name", "pet_name"],
        )
        data = DataFrameInput.from_csv(
            csv_path="tests/resources/pets-arrows-2-pipe.csv",
            general_description="This is a test general.",
            column_descriptions={
                "name": "A person's name.",
                "pet_name": "A pet's name.",
            },
            read_csv_config={
                "sep": "|",
                "skipfooter": 2,
                "header": 0,
                "engine": "python",
            },
        )

        self.assertTrue(truth.equals(data.dataframe))
