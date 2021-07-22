"""
test_etl_job.py
~~~~~~~~~~~~~~~

This module contains unit tests for the transformation steps of the ETL
job defined in etl_job.py. It makes use of a local version of PySpark
that is bundled with the PySpark package.
"""
import unittest

from pyspark.sql.functions import col

from dependencies.spark import start_spark
from jobs.etl_job import transform_data


class SparkETLTests(unittest.TestCase):
    """Test suite for transformation in etl_job.py"""

    def setUp(self):
        """Start Spark, define config and path to test data"""
        self.spark, *_ = start_spark()
        self.test_data_path = "tests/test_data/"

    def tearDown(self):
        """Stop Spark"""
        self.spark.stop()

    def test_transform_data(self):
        """Test data transformer.

        Using small chunks of input data and expected output data, we
        test the transformation step to make sure it's working as
        expected.
        """
        # assemble
        input_data = (
            self.spark.read.format("csv")
            .options(header=True, inferSchema=True)
            .load(self.test_data_path + "input")
        )

        expected_data = (
            self.spark.read.format("csv")
            .options(header=True, inferSchema=True)
            .load(self.test_data_path + "expected_output")
        )

        expected_cols = len(expected_data.columns)

        expected_energy_consumption = (
            expected_data.filter(
                (col("meter") == "A") & (col("start_time") == "2019-06-01 14:00:00.001")
            )
            .select("energy_consumption")
            .collect()[0]
        )

        expected_mean_power = (
            expected_data.filter(
                (col("meter") == "A") & (col("start_time") == "2019-06-01 14:00:00.001")
            )
            .select("mean_power")
            .collect()[0]
        )

        expected_min_power = (
            expected_data.filter(
                (col("meter") == "A") & (col("start_time") == "2019-06-01 14:00:00.001")
            )
            .select("min_power")
            .collect()[0]
        )

        expected_max_power = (
            expected_data.filter(
                (col("meter") == "A") & (col("start_time") == "2019-06-01 14:00:00.001")
            )
            .select("max_power")
            .collect()[0]
        )

        # apply transformation
        data_transformed = transform_data(input_data)

        cols = len(data_transformed.columns)

        energy_consumption = (
            data_transformed.filter(
                (col("meter") == "A") & (col("start_time") == "2019-06-01 14:00:00.001")
            )
            .select("energy_consumption")
            .collect()[0]
        )

        mean_power = (
            data_transformed.filter(
                (col("meter") == "A") & (col("start_time") == "2019-06-01 14:00:00.001")
            )
            .select("mean_power")
            .collect()[0]
        )

        min_power = (
            data_transformed.filter(
                (col("meter") == "A") & (col("start_time") == "2019-06-01 14:00:00.001")
            )
            .select("min_power")
            .collect()[0]
        )

        max_power = (
            data_transformed.filter(
                (col("meter") == "A") & (col("start_time") == "2019-06-01 14:00:00.001")
            )
            .select("max_power")
            .collect()[0]
        )

        # assert
        self.assertEqual(expected_cols, cols)
        self.assertTrue(
            [col in expected_data.columns for col in data_transformed.columns]
        )
        self.assertEqual(expected_energy_consumption, energy_consumption)
        self.assertEqual(expected_mean_power, mean_power)
        self.assertEqual(expected_min_power, min_power)
        self.assertEqual(expected_max_power, max_power)


if __name__ == "__main__":
    unittest.main()
