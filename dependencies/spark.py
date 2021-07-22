"""
spark.py
~~~~~~~~

Module containing helper function for use with Apache Spark
"""

import __main__

from os import environ
from pyspark.sql import SparkSession

from dependencies import logging


def start_spark(app_name='my_spark_app', master='local[*]', spark_config={}):
    """Start Spark session, and Spark logger .

    Start a Spark session on the worker node and register the Spark
    application with the cluster.
    The function checks the enclosing environment to see if it is being
    run from inside an interactive console session or from an
    environment which has a `DEBUG` environment variable set. In this scenario,
    the function uses all available function arguments
    to start a PySpark driver from the local PySpark package as opposed
    to using the spark-submit and Spark cluster defaults. This will also
    use local module imports, as opposed to those in the zip archive
    sent to spark via the --py-files flag in spark-submit.

    :param app_name: Name of Spark app.
    :param master: Cluster connection details (defaults to local[*]).
    :param spark_config: Dictionary of config key-value pairs.
    :return: A tuple of references to the Spark session and logger
    """

    # detect execution environment
    flag_repl = not(hasattr(__main__, '__file__'))
    flag_debug = 'DEBUG' in environ.keys()

    if not (flag_repl or flag_debug):
        # get Spark session factory
        spark_builder = (
            SparkSession
            .builder
            .appName(app_name))

    else:
        # get Spark session factory
        spark_builder = (
            SparkSession
            .builder
            .master(master)
            .appName(app_name))

    # add other config params
    for key, val in spark_config.items():
        spark_builder.config(key, val)

    # create session and retrieve Spark logger object
    spark_sess = spark_builder.getOrCreate()
    spark_logger = logging.Log4j(spark_sess)

    return spark_sess, spark_logger
