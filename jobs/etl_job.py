"""
etl_job.py
~~~~~~~~~~

This Python module can be submitted to a Spark cluster using the 'spark-submit'
command found in the '/bin' directory of all Spark distributions. For example:

    $SPARK_HOME/bin/spark-submit \
    --master local[*] \
    --py-files packages.zip \
    jobs/etl_job.py

where packages.zip contains Python modules required by ETL job and, etl_job.py
contains the Spark application to be executed by a driver process
on the Spark master node.

"""
from pyspark.sql.functions import col, min, max, first, last, mean, to_timestamp, window
from pyspark.sql.window import Window

from dependencies.spark import start_spark


def main():
    """Main ETL script definition.

    :return: None
    """
    # start Spark application and get Spark session and logger
    spark, log = start_spark(
        app_name="etl_eec", spark_config={"spark.sql.legacy.timeParserPolicy": "LEGACY"}
    )

    # log that main ETL job is starting
    log.warn("etl_job is up-and-running")

    # execute ETL pipeline
    data = extract_data(spark)
    data_transformed = transform_data(data)
    load_data(data_transformed)

    # log the success and terminate Spark application
    log.warn("test_etl_job is finished")
    spark.stop()
    return None


def extract_data(spark):
    """Load data from csv file format.

    :param spark: Spark session object.
    :return: Spark DataFrame.
    """
    df = (
        spark.read.format("csv")
        .options(header=True, inferSchema=True)
        .load("data/sample/*.csv")
    )

    return df


def transform_data(df):
    """Transform original dataset.

    :param df: Input DataFrame.
    :return: Transformed DataFrame.
    """
    cols = [
        "meter",
        "start_time",
        "end_time",
        "energy_consumption",
        "mean_power",
        "min_power",
        "max_power",
    ]
    w = (
        Window.partitionBy("meter")
        .orderBy(col("time").cast("long"))
        .rangeBetween(-60 * 60, 0)
    )

    df_transformed = (
        df.withColumn("time", to_timestamp("time", "yyyy-MM-dd HH:mm"))
        .withColumn("first", first(col("energy")).over(w))
        .withColumn("last", last(col("energy")).over(w))
        .groupBy("meter", window("time", "1 hour", startTime="1 millisecond"))
        .agg(
            min("power").alias("min_power"),
            max("power").alias("max_power"),
            mean("power").alias("mean_power"),
            first("first").alias("first"),
            last("last").alias("last"),
        )
        .withColumn("energy_consumption", col("last") - col("first"))
        .withColumn("start_time", col("window").start)
        .withColumn("end_time", col("window").end)
        .select(*cols)
    )

    return df_transformed


def load_data(df):
    """Collect data locally and write to CSV.

    :param df: DataFrame to print.
    :return: None
    """

    (df.coalesce(1).write.csv("ouput_data", mode="overwrite", header=True))

    return None


# entry point for PySpark ETL application
if __name__ == "__main__":
    main()
