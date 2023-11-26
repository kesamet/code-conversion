"""
Spark context manager
"""
from contextlib import contextmanager

from pyspark import HiveContext, SparkConf, SparkContext

spark_context = None
sql_context = None


@contextmanager
def get_spark_context(**kwargs):
    """
    Get the spark context for submitting pyspark applications
    """
    # There can only be one spark context
    # So this function should not be called twice anyway
    global spark_context  # pylint: disable=global-statement

    if spark_context is None:
        try:
            spark_context = SparkContext(conf=get_spark_conf(**kwargs))
            yield spark_context
        except:
            raise
        finally:
            if spark_context:
                spark_context.stop()
    else:
        try:
            yield spark_context
        finally:
            # Don't stop the spark context because it was created by
            # another party, and probably with a larger with-scope
            pass


def get_spark_conf(**kwargs):
    """
    Get the configuration to start up the spark cluster

    :param kwargs: list of keyword arguments to specify settings in conf
    :return: spark configuration
    """
    conf = (
        SparkConf()
        .setMaster(kwargs.get("master", "yarn-client"))
        .setAppName(kwargs.get("app_name", "anonymous"))
        .set("spark.executor.memory", kwargs.get("executor_memory", "8g"))
        .set("spark.driver.maxResultSize", kwargs.get("max_result_size", "8g"))
        .set("spark.executor.instances", kwargs.get("num_executors", "12"))
        .set("spark.executor.cores", kwargs.get("num_cores", "3"))
        .set("spark.memory.fraction", kwargs.get("memory_fraction", "0.5"))
        .set(
            "spark.yarn.executor.memoryOverhead",
            kwargs.get("executors_mem_overhead", 2560),
        )
    )
    return conf


@contextmanager
def get_spark_sql_context(**kwargs):
    global sql_context, spark_context  # pylint: disable=global-statement

    with get_spark_context(**kwargs) as spark_context:
        if sql_context is None:
            sql_context = HiveContext(spark_context)
        yield spark_context, sql_context
