import os
from random import random
from pyspark import SparkContext
from pyspark.sql import SparkSession
from time import perf_counter

PI_20: float = 3.14159265358979323846


def calc_pi(number_of_samples: int, sc: SparkContext) -> float:
    def inside(_: int) -> int:
        x, y = random(), random()
        return x * x + y * y < 1

    inside_count = sc.parallelize(range(0, number_of_samples)).filter(inside).count()

    return (4 * inside_count) / number_of_samples


def main(number_of_samples: int = 100000):
    spark = (
        SparkSession.builder.appName("Databricks Connect Test App").getOrCreate()
    )

    start = perf_counter()
    pi = calc_pi(number_of_samples, spark.sparkContext)
    end = perf_counter()
    elapsed_seconds = end - start

    print(
        f"Approximate value of PI ["
        f"{number_of_samples} samples, "
        f"{elapsed_seconds:.2f} sec]: {pi} "
        f"(percent error: {abs(100 * ((PI_20 - pi)/PI_20)):.2f}%)"
    )
