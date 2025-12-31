from os import truncate
from time import sleep

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import pyspark.sql.functions as F
import pyspark.sql.types as T
import matplotlib.ticker as ticker


from pyspark.sql.window import Window
from pyspark.sql.functions import col, lit, udf, count
from pyspark.sql.types import StructType, StructField

from os.path import basename, dirname, isfile
from urllib.parse import urlparse, urlunparse
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType

from dataclasses import dataclass, field
from typing import Optional

import re
import matplotlib
import gc
import os
import tempfile
import colorsys
from functools import reduce

_spark = None  # module-level private variable

def get_spark():
    global _spark
    if _spark is None:
        _spark = create_spark()
    return _spark

def create_spark():
    return SparkSession.builder \
        .appName("ProfilingJob") \
        .config("spark.driver.memory", "80g") \
        .config("spark.driver.maxResultSize", "4g") \
        .config("spark.sql.shuffle.partitions", "200") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.network.timeout", "300s") \
        .config("spark.sql.codegen.wholeStage", "true") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
        .getOrCreate()

def refresh_spark():
    global _spark
    if _spark is not None:
        _spark.stop()
    _spark = create_spark()
    return _spark

@udf(returnType=StringType())
def ancestor_udf(path, step=1):
    if step <= 0:
        return path

    parsed = urlparse(path)
    path_part = parsed.path
    for _ in range(step):
        path_part = dirname(path_part)
    return urlunparse((parsed.scheme, parsed.netloc, path_part, '', '', ''))

filtered_columns= ["prefix", "config_name", "wl:hot", "wl:mh", "wl:mp", "clients"]

IGNORE_INDEX_CACHE = False
IGNORE_RESULT_CACHE = False

def set_plot_style():
    plt.rc('axes', labelsize=15, titlesize=15)
    plt.rc('xtick', labelsize=14)
    plt.rc('ytick', labelsize=14)
    plt.rc('legend', fontsize=15)
    matplotlib.rcParams['pdf.fonttype'] = 42
    matplotlib.rcParams['ps.fonttype'] = 42

def set_pandas_config():
    pd.set_option('display.max_rows', 100)
    pd.set_option('display.max_rows', 20)
    pd.set_option('display.max_colwidth', None)

@dataclass
class GraphConfigs:
    offset: int
    duration: int
    IGNORE_INDEX_CACHE: bool
    IGNORE_RESULT_CACHE: bool
    spark: SparkSession
    output_path: str

@dataclass
class ColorSchemes:
    profile_color: list
    color: str

@dataclass
class Experiment:
    config: str
    label: str
    epoch_delay: int
    colors: ColorSchemes
    partial_exec: bool
    modules: dict
    collapsing_columns: dict
    mask: Optional[dict] = None
    cutoff: Optional[dict] = None


def apply_mask(df, mask):
    df_mask = pd.Series(True, index=df.index)
    for key, value in mask.items():
        if isinstance(value, list):
            if value[0] == "<":
                df_mask &= df[key] <= value[1]
            elif value[0] == ">":
                df_mask &= df[key] >= value[1]
            else:
                df_mask &= df[key] == value[1]
        else:
            df_mask &= df[key] == value

    return df[df_mask]

    
def deadlocks_dfs(spark, prefixes, start_time = 0):

    deadlocks_df_list = []
    for p in prefixes:
        deadlocks_df = deadlocks_csv(spark, p)
        deadlocks_df = deadlocks_df.withColumn("prefix", lit(p))
        if start_time:
            deadlocks_df.withColumn("time", F.greatest(F.col("time") - F.lit(start_time), F.lit(0)))
        deadlocks_df_list.append(deadlocks_df)

    return reduce(lambda df1, df2: df1.unionByName(df2), deadlocks_df_list)

def stub_goodput_dfs(spark, prefixes, start_time = 0):

    stub_goodput_df_list = []
    for p in prefixes:
        stub_goodput_df = stub_goodput_csv(spark, p)

        stub_goodput_df = stub_goodput_df.withColumn("prefix", lit(p))
        if start_time:
            stub_goodput_df = (stub_goodput_df
                               .withColumn("start_timestamp", F.greatest((F.col("start_timestamp") - F.lit(start_time))/1000000000, F.lit(0)))
                               .withColumn("end_timestamp", F.greatest((F.col("end_timestamp") - F.lit(start_time))/1000000000, F.lit(0))))

        stub_goodput_df_list.append(stub_goodput_df)

    return reduce(lambda df1, df2: df1.unionByName(df2), stub_goodput_df_list)

def stub_queue_dfs(spark, prefixes, start_time = 0):

    stub_queue_df_list = []
    for p in prefixes:
        stub_queue_df = stub_queue_csv(spark, p)

        if not stub_queue_df.head(1):
            continue

        stub_queue_filter_df = stub_queue_df.filter(col("start_timestamp") > start_time).withColumn("latency", F.col("end_timestamp") - F.col("start_timestamp"))

        schema = StructType([
            StructField("avg_latency", T.LongType(), True)
        ])

        avg_queue = stub_queue_filter_df.select(F.avg(F.col("latency"))).first()[0]
        data = [(round(avg_queue), )]
        stub_queue_final_df = spark.createDataFrame(data, schema=schema).withColumn("prefix", lit(p))

        stub_queue_df_list.append(stub_queue_final_df)

    return reduce(lambda df1, df2: df1.unionByName(df2), stub_queue_df_list)


def hex_to_rgb(hex_color):
    hex_color = hex_color.lstrip('#')
    return tuple(int(hex_color[i:i + 2], 16) / 255.0 for i in (0, 2, 4))


def rgb_to_hex(rgb):
    return '#{:02X}{:02X}{:02X}'.format(*(int(round(c * 255)) for c in rgb))


def generate_n_colors(base_hex, n):
    base_rgb = hex_to_rgb(base_hex)
    base_hsv = colorsys.rgb_to_hsv(*base_rgb)

    colors = []
    for i in range(n):
        # Evenly spaced hues around the circle
        new_hue = (base_hsv[0] + i / n) % 1.0
        new_rgb = colorsys.hsv_to_rgb(new_hue, base_hsv[1], base_hsv[2])
        colors.append(rgb_to_hex(new_rgb))

    return colors


def collect_col(sdf, col_name):
    return list(map(lambda r : r[col_name], sdf.collect()))


# clean everything
def clean_sdfs(sdfs):
    for sdf in sdfs:
        sdf.unpersist()
        del sdf
    gc.collect()


basename_udf = udf(basename, T.StringType())

def get_single_exp_index(spark, prefix, drop_region=True):
    client_sdf = spark.read.csv(f"{prefix}/client/*/metadata.csv", header=True) \
        .withColumn(
        "prefix",
        ancestor_udf(F.input_file_name(), lit(3))
    ) \
        .dropDuplicates()

    if drop_region:
        client_sdf = client_sdf.drop('region').dropDuplicates()

    server_sdf = spark.read.csv(f"{prefix}/server/*/metadata.csv", header=True) \
        .withColumn(
        "prefix",
        ancestor_udf(F.input_file_name(), lit(3))
    ) \
        .dropDuplicates()

    return server_sdf.join(client_sdf, on='prefix') \
        .withColumn("duration", col("duration").cast(T.IntegerType())) \
        .withColumn("txns", col("txns").cast(T.IntegerType())) \
        .withColumn("clients", col("clients").cast(T.IntegerType())) \
        .withColumn("rate", col("rate").cast(T.IntegerType()))

# Todo - update this
def get_index(spark, prefix, drop_region=True):
    client_sdf = spark.read.csv(f"{prefix}/*/client/*/metadata.csv", header=True)\
        .withColumn(
            "prefix",
            ancestor_udf(F.input_file_name(), lit(3))
        )\
        .dropDuplicates()

    if drop_region:
        client_sdf = client_sdf.drop('region').dropDuplicates()

    server_sdf = spark.read.csv(f"{prefix}/*/server/*/metadata.csv", header=True)\
        .withColumn(
            "prefix",
            ancestor_udf(F.input_file_name(), lit(3))
        )\
        .dropDuplicates()

    return server_sdf.join(client_sdf, on='prefix')\
        .withColumn("duration", col("duration").cast(T.IntegerType()))\
        .withColumn("txns", col("txns").cast(T.IntegerType()))\
        .withColumn("clients", col("clients").cast(T.IntegerType()))\
        .withColumn("rate", col("rate").cast(T.IntegerType()))

def get_num_regions(prefix):
    server_prefix = f"{prefix}/server"
    dirs = [d for d in os.listdir(server_prefix) if os.path.isdir(os.path.join(server_prefix, d))]

    regions = {d.split('-')[0] for d in dirs}  # left number
    partitions = {d.split('-')[1] for d in dirs}  # right number

    return len(regions), len(partitions)


def is_hex_color(s):
    return bool(re.fullmatch(r'#([A-Fa-f0-9]{6}|[A-Fa-f0-9]{3})', s))

def check_color_scheme(color_scheme):
    profile_color_check = True
    for color in color_scheme.profile_color:
        profile_color_check = profile_color_check and is_hex_color(color)

    return profile_color_check and is_hex_color(color_scheme.color)

def check_experiments(experiments):
    for prefix in experiments:
        assert len(experiments[prefix]) == 4, "Experiment must be of the type [Config, Label, EpochDelay, profile_function]"
        assert isinstance(experiments[prefix][0], str), "Config should be a string"
        assert isinstance(experiments[prefix][1], str), "Label should be a string"
        assert isinstance(experiments[prefix][2], int), "Epoch delay should be an integer"
        assert experiments[prefix][2] >= 0, "Epoch delay should be >= 0"
        assert callable(experiments[prefix][3]), "Profile Function should be a function"
        assert isinstance(experiments[prefix][4], ColorSchemes) and check_color_scheme(experiments[prefix][4]), "Color must be an hex string"

#----------------------- client/*/transactions.csv -----------------------

def transactions_csv(spark, prefix, start_offset_sec=0, duration_sec=1000000000):
    '''Reads client/*/transactions.csv files into a Spark dataframe'''

    transactions_schema = StructType([
        StructField("txn_id", T.LongType(), False),
        StructField("coordinator", T.IntegerType(), False),
        StructField("regions", T.StringType(), False),
        StructField("partitions", T.StringType(), False),
        StructField("generator", T.LongType(), False),
        StructField("restarts", T.IntegerType(), False),
        StructField("global_log_pos", T.StringType(), False),
        StructField("sent_at", T.DoubleType(), False),
        StructField("received_at", T.DoubleType(), False),
        StructField("last_machine", T.IntegerType(), False),
        StructField("orig_region", T.IntegerType(), False),
        StructField("last_remote_read_machine", T.IntegerType(), False),
        StructField("is_rw", T.BooleanType(), False),
        StructField("is_2fi", T.BooleanType(), False),
    ])


    pref = None
    if isinstance(prefix, str):
        pref = f"{prefix}/client/*/transactions.csv"
    else:
        pref = [f"{p}/client/*/transactions.csv" for p in prefix]

    sdf = spark.read.csv(
        pref,
        header=True,
        schema=transactions_schema
    )


    min_time = sdf.agg(F.min("sent_at")).first()[0]


    filtered = sdf.where(
        (F.col("sent_at") >= min_time + start_offset_sec * 1e9) &
        (F.col("sent_at") <= min_time + (start_offset_sec + duration_sec) * 1e9)
    )

    filtered = (
        filtered
        .withColumn(
            "regions",
            F.array_sort(F.split("regions", ";").cast(T.ArrayType(T.IntegerType())))
        )
        .withColumn(
            "partitions",
            F.array_sort(F.split("partitions", ";").cast(T.ArrayType(T.IntegerType())))
        )
        .withColumn(
            "machine",
            basename_udf(ancestor_udf(F.input_file_name())).cast(T.IntegerType())
        )
        .withColumn(
            "global_log_pos",
            F.split("global_log_pos", ";").cast(T.ArrayType(T.IntegerType()))
        )
    )

    return filtered

def deadlock_resolver_csv(spark, prefix):
    deadlock_resolver_schema = StructType([
        StructField("time", T.DoubleType(), False),
        StructField("partition", T.IntegerType(), False),
        StructField("region", T.IntegerType(), False),
        StructField("runtime", T.DoubleType(), False),
        StructField("unstable_graph_sz", T.DoubleType(), False),
        StructField("stable_graph_sz", T.DoubleType(), False),
        StructField("deadlocks_resolved", T.DoubleType(), False),
        StructField("graph_update_time", T.DoubleType(), False)
    ])

    sdf = spark.read.csv(
        f"{prefix}/server/*/deadlock_resolver.csv",
        header=True,
        schema=deadlock_resolver_schema,
    )

    return sdf
def ordering_overhead_csv(spark, prefix, start_offset_sec=0, duration_sec=1000000000):
    '''Reads client/*/transactions.csv files into a Spark dataframe'''
    ordering_overhead_schema = StructType([
        StructField("latency", T.DoubleType(), False),
    ])

    sdf = spark.read.csv(
        f"{prefix}/server/*/ordering_overheads.csv",
        header=True,
        schema=ordering_overhead_schema,
    )

    return sdf

def summary_csv(spark, prefix):
    summary_schema = StructType([
        StructField("committed", T.LongType(), False),
        StructField("aborted", T.LongType(), False),
        StructField("not_started", T.LongType(), False),
        StructField("restarted", T.LongType(), False),
        StructField("single_home", T.LongType(), False),
        StructField("multi_home", T.LongType(), False),
        StructField("single_partition", T.LongType(), False),
        StructField("multi_partition", T.LongType(), False),
        StructField("remaster", T.LongType(), False),
        StructField("rw", T.LongType(), False),
        StructField("2fi", T.LongType(), False),
        StructField("elapsed_time", T.LongType(), False),
        StructField("start_time", T.DoubleType(), False),
    ])

    return spark.read.csv(
        f"{prefix}/client/*/summary.csv",
        header=True,
        schema=summary_schema
    )\
    .withColumn(
        "machine",
        basename_udf(ancestor_udf(F.input_file_name())).cast(T.IntegerType())
    )


def deadlocks_csv(spark, prefix, new_schema=False):
    region_col_name = "region" if new_schema else "replica"

    deadlocks_schema = StructType([
        StructField("time", T.LongType(), False),
        StructField("partition", T.IntegerType(), False),
        StructField(region_col_name, T.IntegerType(), False),
        StructField("vertices", T.IntegerType(), False),
    ])

    return spark.read.csv(
        f"{prefix}/server/*/deadlocks.csv",
        header=True,
        schema=deadlocks_schema
    )


def txn_timestamps_csv(spark, prefix):
    txn_timestamp_schema = StructType([
        StructField("txn_id", T.LongType(), False),
        StructField("from", T.IntegerType(), False),
        StructField("txn_timestamp", T.LongType(), False),
        StructField("server_time", T.LongType(), False)
    ])
    split_file_name = F.split(basename_udf(ancestor_udf(F.input_file_name())), '-')
    return spark.read.csv(
        f"{prefix}/server/*/txn_timestamps.csv",
        header=True,
        schema=txn_timestamp_schema
    )\
    .withColumn("dev", (col("txn_timestamp") - col("server_time")) / 1000000)\
    .withColumn("region", split_file_name[0].cast(T.IntegerType()))\
    .withColumn("partition", split_file_name[1].cast(T.IntegerType()))


def generic_csv(spark, prefix):
    generic_schema = StructType([
        StructField("type", T.IntegerType(), False),
        StructField("time", T.LongType(), False),
        StructField("data", T.LongType(), False),
        StructField("partition", T.IntegerType(), False),
        StructField("region", T.IntegerType(), False),
    ])

    return spark.read.csv(
        f"{prefix}/server/*/generic.csv",
        header=True,
        schema=generic_schema
    )

def stub_goodput_csv(spark, prefix):
    stub_goodput_schema = StructType([
        StructField("start_timestamp", T.LongType(), False),
        StructField("end_timestamp", T.LongType(), False),
        StructField("goodput", T.LongType(), False),
        StructField("badput", T.LongType(), False),
        StructField("region_slot_null", T.LongType(), False),
        StructField("superposition_slot_null", T.LongType(), False),
        StructField("region_slot_used", T.LongType(), False),
        StructField("superposition_slot_used", T.LongType(), False),
    ])

    return spark.read.csv(
        f"{prefix}/server/*/stub_goodput.csv",
        header=True,
        schema=stub_goodput_schema
    )

def stub_queue_csv(spark, prefix):
    stub_queue_schema = StructType([
        StructField("txn_id", T.LongType(), False),
        StructField("start_timestamp", T.LongType(), False),
        StructField("end_timestamp", T.LongType(), False),
    ])

    return spark.read.csv(
        f"{prefix}/server/*/stub_queue.csv",
        header=True,
        schema=stub_queue_schema
    )
def committed(spark, prefix):
    return summary_csv(spark, prefix).select("committed").groupby().sum().collect()[0][0]


def sample_rate(spark, prefix):
    sampled = transactions_csv(spark, prefix).count()
    txns = committed(spark, prefix)
    return sampled / txns * 100

    
def throughput(spark, prefix, per_region=False, **kwargs):
    '''Computes throughput from client/*/transactions.csv file'''
    sample = sample_rate(spark, prefix)
    throughput_sdf = transactions_csv(spark, prefix, **kwargs)\
        .select("regions", col("sent_at").alias("time"))\
        .groupBy("regions")\
        .agg(
            (
                F.count("time") * 1000000000 * (100/sample) / (F.max("time") - F.min("time"))
            ).alias("throughput")
        )
    print(throughput_sdf.show(truncate=False))
    if per_region:
        return throughput_sdf
    else:
        return throughput_sdf.select(F.sum("throughput").alias("throughput"))


def throughput2(spark, prefix):
    summary = summary_csv(spark, prefix)\
        .select((col("committed") / col("elapsed_time") * 1000000000).alias("throughput"))
    return summary.select(F.sum("throughput").alias("throughput"))


def latency(spark, prefixes, sample=1.0, **kwargs):
    '''Compute latency from client/*/transactions.csv file'''

    return (
        transactions_csv(spark, prefixes, **kwargs).select(
            "txn_id",
            "coordinator",
            "regions",
            "partitions",
            "orig_region",
            ((col("received_at") - col("sent_at")) / 1000000).alias("latency"),
            (F.size(col("regions")) > 1).alias("is_mh"),
            "is_rw",
            "is_2fi",
            F.input_file_name().alias("prefix")
        ).sample(sample, seed=0)
    )

    """

    latency_sdf = None

    for p in prefixes:
        df = load_one(p)
        latency_sdf = df if latency_sdf is None else latency_sdf.unionByName(df)

    return latency_sdf
    latency_sdfs = []
    for p in prefixes:
        lat_sdf = transactions_csv(spark, p, **kwargs).select(
            "txn_id",
            "coordinator",
            "regions",
            "partitions",
            "orig_region",
            ((col("received_at") - col("sent_at")) / 1000000).alias("latency"),
            (F.size(col("regions")) > 1).alias("is_mh"),
            "is_rw",
            "is_2fi"
        )\
        .withColumn("prefix", lit(p))\
        .sample(sample, seed=0)
        latency_sdfs.append(lat_sdf)
    assert len(latency_sdfs) > 0, "No latency data found"

    latency_sdf = latency_sdfs[0]
    for l in latency_sdfs[1:]:
        latency_sdf = latency_sdf.union(l)

    return latency_sdf
    """

def latency_evolution(spark, prefixes, sample=1.0, **kwargs):
    '''Compute latency from client/*/transactions.csv file'''
    latency_sdfs = []
    for p in prefixes:
        lat_sdf = transactions_csv(spark, p, **kwargs).select(
            "txn_id",
            "coordinator",
            "regions",
            "partitions",
            ((col("received_at") - col("sent_at")) / 1000000).alias("latency"),
            "sent_at",
            "orig_region"
        )\
        .withColumn("prefix", lit(p))\
        .sample(sample, seed=0)
        latency_sdfs.append(lat_sdf)
    assert len(latency_sdfs) > 0, "No latency data found"

    latency_sdf = latency_sdfs[0]
    for l in latency_sdfs[1:]:
        latency_sdf = latency_sdf.union(l)

    return latency_sdf

def latency_get_df(spark, prefix, config, start_offset, duration_sec, ignore_cache_index=False, ignore_cache_result=False):
    index_df = from_cache_or_compute(
        f'{prefix}/index_{config}.parquet',
        lambda: get_index(spark, prefix).toPandas().convert_dtypes().astype({
            "wl:hot": "int32",
            "wl:mh": "int32",
            "wl:mp": "int32"
        }),
        ignore_cache=ignore_cache_index,
    )
    index_df
    latency_df_result = from_cache_or_compute(
        f'{prefix}/latency_cdf_{config}.parquet',
        lambda: latency(
            spark,
            index_df["prefix"],
            sample=1.0,
            start_offset_sec=start_offset,
            duration_sec=duration_sec
        ).toPandas() \
            .merge(index_df, on="prefix"),
        ignore_cache=ignore_cache_result,
    )
    latency_df_result

    return latency_df_result

def latency_cdf_df(latency_sdf):
    latency_sdf_sorted = latency_sdf.sort_values("latency")

    latency_sdf_sorted['CDF'] = latency_sdf_sorted['latency'].rank(method='average', pct=True)

    return latency_sdf_sorted
def txn_full_profiling(spark, prefixes, **kwargs):
    final_sdfs = []
    labels = ["ENTER_SERVER",
              "EXIT_SERVER_TO_FORWARDER",
              "ENTER_FORWARDER",
              "EXIT_FORWARDER_TO_SEQUENCER",
              "ENTER_SEQUENCER",
              "ENTER_LOCAL_BATCH",
              "ENTER_LOG_MANAGER_IN_BATCH",
              "ENTER_LOG_MANAGER_ORDER",
              "EXIT_SEQUENCER_IN_BATCH",
              "EXIT_LOG_MANAGER",
              "ENTER_SCHEDULER",
              "ENTER_LOCK_MANAGER",
              "ENTER_WORKER",
              "EXIT_WORKER",
              "EXIT_SERVER_TO_CLIENT"
              ]
    for p in prefixes:
        final_sdf = events_csv(spark, p)\
            .where(col("event") == labels[0])\
            .select("txn_id", col("time").alias(labels[0]))

        for label in labels[1:]:
            tmp_sdf = events_csv(spark, p) \
                .where(col("event") == label) \
                .groupBy("txn_id") \
                .agg(F.mean("time").alias(label))

            #if final_sdf.head(1).isEmpty:
            #    final_sdf = tmp_sdf
            #elif not tmp_sdf.head(1).isEmpty:
            final_sdf = final_sdf.join(tmp_sdf, on=["txn_id"])

        req_sdf = transactions_csv(spark, p, **kwargs)\
            .select("txn_id", "sent_at", "received_at")

        final_sdf = final_sdf.join(req_sdf, on=["txn_id"])
        calculations_sdf = final_sdf.select(
            "txn_id",
            ((col("ENTER_SERVER") - col("sent_at")) / 1000000).alias("CLIENT_SENT_COMM"),
            ((col("EXIT_SERVER_TO_FORWARDER") - col("ENTER_SERVER")) / 1000000).alias("SERVER"),
            ((col("ENTER_FORWARDER") - col("EXIT_SERVER_TO_FORWARDER")) / 1000000).alias("SERVER_COMM"),
            ((col("EXIT_FORWARDER_TO_SEQUENCER") - col("ENTER_FORWARDER")) / 1000000).alias("FORWARDER"),
            ((col("ENTER_SEQUENCER") - col("EXIT_FORWARDER_TO_SEQUENCER")) / 1000000).alias("FORWARDER_COMM"),
            ((col("ENTER_LOCAL_BATCH") - col("ENTER_SEQUENCER")) / 1000000).alias("SEQUENCER"),
            ((col("EXIT_SEQUENCER_IN_BATCH") - col("ENTER_LOCAL_BATCH")) / 1000000).alias("BATCHER"),
            ((col("ENTER_LOG_MANAGER_IN_BATCH") - col("EXIT_SEQUENCER_IN_BATCH")) / 1000000).alias("LOG_MANAGER_BATCH"),
            ((F.greatest("ENTER_LOG_MANAGER_IN_BATCH", "ENTER_LOG_MANAGER_ORDER") - col("ENTER_LOG_MANAGER_IN_BATCH")) / 1000000).alias("PAXOS"),
            ((col("EXIT_LOG_MANAGER") - F.greatest("ENTER_LOG_MANAGER_IN_BATCH", "ENTER_LOG_MANAGER_ORDER")) / 1000000).alias("PROCESSING_BATCH"),
            ((col("ENTER_SCHEDULER") - col("EXIT_LOG_MANAGER")) / 1000000).alias("LOG_MANAGER_COMM"),
            ((col("ENTER_LOCK_MANAGER") - col("ENTER_SCHEDULER")) / 1000000).alias("SCHEDULER"),
            ((col("ENTER_WORKER") - col("ENTER_LOCK_MANAGER")) / 1000000).alias("LOCK_MANAGER"),
            ((col("EXIT_WORKER") - col("ENTER_WORKER")) / 1000000).alias("WORKER"),
            ((col("EXIT_SERVER_TO_CLIENT") - col("EXIT_WORKER")) / 1000000).alias("EXIT_WORKER_SENT"),
            ((col("received_at") - col("EXIT_SERVER_TO_CLIENT")) / 1000000).alias("CLIENT_RECV_COMM"),
            ((col("received_at") - col("sent_at")) / 1000000).alias("LATENCY")
        )

        percentile_cols = []
        tags = ["CLIENT_SENT_COMM",
                "SERVER",
                "SERVER_COMM",
                "FORWARDER",
                "FORWARDER_COMM",
                "SEQUENCER",
                "BATCHER",
                "LOG_MANAGER_BATCH",
                "PAXOS",
                "PROCESSING_BATCH",
                "LOG_MANAGER_COMM",
                "SCHEDULER",
                "LOCK_MANAGER",
                "WORKER",
                "EXIT_WORKER_SENT",
                "CLIENT_RECV_COMM",
                "LATENCY"]

        for tag in tags:
            percentile_cols.append(F.percentile_approx(tag, 0.5).alias(tag))

        test2 = calculations_sdf\
            .agg(*percentile_cols)\
            .withColumn("mp", lit(100))

        test2 = test2.drop(col("LATENCY"))

        attempt_plot = test2.toPandas()
        ax = attempt_plot.plot.bar(
            x="mp",
            stacked=True
        )
        ax.set_ylabel("latency (ms)")
        ax.set_xlabel("% multi-partition")
        ax.yaxis.set_major_locator(ticker.AutoLocator())
        ax.yaxis.set_minor_locator(ticker.AutoMinorLocator())
        ax.legend(bbox_to_anchor=(1, 0), loc='center left')
        fig = ax.get_figure()

        fig.savefig('output/profile.pdf', bbox_inches='tight')
        fig.savefig('output/profile.jpg', bbox_inches='tight')


        final_sdfs.append(test2)
    profile_aggregated_sdf = final_sdfs[0]
    for l in final_sdfs[1:]:
        profile_aggregated_sdf = profile_aggregated_sdf.union(l)

def txn_profiling(spark, prefixes, **kwargs):
    final_sdfs = []
    labels = ["ENTER_SERVER",
              "EXIT_SERVER_TO_FORWARDER",
              "ENTER_FORWARDER",
              "EXIT_FORWARDER_TO_SEQUENCER",
              "ENTER_SEQUENCER",
              "ENTER_LOCAL_BATCH",
              "ENTER_LOG_MANAGER_IN_BATCH",
              "ENTER_LOG_MANAGER_ORDER",
              "EXIT_SEQUENCER_IN_BATCH",
              "EXIT_LOG_MANAGER",
              "ENTER_SCHEDULER",
              "ENTER_LOCK_MANAGER",
              "ENTER_WORKER",
              "EXIT_WORKER",
              "EXIT_SERVER_TO_CLIENT"
              ]

    for p in prefixes:
        final_sdf = events_csv(spark, p) \
            .where(col("event") == labels[0]) \
            .select("txn_id", col("time").alias(labels[0]))

        for label in labels[1:]:
            tmp_sdf = events_csv(spark, p) \
                .where(col("event") == label) \
                .groupBy("txn_id") \
                .agg(F.mean("time").alias(label))
            #if final_sdf.head(1).isEmpty:
            #    final_sdf = tmp_sdf
            #elif not tmp_sdf.head(1).isEmpty:
            final_sdf = final_sdf.join(tmp_sdf, on=["txn_id"])


        req_sdf = transactions_csv(spark, p, **kwargs) \
            .select("txn_id", "sent_at", "received_at")

        final_sdf = final_sdf.join(req_sdf, on=["txn_id"])
        calculations_sdf = final_sdf.select(
            "txn_id",
            ((col("ENTER_FORWARDER") - col("sent_at")) / 1000000).alias("CLIENT_SENT_COMM"),
            ((col("ENTER_SEQUENCER") - col("ENTER_FORWARDER")) / 1000000).alias("FORWARDER"),
            ((col("EXIT_SEQUENCER_IN_BATCH") - col("ENTER_SEQUENCER")) / 1000000).alias("SEQUENCER/BATCHER"),
            ((col("ENTER_LOG_MANAGER_IN_BATCH") - col("EXIT_SEQUENCER_IN_BATCH")) / 1000000).alias("LOG_MANAGER_BATCH"),
            ((F.greatest("ENTER_LOG_MANAGER_IN_BATCH", "ENTER_LOG_MANAGER_ORDER") - col("ENTER_LOG_MANAGER_IN_BATCH")) / 1000000).alias("PAXOS"),
            ((col("ENTER_SCHEDULER") - F.greatest("ENTER_LOG_MANAGER_IN_BATCH", "ENTER_LOG_MANAGER_ORDER")) / 1000000).alias("PROCESSING_BATCH"),
            ((col("ENTER_WORKER") - col("ENTER_SCHEDULER")) / 1000000).alias("SCHEDULER"),
            ((col("EXIT_WORKER") - col("ENTER_WORKER")) / 1000000).alias("WORKER"),
            ((col("EXIT_SERVER_TO_CLIENT") - col("EXIT_WORKER")) / 1000000).alias("EXIT_WORKER_SENT"),
            ((col("received_at") - col("EXIT_SERVER_TO_CLIENT")) / 1000000).alias("CLIENT_RECV_COMM"),
            ((col("received_at") - col("sent_at")) / 1000000).alias("LATENCY")
        )


        percentile_cols = []
        tags = ["CLIENT_SENT_COMM",
                "FORWARDER",
                "SEQUENCER/BATCHER",
                "LOG_MANAGER_BATCH",
                "PAXOS",
                "PROCESSING_BATCH",
                "SCHEDULER",
                "WORKER",
                "EXIT_WORKER_SENT",
                "CLIENT_RECV_COMM",
                "LATENCY"]

        for tag in tags:
            percentile_cols.append(F.percentile_approx(tag, 0.5).alias(tag))

        test2 = calculations_sdf \
            .agg(*percentile_cols) \
            .withColumn("prefix", lit(p)) \
            .drop(col("LATENCY"))
        final_sdfs.append(test2)


        #attempt_plot = test2.toPandas()
        #ax = attempt_plot.plot.bar(
        #    x="mp",
        #    stacked=True
        #)
        #ax.set_ylabel("latency (ms)")
        #ax.set_xlabel("% multi-partition")
        #ax.yaxis.set_major_locator(ticker.AutoLocator())
        #ax.yaxis.set_minor_locator(ticker.AutoMinorLocator())
        #ax.legend(bbox_to_anchor=(1, 0.5), loc='center left')
        #fig = ax.get_figure()

        #fig.savefig('output/profile.pdf', bbox_inches='tight')
        #fig.savefig('output/profile.jpg', bbox_inches='tight')

    profile_aggregated_sdf = final_sdfs[0]
    for l in final_sdfs[1:]:
        profile_aggregated_sdf = profile_aggregated_sdf.union(l)
    return profile_aggregated_sdf


def ordering_overhead(spark, prefixes, sample=1.0, **kwargs):
    '''Compute latency from client/*/transactions.csv file'''
    latency_sdfs = []
    for p in prefixes:
        lat_sdf = ordering_overhead_csv(spark, p, **kwargs).select(
            (col("latency") / 1000000).alias("latency")
        ) \
            .withColumn("prefix", lit(p)) \
            .sample(sample, seed=0)

        latency_sdfs.append(lat_sdf)

    assert len(latency_sdfs) > 0, "No latency data found"

    latency_sdf = latency_sdfs[0]
    for l in latency_sdfs[1:]:
        latency_sdf = latency_sdf.union(l)

    return latency_sdf

def all_events(spark, prefix):

    enter_server_sdf = events_csv(spark,prefix)\
        .where(col("event") == "ENTER_SERVER")\
        .select("txn_id", "time".alias("server_enter"))

#----------------------- client/*/txn_events.csv -----------------------

def txn_events_csv(spark, prefix, start_offset_sec=0, duration_sec=1000000000):
    '''Compute latency from client/*/transactions.csv file'''

    txn_events_schema = StructType([
        StructField("txn_id", T.LongType(), False),
        StructField("event", T.StringType(), False),
        StructField("time", T.DoubleType(), False),
        StructField("machine", T.IntegerType(), False),
        StructField("home", T.IntegerType(), False),
    ])

    sdf = (spark.read.csv(
        f"{prefix}/client/*/txn_events.csv",
        header=True,
        schema=txn_events_schema
    ))

    """
    min_time = sdf.agg(F.min("time")).collect()[0][0]
    sdf = sdf[sdf["time"] >= lit(min_time + start_offset_sec * 1000000000)]
    sdf = sdf[sdf["time"] <= lit(min_time + (start_offset_sec + duration_sec) * 1000000000)]

    sdf = sdf.filter((col("time") >= lit(min_time + start_offset_sec * 1000000000)) &
                     (col("time") <= lit(min_time + (start_offset_sec + duration_sec) * 1000000000)))
    """

    return sdf


def events_latency(spark, prefix, from_event, to_event, new_col_name=None):
    '''Computes latency between a pair of events'''

    from_sdf = txn_events_csv(spark, prefix)\
        .where(col("event") == from_event)\
        .groupBy("txn_id", "machine")\
        .agg(F.max("time").alias("from_time"))
    to_sdf = txn_events_csv(spark, prefix)\
        .where(col("event") == to_event)\
        .groupBy("txn_id", "machine")\
        .agg(F.max("time").alias("to_time"))
    diff_col_name = new_col_name if new_col_name else f"{from_event}_to_{to_event}"
    return from_sdf.join(to_sdf, on=["txn_id", "machine"])\
        .select(
            "txn_id",
            "machine",
            ((col("to_time") - col("from_time")) / 1000000).alias(diff_col_name)
        )


#----------------------- server/*/events.csv -----------------------

def events_csv(spark, prefix):
    '''Reads server/*/events.csv files into a Spark dataframe'''

    events_schema = StructType([
        StructField("txn_id", T.LongType(), False),
        StructField("event", T.StringType(), False),
        StructField("time", T.DoubleType(), False),
        StructField("partition", T.IntegerType(), False),
        StructField("region", T.IntegerType(), False),
    ])

    return spark.read.csv(
        f"{prefix}/server/*/events.csv",
        header=True,
        schema=events_schema
    )


def events_throughput(spark, prefix, sample, per_machine=False):
    '''Counts number of events per time unit'''

    group_by_cols = ["event", "time"]
    if per_machine:
        group_by_cols += ["partition", "region"]

    return events_csv(spark, prefix).withColumn(
        "time",
        (col("time") / 1000000000).cast(T.LongType())
    )\
    .groupBy(*group_by_cols)\
    .agg((F.count("txn_id")  * (100 / sample)).alias("throughput"))\
    .sort("time")


#----------------------- server/*/forw_sequ_latency.csv -----------------------

def fs_latency_csv(spark, prefix):
    '''Reads server/*/forw_sequ_latency.csv files into a Spark dataframe'''

    fs_latency_schema = StructType([
        StructField("dst", T.IntegerType(), False),
        StructField("send_time", T.LongType(), False),
        StructField("recv_time", T.LongType(), False),
        StructField("avg_latency", T.LongType(), False),
    ])

    return spark.read.csv(
        f"{prefix}/server/*/forw_sequ_latency.csv",
        header=True,
        schema=fs_latency_schema
    )\
    .withColumn("src", basename_udf(ancestor_udf(F.input_file_name())))


#----------------------- helper functions -----------------------

def remove_constant_columns(df, ignore=None):
    for c in df.columns:
        if ignore is not None and c in ignore:
            continue
        if len(df[c].unique()) == 1:
            df.drop(c, inplace=True, axis=1)
            
            
def normalize(col):
    min_val = np.nanmin(col.values)
    return col.values - min_val

            
def normalize2(col):
    min_val = np.nanmin(col.values)
    max_val = np.nanmax(col.values)
    return (col.values - min_val) / (max_val - min_val)


def compute_rows_cols(num_axes, num_cols=3):
    num_rows = num_axes // num_cols + (num_axes % num_cols > 0)
    return num_rows, num_cols


def from_cache_or_compute(cache_path, func, ignore_cache=False):
    if not ignore_cache and isfile(cache_path):
        return pd.read_parquet(cache_path)
    res = func()
    res.to_parquet(cache_path)        
    print(f"Saved to: {cache_path}")
    return res

def from_cache_or_compute_pyspark(cache_path, func, ignore_cache=False):

    if not ignore_cache and isfile(cache_path):
        return spark.read.parquet(cache_path)
    res = func()
    res.write.mode('overwrite').parquet(cache_path)
    print(f"Saved to: {cache_path}")
    return res

def cached_or_compute(spark, path, compute_fn):
    """
    Check if path exists, if so load the DataFrame. Otherwise, compute it and save.

    Args:
        spark: SparkSession
        path (str): HDFS/local path to check/write
        compute_fn (function): a function that returns a DataFrame when called

    Returns:
        DataFrame
    """
    if os.path.exists(path):  # for local FS
        print(f"[cache] Loading from {path}")
        return spark.read.parquet(path)
    else:
        print(f"[cache] Computing and writing to {path}")
        df = compute_fn()
        df.write.mode("overwrite").parquet(path)
        return df

#----------------------- plot functions -----------------------

def plot_cdf(a, ax=None, scale="log", **kargs):
    x = np.sort(a)
    y = np.arange(1, len(x)+1) / len(x)
    kargs.setdefault("markersize", 2)
    if ax is not None:
        ax.plot(x, y, **kargs)
        ax.set_xscale(scale)
    else:
        plt.plot(x, y, **kargs)
        plt.xscale(scale)


def plot_event_throughput(dfs, group_by_machine=True, sharey=True, sharex=True, **kargs):
    events = [
        'ENTER_SERVER',
        'EXIT_SERVER_TO_FORWARDER',
        '',
        'ENTER_FORWARDER',
        'EXIT_FORWARDER_TO_SEQUENCER',
        'EXIT_FORWARDER_TO_MULTI_HOME_ORDERER',
        'ENTER_MULTI_HOME_ORDERER',
        'ENTER_MULTI_HOME_ORDERER_IN_BATCH',
        'EXIT_MULTI_HOME_ORDERER',
        'ENTER_SEQUENCER',
        'EXIT_SEQUENCER_IN_BATCH',
        '',
        'ENTER_LOG_MANAGER_IN_BATCH',
        'EXIT_LOG_MANAGER',
        '',
        'ENTER_SCHEDULER',
        'ENTER_SCHEDULER_LO',
        'ENTER_LOCK_MANAGER',
        'DISPATCHED_FAST',
        'DISPATCHED_SLOW',
        'DISPATCHED_SLOW_DEADLOCKED',
        'ENTER_WORKER',
        'EXIT_WORKER',
        '',
        'RETURN_TO_SERVER',
        'EXIT_SERVER_TO_CLIENT'
        '',
    ]
    num_rows, num_cols = compute_rows_cols(len(events), num_cols=3)
    fig, axes = plt.subplots(num_rows, num_cols, sharey=sharey, sharex=sharex, figsize=(17, 25))
    
    for df in dfs.values():
        df.loc[:, 'time'] = normalize(df.loc[:, 'time'])
    
    for i, event in enumerate(events):
        r, c = i // num_cols, i % num_cols
        for label, df in dfs.items():
            if "partition" in df.columns:
                if group_by_machine:
                    partitions = df.partition.unique()
                    for p in partitions:
                        df[(df.event == event) & (df.partition == p)].plot(
                            x="time",
                            y="throughput",
                            title=event.replace('_', ' '),
                            marker='.',
                            label=f"{label}_{p}",
                            ax=axes[r, c]
                        )
                else: 
                    df[(df.event == event) & (df.partition == 0)].plot(
                        x="time",
                        y="throughput",
                        title=event.replace('_', ' '),
                        marker='.',
                        label=f"{label}_0",
                        ax=axes[r, c]
                    )
            else:
                df[df.event == event].plot(
                    x="time",
                    y="throughput",
                    title=event.replace('_', ' '),
                    marker='.',
                    label=label,
                    ax=axes[r, c]
                )
        axes[r, c].grid(axis='y')
        axes[r, c].set_xlabel('time')
        axes[r, c].set_ylabel('txn/sec')


    row_labels = [
        'SERVER',
        'FORWARDER',
        'MH_ORDERER',
        'SEQUENCER',
        'LOG_MANAGER',
        'SCHEDULER-1',
        'SCHEDULER-2',
        'WORKER',
        'SERVER',
    ]
    for ax, row in zip(axes[:,0], row_labels):
        ax.annotate(row, xy=(-50, 110), xytext=(0, 0),
                    xycoords="axes points", textcoords='offset points',
                    size='x-large', ha='right')

    return fig, axes

#----------------------- client/*/client_epochs.csv -----------------------

def client_epochs_csv(spark, prefix, start_offset_sec=0, duration_sec=1000000000, region="*"):
    '''Reads client/*/client_epochs.csv files into a Spark dataframe'''

    events_schema = StructType([
        StructField("stub_id", T.DecimalType(20, 0), False),
        StructField("time", T.DoubleType(), False),
        StructField("inserted_requests", T.IntegerType(), False),
        StructField("sent_requests", T.IntegerType(), False),
        StructField("sent_noops", T.IntegerType(), False),
        StructField("sent_txns", T.IntegerType(), False),
        StructField("slots_requested", T.LongType(), False),
        StructField("current_slots", T.LongType(), False),
        StructField("sh_stub", T.IntegerType(), False)
    ])

    sdf = spark.read.csv(
        f"{prefix}/client/{region}/client_epochs.csv",
        header=True,
        schema=events_schema
    )


    min_time = sdf.agg(F.min("time")).collect()[0][0]
    sdf = sdf[sdf["time"] >= lit(min_time + start_offset_sec * 1000000000)]
    sdf = sdf[sdf["time"] <= lit(min_time + (start_offset_sec + duration_sec) * 1000000000)]

    sdf = sdf.filter((col("time") >= lit(min_time + start_offset_sec * 1000000000)) &
                     (col("time") <= lit(min_time + (start_offset_sec + duration_sec) * 1000000000)))
    return sdf

#----------------------- server/*/log_insertions.csv -----------------------
def log_profiling_csv(spark, prefix):
    '''Reads server/*/log_insertion.csv files into a Spark dataframe'''

    log_profiling_csv = StructType([
        StructField("machine_id", T.IntegerType(), False),
        StructField("batch_id", T.LongType(), False),
        StructField("txn_id", T.LongType(), False),
        StructField("gsn", T.StringType(), False),
        StructField("gsn_at_insertion", T.LongType(), False),
        StructField("insertion", T.DoubleType(), False),
        StructField("wait", T.DoubleType(), False),
        StructField("processing", T.DoubleType(), False)
    ])

    sdf = spark.read.csv(
        f"{prefix}/server/*/log_profiling.csv",
        header=True,
        schema=log_profiling_csv
    )

    return sdf

def active_redirect_csv(spark, prefix):
    '''Reads server/*/active_redirection.csv files into a Spark dataframe'''

    active_redirect_csv = StructType([
        StructField("worker_id", T.LongType(), False),
        StructField("timestamp", T.LongType(), False),
        StructField("active_redirects", T.IntegerType(), False)
    ])

    sdf = spark.read.csv(
        f"{prefix}/server/*/active_redirect.csv",
        header=True,
        schema=active_redirect_csv
    )

    return sdf.withColumnRenamed("timestamp", "time")


def single_active_redirect(spark, prefix, start_offset_sec=0, duration_sec=1000000000):
    redirects_sdf = active_redirect_csv(spark, prefix) \
        .withColumn("prefix", lit(prefix))

    min_time_sdf = redirects_sdf.select(F.min(col("time")).alias("min_time")).first()
    min_time = min_time_sdf["min_time"]

    redirects_sdf = redirects_sdf.withColumn("time", redirects_sdf.time - min_time)


    redirects_sdf = redirects_sdf.where(
        (col("time") >= start_offset_sec * 1000000000) &
        (col("time") <= (start_offset_sec + duration_sec) * 1000000000)
    )
    return redirects_sdf

def active_redirects_quartils(spark, prefixes, **kwargs):

    quantiles = [0.0, 0.25, 0.5, 0.75, 0.9, 0.99, 1.0]

    rows = []
    for p in prefixes:
        redirects_sdf = single_active_redirect(spark, p, **kwargs)

        quantile_vals = redirects_sdf.approxQuantile("active_redirects", quantiles, 0.01)
        rows.append({"min": quantile_vals[0], "q1": quantile_vals[1],
                     "median": quantile_vals[2], "q3": quantile_vals[3], "p90": quantile_vals[4],
                     "p99": quantile_vals[5], "max": quantile_vals[6], "prefix": p})

    if not rows:
        raise Exception("No active redirects found")

    return spark.createDataFrame(rows)
