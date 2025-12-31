from concurrent.futures.process import ProcessPoolExecutor
from time import sleep

from common import GraphConfigs
import common
import os
import pandas as pd
import numpy as np
from datetime import datetime
import shutil
import profiling
from filelock import FileLock
import pyarrow.parquet as pq
from glob import glob
import gc
import json
import multiprocessing as mp
from modules import *
import argparse

# Overall Flow


#   Input:
#       Input Directory: Directory where results are being inserted
#       Output Directory: Where to save the parquet files

# Main:

def ensure_dataframe(data):
    if isinstance(data, pd.DataFrame):
        return data
    elif isinstance(data, str) and os.path.exists(data):
        table = pq.read_table(data, memory_map=True)
        return table.to_pandas()
    else:
        raise TypeError("Input must be a pandas DataFrame or a valid file path.")

def append_rows_to_parquet(new_data: pd.DataFrame, path: str):
    lock_path = path + '.lock'
    with FileLock(lock_path):
        if os.path.exists(path):
            existing = pd.read_parquet(path)
            combined = pd.concat([existing, new_data], ignore_index=True)
        else:
            combined = new_data
        combined.to_parquet(path, index=False)

"""
Latency-related Statistics
"""

def latency_genuine_metric(latency_df, parquet_path, summary_parquet_path, prefix, percentile_cols):
    dataframe = ensure_dataframe(latency_df)
    if dataframe.empty:
        return

    now = datetime.now()

    mh_latency = dataframe[dataframe["is_mh"] == True]
    if mh_latency.empty:
        return

    DELAY = [
        [0.1, 6, 33, 38, 74, 87, 106, 99],
        [6, 0.1, 38, 43, 66, 80, 99, 94],
        [33, 38, 0.1, 6, 101, 114, 92, 127],
        [38, 43, 6, 0.1, 105, 118, 86, 132],
        [74, 66, 101, 105, 0.1, 16, 36, 64],
        [87, 80, 114, 118, 16, 0.1, 36, 74],
        [106, 99, 92, 86, 36, 36, 0.1, 46],
        [99, 94, 127, 132, 64, 74, 46, 0.1],
    ]


    mh_latency["region_filtered"] = mh_latency.apply(
        lambda row: next(r for r in row["regions"] if r != row["orig_region"]),
        axis=1
    )

    mh_latency["expected_roundtrip"] = mh_latency.apply(
        lambda row: DELAY[int(row["orig_region"])][int(row["region_filtered"])]*2,
        axis=1
    )

    mh_latency["latency_genuine_degree"] = (
            mh_latency["latency"] / mh_latency["expected_roundtrip"]
    )

    total_rows = len(mh_latency)

    agg_funcs = {
        f'p{int(quantile * 100)}': ('latency_genuine_degree', lambda x, q=quantile: x.quantile(q)) for quantile in
        percentile_cols.values()
    }

    global_stats = {
        "expected_roundtrip": 0.0,
        **{
            f'p{int(p * 100)}': mh_latency["latency_genuine_degree"].quantile(p) for p in
            percentile_cols.values()
        },
        "pct_of_total": 1.0,
        "prefix": prefix
    }

    global_df = pd.DataFrame([global_stats])

    # Make statistics
    summary = (
        mh_latency
        .groupby("expected_roundtrip")
        .agg(**agg_funcs,
             count=("latency_genuine_degree", "size"))
        .reset_index()
    )
    summary["pct_of_total"] = summary["count"] / total_rows

    summary["prefix"] = prefix
    summary = summary.drop(columns="count")

    summary_with_global = pd.concat(
        [summary, global_df],
        ignore_index=True
    )
    append_rows_to_parquet(summary_with_global, summary_parquet_path)


    sorted_degrees = np.sort(mh_latency["latency_genuine_degree"])
    quantiles = np.linspace(0, 1, 1001)  # 0%, 0.1%, ..., 100%
    degree_quantiles = np.quantile(sorted_degrees, quantiles)
    cdf = np.arange(1, len(degree_quantiles) + 1) / len(degree_quantiles)

    cdf_df = pd.DataFrame({
        'latency': degree_quantiles,
        'cdf': cdf,
        'prefix': prefix
    })
    append_rows_to_parquet(cdf_df, parquet_path)
    print(f"Latency Genuine Degree calc: {(datetime.now() - now).total_seconds():.2f}s")
    del mh_latency
    del dataframe


def quantiles_calc(latency_df, parquet_path, prefix, percentile_cols, duration, txn_type="all", region="all", op_type="all", participants="all"):
    dataframe = ensure_dataframe(latency_df)
    if dataframe.empty:
        return
    now = datetime.now()
    agg_funcs = {
        f'p{int(quantile * 100)}': ('latency', lambda x, q=quantile: x.quantile(q)) for quantile in
        percentile_cols.values()
    }
    agg_funcs['tput'] = ('latency', lambda x, q=duration: x.count() / q)
    agg_result = {
        name: func(dataframe[col])
        for name, (col, func) in agg_funcs.items()
    }
    agg_result["prefix"] = prefix
    agg_result["txn_type"] = txn_type
    agg_result["region"] = region
    agg_result["op_type"] = op_type
    agg_result["participants"] = participants
    append_rows_to_parquet(pd.DataFrame([agg_result]), parquet_path)
    del dataframe
    print(f"Quantile calc: {(datetime.now() - now).total_seconds():.2f}s")

# Individual processing for transactions that are read-only or read-write
def per_operation_type_quantiles_calc(latency_df, parquet_path, prefix, percentile_cols, duration, txn_type="all", region="all"):
    dataframe = ensure_dataframe(latency_df)
    if dataframe.empty:
        return
    read_only_latency = dataframe[dataframe["is_rw"] == False]
    if not read_only_latency.empty:
        quantiles_calc(read_only_latency, parquet_path, prefix, percentile_cols, duration, txn_type, region, "ro")

    rw_latency = dataframe[dataframe["is_rw"] == True]
    if not rw_latency.empty:
        quantiles_calc(rw_latency, parquet_path, prefix, percentile_cols, duration, txn_type, region, "rw")
    del dataframe, read_only_latency, rw_latency

def per_type_quantiles_calc(latency_df, parquet_path, prefix, percentile_cols, duration, region="all"):
    dataframe = ensure_dataframe(latency_df)
    if dataframe.empty:
        return
    sh_latency = dataframe[dataframe["is_mh"] == False]
    if not sh_latency.empty:
        quantiles_calc(sh_latency, parquet_path, prefix, percentile_cols, duration, "sh", region)
        per_operation_type_quantiles_calc(sh_latency, parquet_path, prefix, percentile_cols, duration, "sh", region)
    mh_latency = dataframe[dataframe["is_mh"] == True]
    if not mh_latency.empty:
        quantiles_calc(mh_latency, parquet_path, prefix, percentile_cols, duration, "mh", region)
        per_operation_type_quantiles_calc(mh_latency, parquet_path, prefix, percentile_cols, duration, "mh", region)

    del dataframe, sh_latency, mh_latency

def per_participating_regions_calc(latency_df, parquet_path, prefix, percentile_cols, duration, all_regions, region="all"):
    dataframe = ensure_dataframe(latency_df)
    if dataframe.empty:
        return

    mh_latency = dataframe[dataframe["is_mh"] == True]
    if not mh_latency.empty:

        for r in all_regions:
            region_latency_df = mh_latency[mh_latency["regions"].apply(lambda arr: r in arr)]

            if not region_latency_df.empty:
                quantiles_calc(region_latency_df, parquet_path, prefix, percentile_cols, duration, region=str(region), participants=str(r))



def per_region_quantiles_calc(latency_df, parquet_path, prefix, percentile_cols, duration):
    dataframe = ensure_dataframe(latency_df)
    if dataframe.empty:
        return
    regions = dataframe["orig_region"].unique().tolist()

    for region in regions:
        region_latency_df = dataframe[dataframe["orig_region"] == region]
        if not region_latency_df.empty:

            subset_regions = regions.copy()
            subset_regions.remove(region)

            quantiles_calc(region_latency_df, parquet_path, prefix, percentile_cols, duration, region=str(region))
            per_operation_type_quantiles_calc(region_latency_df, parquet_path, prefix, percentile_cols, duration, region=str(region))
            per_type_quantiles_calc(region_latency_df, parquet_path, prefix, percentile_cols, duration, region=str(region))
            per_participating_regions_calc(region_latency_df, parquet_path, prefix, percentile_cols, duration, subset_regions, region=str(region))

        del region_latency_df

    del dataframe


def cdf_calc(latency_df, parquet_path, prefix, txn_type="all", region="all", op_type="all"):

    dataframe = ensure_dataframe(latency_df)

    if dataframe.empty:
        return

    now = datetime.now()

    sorted_latencies = np.sort(dataframe['latency'])

    quantiles = np.linspace(0, 1, 1001)  # 0%, 0.1%, ..., 100%
    latency_quantiles = np.quantile(sorted_latencies, quantiles)
    cdf = np.arange(1, len(latency_quantiles) + 1) / len(latency_quantiles)

    cdf_df = pd.DataFrame({
        'latency': latency_quantiles,
        'cdf': cdf,
        'region': region,
        'txn_type': txn_type,
        'op_type': op_type,
        'prefix': prefix
    })
    append_rows_to_parquet(cdf_df, parquet_path)
    print(f"Quantile calc: {(datetime.now() - now).total_seconds():.2f}s")
    del dataframe

def per_op_type_cdf_calc(latency_df, parquet_path, prefix, txn_type="all", region="all"):
    dataframe = ensure_dataframe(latency_df)
    if dataframe.empty:
        return
    ro_latency = dataframe[dataframe["is_rw"] == False]
    if not ro_latency.empty:
        cdf_calc(ro_latency, parquet_path, prefix, txn_type, region, "ro")

    rw_latency = dataframe[dataframe["is_rw"] == True]
    if not rw_latency.empty:
        cdf_calc(rw_latency, parquet_path, prefix, txn_type, region, "rw")

    del dataframe, ro_latency, rw_latency

def per_type_cdf_calc(latency_df, parquet_path, prefix, region="all"):
    dataframe = ensure_dataframe(latency_df)
    if dataframe.empty:
        return
    sh_latency = dataframe[dataframe["is_mh"] == False]
    if not sh_latency.empty:
        cdf_calc(sh_latency, parquet_path, prefix, "sh", region)
        per_op_type_cdf_calc(sh_latency, parquet_path, prefix, "sh", region)

    mh_latency = dataframe[dataframe["is_mh"] == True]
    if not mh_latency.empty:
        cdf_calc(mh_latency, parquet_path, prefix, "mh", region)
        per_op_type_cdf_calc(mh_latency, parquet_path, prefix, "mh", region)


    del dataframe, sh_latency, mh_latency

def per_region_cdf_calc(latency_df, parquet_path, prefix):

    dataframe = ensure_dataframe(latency_df)
    if dataframe.empty:
        return
    regions = dataframe["orig_region"].unique().tolist()

    for region in regions:
        region_latency_df = dataframe[dataframe["orig_region"] == region]
        if not region_latency_df.empty:
            cdf_calc(region_latency_df, parquet_path, prefix, region=region)
            per_op_type_cdf_calc(region_latency_df, parquet_path, prefix, region=region)
            per_type_cdf_calc(region_latency_df, parquet_path, prefix, region=region)

        del region_latency_df

    del dataframe

"""
Profile-related Statistics
"""

def profile_exec(profiling_df, experiment, percentile_cols, path, local_prefix, txn_type="all", region="all"):

    dataframe = ensure_dataframe(profiling_df)

    if dataframe.empty:
        return

    now = datetime.now()

    q_values = [q for q in percentile_cols.values()]

    modules = list(experiment.modules.keys())

    rows = []

    for q in q_values:
        row = {'quantile': q}
        for module in modules:
            if module in dataframe:
                row[module] = dataframe[module].quantile(q)
        row['prefix'] = local_prefix
        row['txn_type'] = txn_type
        row['region'] = region
        rows.append(row)

    final_df = pd.DataFrame(rows)
    append_rows_to_parquet(final_df, path)

    print(f"Profile calc: {(datetime.now() - now).total_seconds():.2f}s")

    del dataframe, final_df

def per_txn_type_profile_exec(profiling_df, experiment, percentile_cols, path, local_prefix, region="all"):
    dataframe = ensure_dataframe(profiling_df)
    if dataframe.empty:
        return
    sh_profiling = dataframe[dataframe["is_mh"] == False]
    if not sh_profiling.empty:
        profile_exec(sh_profiling, experiment, percentile_cols, path, local_prefix, "sh", region)

    mh_profiling = dataframe[dataframe["is_mh"] == True]
    if not mh_profiling.empty:
        profile_exec(mh_profiling, experiment, percentile_cols, path, local_prefix, "mh", region)

    del dataframe, sh_profiling, mh_profiling


def per_region_profile_exec(profiling_df, experiment, percentile_cols, path, local_prefix):

    dataframe = ensure_dataframe(profiling_df)
    if dataframe.empty:
        return
    regions = dataframe["orig_region"].unique().tolist()

    for region in regions:
        region_profiling_df = dataframe[dataframe["orig_region"] == region]
        if not region_profiling_df.empty:
            profile_exec(region_profiling_df, experiment, percentile_cols, path, local_prefix, region=str(region))
            per_txn_type_profile_exec(region_profiling_df, experiment, percentile_cols, path, local_prefix, region=str(region))

        del region_profiling_df

    del dataframe



def get_index_df(tmp_path, workload):
    spark = common.create_spark()

    if workload == "tpcc":
        index_df = common.get_single_exp_index(spark, tmp_path).toPandas().convert_dtypes().astype({
                            "wl:sh_only": "int32",
                            "wl:mh_zipf": "int32"
                        })
    elif workload == "ycsb":
        index_df = common.get_single_exp_index(spark, tmp_path).toPandas().convert_dtypes().astype({
            "wl:zipf": "double",
            "wl:mh": "int32",
            "wl:mh_zipf": "int32",
            "wl:fi": "int32"

        })
    else:
        index_df = common.get_single_exp_index(spark, tmp_path).toPandas().convert_dtypes().astype({
            "wl:hot": "int32",
            "wl:mh": "int32",
            "wl:mp": "int32",
            "clients": "int32",
            "wl:mh_zipf": "int32"
        })

    spark.stop()
    return index_df

def get_latency_df(prefix, local_offset, duration, latency_tmp_path, keep_latency=False, latency_path=""):
    spark = common.create_spark()

    latency_sdf = common.latency(
        spark,
        prefix,
        sample=1.0,
        start_offset_sec=local_offset,
        duration_sec=duration
    )

    latency_sdf.write.parquet(latency_tmp_path)

    if keep_latency:
        latency_sdf.toPandas().to_parquet(latency_path)

    spark.stop()

def get_profiling_df(prefix, experiment, local_offset, duration):
    spark = common.create_spark()
    profiling_df = profiling.general_profiling_modules(
        spark,
        prefix,
        experiment.label,
        experiment.modules,
        experiment.collapsing_columns,
        1,
        start_offset_sec=local_offset,
        duration_sec=duration
    ) \
        .toPandas()
    spark.stop()
    return profiling_df

def get_deadlock_df(prefix):
    spark = common.create_spark()
    start_time = 0
    for p in prefix:
        start_time = max(common.summary_csv(spark, p).toPandas()["start_time"].min(), start_time)
    deadlocks_df = common.deadlocks_dfs(spark, prefix, start_time).toPandas()

    spark.stop()
    return deadlocks_df

def get_stub_goodput_df(prefix):
    spark = common.create_spark()
    start_time = 0
    for p in prefix:
        start_time = max(common.summary_csv(spark, p).toPandas()["start_time"].min(), start_time)

    stub_goodput_df = common.stub_goodput_dfs(spark, prefix, start_time).toPandas()


    spark.stop()
    return stub_goodput_df

def get_stub_queue_df(prefix):
    spark = common.create_spark()
    start_time = 0
    for p in prefix:
        start_time = max(common.summary_csv(spark, p).toPandas()["start_time"].min(), start_time)

    stub_goodput_df = common.stub_queue_dfs(spark, prefix, start_time).toPandas()

    spark.stop()
    return stub_goodput_df

def run_in_process(func, *args, **kwargs):
    # Wrapper to run in subprocess and get result via Queue
    def wrapper(q, *args, **kwargs):
        result = func(*args, **kwargs)
        q.put(result)
    q = mp.Queue()
    p = mp.Process(target=wrapper, args=(q, *args), kwargs=kwargs)
    p.start()
    f_result = q.get()
    p.join()
    return f_result

def process_experiment(exp_path, settings):

    remove_error = settings.remove_error
    extract_profiling = settings.extract_profiling
    extract_cdf = settings.extract_cdf
    workers = settings.workers
    keep_latency = settings.keep_latency


    # First, check if it has the required metadata in finished.json
    if not os.path.exists(f"{exp_path}/finished.json"):
        print(f"")
        print(f"Error: Experiment {exp_path} does not have the required metadata file finished.json.")
        if remove_error:
            shutil.rmtree(exp_path)
        return False


    finished_json = None
    try:
        with open(f"{exp_path}/finished.json", 'r') as file:
            finished_json = json.load(file)
    except Exception as e:
        print(f"Unable to load {exp_path}/finished.json: {e} ")
        if remove_error:
            shutil.rmtree(exp_path)
        return False


    begin = datetime.now()

    # Depending on the type of workload, the index dataframe may have different information
    # Extract the workload type first to later obtain the index dataframe
    workload = ""
    if "exec_type" in finished_json:
        workload = finished_json["exec_type"]

    try:
        index_df = run_in_process(get_index_df, exp_path, workload)
    except Exception as e:
        print(f"Error: Unable to obtain index_df for experiment {exp_path}.")
        if remove_error:
            shutil.rmtree(exp_path)
        return False

    if not len(index_df["prefix"]) == 1:
        print(f"Error: Experiment {exp_path} should only include one experiment.")
        if remove_error:
            shutil.rmtree(exp_path)
        return False

    # Add the missing metadata obtained from the json into the index
    json_information = ["system", "replication", "execution"]

    for column in json_information:
        if column not in finished_json:
            print(f"Error: Value {column} does not exist in finished.json")
            if remove_error:
                shutil.rmtree(exp_path)
            return False

        index_df[column] = finished_json[column]

    local_prefix = os.path.basename(index_df["prefix"][0])

    index_df["exp_name"] = local_prefix
    index_df["workload"] = finished_json["exec_type"]

    prefix_path = os.path.join(parquet_paths, local_prefix)
    if os.path.exists(prefix_path):
        print(f"Experiment {prefix_path} already exists, overwrite with new result")
        shutil.rmtree(prefix_path)

    os.makedirs(prefix_path)

    additional_params = ["jitter", "warehouses", "txn_issuing_delay"]

    for p in additional_params:
        if p in finished_json:
            index_df[p] = finished_json[p]

    # Extract the number of regions and partitions
    num_regs, num_partitions = common.get_num_regions(exp_path)

    index_df["num_regions"] = num_regs
    index_df["num_partitions"] = num_partitions

    # Initialize experiment info according to the system the experiment corresponds to

    experiment = exp_dict[finished_json["system"]]
    experiment.initialize(finished_json["config"], finished_json["system"])

    local_offset = offset + experiment.epoch_delay
    now = datetime.now()

    # Obtain latency dataframe and keep it as a parquet to reutilize later if needed
    if not os.path.exists(latency_tmp_path):
        try:
            run_in_process(get_latency_df, *[index_df["prefix"], local_offset, duration, latency_tmp_path])

        except Exception as e:
            print(f"Error: Unable to obtain latency_df for experiment {exp_path}: {e}")
            if remove_error:
                shutil.rmtree(exp_path)
            return False

    else:
        print("Continuing with existing latency cdf")
    print(f"Latency cdf: {(datetime.now() - now).total_seconds():.2f}s")

    now = datetime.now()


    profiling_df = pd.DataFrame()

    if extract_profiling:
        if not os.path.exists(profiling_tmp_path):
            profiling_df = run_in_process(get_profiling_df,
                                          *[index_df["prefix"], experiment, local_offset, duration])
            # Remove temporary information from checkpoints
            checkpoint_paths = glob("/tmp/checkpoint_*.csv")
            for path in checkpoint_paths:
                if os.path.isdir(path):
                    shutil.rmtree(path)

            result_paths = glob("/tmp/result_*.parquet")

            for path in result_paths:
                if os.path.isdir(path):
                    shutil.rmtree(path)

            profiling_df.to_parquet(profiling_tmp_path)

    print(f"Profiling cdf: {(datetime.now() - now).total_seconds():.2f}s")

    now = datetime.now()

    index_df.to_parquet(f"{prefix_path}/index.parquet")
    local_index = index_df["prefix"][0]

    if os.path.exists(f"{tmp_path}/{local_prefix}/server/0-0/deadlocks.csv"):
        deadlocks_df = run_in_process(get_deadlock_df, index_df["prefix"])
        deadlocks_df.to_parquet(f"{prefix_path}/deadlocks.parquet")

    if os.path.exists(f"{tmp_path}/{local_prefix}/server/0-0/stub_goodput.csv"):
        stub_goodput_df = run_in_process(get_stub_goodput_df, index_df["prefix"])
        stub_goodput_df.to_parquet(f"{prefix_path}/stub_goodput.parquet")

    if os.path.exists(f"{tmp_path}/{local_prefix}/server/0-0/stub_queue.csv"):
        stub_queue_df = run_in_process(get_stub_queue_df, index_df["prefix"])
        stub_queue_df.to_parquet(f"{prefix_path}/stub_queue.parquet")
    success = True

    if keep_latency:
        run_in_process(latency_genuine_metric, latency_tmp_path, f"{prefix_path}/latency_genuine_degree.parquet", f"{prefix_path}/latency_genuine_degree_summary.parquet", index_df["prefix"][0], percentile_cols)

    with ProcessPoolExecutor(max_workers=workers) as executor:
        quantiles_futures = [
            executor.submit(quantiles_calc, latency_tmp_path, f"{prefix_path}/latency_quantiles.parquet",
                            index_df["prefix"][0], percentile_cols, duration),
            executor.submit(per_operation_type_quantiles_calc, latency_tmp_path,
                            f"{prefix_path}/per_op_type_latency_quantiles.parquet", index_df["prefix"][0],
                            percentile_cols, duration),
            executor.submit(per_type_quantiles_calc, latency_tmp_path,
                            f"{prefix_path}/per_type_latency_quantiles.parquet",
                            index_df["prefix"][0], percentile_cols, duration),
            executor.submit(per_region_quantiles_calc, latency_tmp_path,
                            f"{prefix_path}/per_region_latency_quantiles.parquet",
                            index_df["prefix"][0], percentile_cols, duration)
        ]


        results = [f.result() for f in quantiles_futures]

    if extract_cdf:
        try:
            with ProcessPoolExecutor(max_workers=workers) as executor:

                cdf_futures = [
                    executor.submit(cdf_calc, latency_tmp_path, f"{prefix_path}/cdf.parquet", index_df["prefix"][0]),
                    executor.submit(per_op_type_cdf_calc, latency_tmp_path, f"{prefix_path}/per_op_type_cdf.parquet",
                                    index_df["prefix"][0]),
                    executor.submit(per_type_cdf_calc, latency_tmp_path, f"{prefix_path}/per_type_cdf.parquet",
                                    index_df["prefix"][0]),
                    executor.submit(per_region_cdf_calc, latency_tmp_path,
                                    f"{prefix_path}/per_region_cdf.parquet", index_df["prefix"][0])
                ]

                results = [f.result() for f in cdf_futures]
        except Exception as e:
            print(f"Failed processing of of CDF functions: {e}")
            success = False

    if extract_profiling:
        try:
            with ProcessPoolExecutor(max_workers=workers) as executor:
                profiling_futures = [
                    executor.submit(profile_exec, profiling_tmp_path, experiment, percentile_cols,
                                    f"{prefix_path}/profiling_quantiles.parquet", index_df["prefix"][0]),
                    executor.submit(per_txn_type_profile_exec, profiling_tmp_path, experiment, percentile_cols,
                                    f"{prefix_path}/per_type_profiling_quantiles.parquet", index_df["prefix"][0]),
                    executor.submit(per_region_profile_exec, profiling_tmp_path, experiment, percentile_cols,
                                    f"{prefix_path}/per_region_profiling_quantiles.parquet", index_df["prefix"][0])
                ]

                results = [f.result() for f in profiling_futures]
        except Exception as e:
            print(f"Failed processing of profiling functions: {e}")
            success = False

    print(f"Executors Done: {(datetime.now() - now).total_seconds():.2f}s")

    end = datetime.now()
    print(f"Time Execution: {(end - begin).total_seconds():.2f}s ")

    del index_df

    if success or remove_error:
        # Remove all tmp information
        shutil.rmtree(tmp_path)
        os.makedirs(tmp_path)

        df_tmp_paths = [profiling_tmp_path, latency_tmp_path]

        for df_path in df_tmp_paths:
            if os.path.exists(df_path):
                if os.path.isdir(df_path):
                    shutil.rmtree(df_path)
                else:
                    os.remove(df_path)

        return True

    else:
        print(f"Processing of experiment {exp_path} failed. Stoping the program")
        return False

# Current hard-coded path and information

offset = 25
duration = 30
latency_tmp_path = "/tmp/latency_df.parquet"
profiling_tmp_path = "/tmp/profiling_df.parquet"
result_path = "/home/rasoares/results"
tmp_path = "/home/rasoares/parquet_tmp"
parquet_paths = "/home/rasoares/parquets"

percentile_cols = {
    "med": 0.5,
    "q1": 0.25,
    "q3": 0.75,
    "min": 0.0,
    "p90": 0.9,
    "p99": 0.99,
    "max": 1
}


def process_results(settings):


    stop_on_error = True

    # Check if all requires paths exist and create them otherwise
    prerequisite_paths = [result_path, tmp_path, parquet_paths]

    for path in prerequisite_paths:
        if not os.path.exists(path):
            os.makedirs(path)

    # Check if the script was in the middle of processing some command. If so, start there
    if len(os.listdir(tmp_path)) > 0:
        experiments = os.listdir(tmp_path)
        for exp in experiments:
            single_exp_path = os.path.join(tmp_path, exp)

            success = process_experiment(single_exp_path, settings)

            if stop_on_error and not success:
                return


    while True:
        experiments = os.listdir(result_path)

        for exp_set in experiments:
            exp_set_path = os.path.join(result_path, exp_set)

            for single_exp in os.listdir(exp_set_path):

                single_exp_path = os.path.join(exp_set_path, single_exp)

                # Check if finished json exists, marking the completion of the experiment
                if not os.path.exists(f"{single_exp_path}/finished.json"):
                    continue

                print(f"Moving {single_exp_path} to {tmp_path}")
                shutil.move(single_exp_path, tmp_path)

                success = process_experiment(os.path.join(tmp_path, single_exp), settings)

                if stop_on_error and not success:
                    return
        gc.collect()

        print("Waiting for more experiments!")
        sleep(5)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Example script with an extract profiling flag"
    )

    parser.add_argument("--workers", "-t",
                        default=4, type=int, help="Number of parallel worker threads")

    parser.add_argument(
        "--extract-profiling", "-p",
        action="store_true",
        help="Enable extract profiling"
    )

    parser.add_argument(
        "--extract-cdf", "-c",
        action="store_true",
        help="Enable extract CDF"
    )

    parser.add_argument(
        "--keep-latency", "-l",
        action="store_true",
        help="Keep transaction latencies"
    )

    parser.add_argument(
        "--remove-error", "-e",
        action="store_true",
        help="Remove experiment upon error in processing"
    )

    args = parser.parse_args()

    print(f"Profiling: {args.extract_profiling}")
    print(f"CDF: {args.extract_cdf}")
    print(f"Workers: {args.workers}")
    print(f"Remove on Error: {args.remove_error}")
    print(f"Keep Latency: {args.keep_latency}")

    process_results(args)

# While(True):
#   Scan the Input directory for any new directories
#   If it finds one, check if a special file flagging the end of experiment has been set
#   When the file has been inserted (probably a json), read the information regarding
#       what is its contents and which functions to run

#   Begin Processing:
#       Get required info for the multiple graphs. (Get per-region and global)
#       This information should encompass:
#           Global and Per-Region
#           Per Txn Type (Local, Global and Both)
#
#       Latency:
#           Get quantile information (both Single Home, Multi-Home and both At the same Time)
#           Get CDF Graph
#
#       Tput:
#           Txns/s
#
#       Profiling:
#           Quantile Information
#           CDF
#
#       Detock Deadlocks
