import code

import common
from pyspark import StorageLevel
import pyspark.sql.functions as F
from pyspark.sql.types import StructType
from pyspark.sql.functions import col, lit
import matplotlib.pyplot as plt
import matplotlib
import numpy as np
import gc
import os
import datetime
from functools import reduce
import pandas as pd
from glob import glob
from urllib.parse import urlparse
import shutil
from modules import exp_dict
import matplotlib.gridspec as gridspec

def slog_profiling_modules(spark, prefixes, **kwargs):
    """
    Relevant modules for SLOG execution
    """
    modules = {
        "Enter_Server": ["sent_at", "ENTER_SERVER"],
        "Server": ["ENTER_SERVER", "EXIT_SERVER_TO_FORWARDER"],
        "Forwarder": ["ENTER_FORWARDER", "EXIT_FORWARDER_TO_SEQUENCER"],
        "Forwarder_Sequencer": ["EXIT_FORWARDER_TO_SEQUENCER", "ENTER_SEQUENCER"],
        "Stub": ["ENTER_STUB", "EXIT_STUB"],
        "Stub_Glog": ["EXIT_STUB", "ENTER_GLOG"],
        "GLOG": ["ENTER_GLOG", "EXIT_GLOG"],
        "GLOG_Sequencer": ["EXIT_GLOG", "ENTER_SEQUENCER"],
        "Sequencer": ["ENTER_SEQUENCER", "EXIT_SEQUENCER_IN_BATCH"],
        "Sequencer_RLOG": ["EXIT_SEQUENCER_IN_BATCH", "ENTER_LOG_MANAGER_IN_BATCH"],
        "Paxos": ["ENTER_LOG_MANAGER_IN_BATCH", ["ENTER_LOG_MANAGER_ORDER", "ENTER_LOG_MANAGER_IN_BATCH"]],
        "RLOG": [["ENTER_LOG_MANAGER_ORDER", "ENTER_LOG_MANAGER_IN_BATCH"], "EXIT_LOG_MANAGER"],
        "RLOG_Scheduler": ["EXIT_LOG_MANAGER", "ENTER_SCHEDULER"],
        "Scheduler": [["ENTER_SCHEDULER", "ENTER_SCHEDULER_LO"], "ENTER_LOCK_MANAGER"],
        "Lock_Manager": ["ENTER_LOCK_MANAGER", "DISPATCH"],
        "Scheduler_Worker": ["DISPATCH", "ENTER_WORKER"],
        "Worker_total": ["ENTER_WORKER", "EXIT_WORKER"],
        "Worker_Broadcast": ["ENTER_WORKER", "BROADCAST_READ"],
        "Worker_Remote_read": ["BROADCAST_READ", "GOT_REMOTE_READS"],
        "Worker_Execute": ["GOT_REMOTE_READS", "EXECUTE_TXN"],
        "Worker_Server": ["EXIT_WORKER", "RETURN_TO_SERVER"],
        "Server_end": ["RETURN_TO_SERVER", "EXIT_SERVER_TO_CLIENT"],
        "Exit_Server": ["EXIT_SERVER_TO_CLIENT", "received_at"],
    }

    columns_to_collapse = {
        "DISPATCHED": ["DISPATCHED_FAST", "DISPATCHED_SLOW", "DISPATCHED_SLOW_DEADLOCKED"]
    }

    return general_profiling_modules(spark, prefixes, "slog", modules, columns_to_collapse, **kwargs)

"""
Helper functions
"""

def txn_sdf_normalized(spark, p, column_list, **kwargs):
    """
    Returns the transaction sdf with issuing and received times normalized to the transaction start

    :param spark: Spark session
    :param p: Location of transaction csv logs
    :param column_list: List of columns to obtain from transaction csv
    :param kwargs: additional arguments for transaction_csv, such as limiting transaction scope
    :return: Spark SDF
    """
    txn_sdf = common.transactions_csv(spark, p, **kwargs) \
        .select(column_list)

    # Get lowest timestamp to normalize results
    min_time_sdf = txn_sdf.select(F.min(col("sent_at")).alias("min_sent_at")).first()
    min_time = min_time_sdf["min_sent_at"]

    txn_sdf = txn_sdf.withColumn("sent_at", txn_sdf.sent_at - min_time)
    txn_sdf = txn_sdf.withColumn("prefix", lit(p))

    return txn_sdf

def txn_sdf_sampled(spark, p, column_list, sample=1.0, **kwargs):
    """
    Returns the per-client sampled transaction sdf
    """

    new_column_list = column_list
    # We require this columns from the transaction_csv to sample correctly
    required_columns = ["generator", "orig_region"]
    for column in required_columns:
        if column not in column_list:
            new_column_list.append(column)

    txn_sdf = txn_sdf_normalized(spark, p, new_column_list, **kwargs)

    txn_sdf = txn_sdf.withColumn("isMH", F.size(col("regions")) > 1)

    txn_sdf = txn_sdf.withColumn("temp_composing_key", F.concat_ws("_", txn_sdf["generator"], txn_sdf["isMH"]))
    txn_sdf = txn_sdf.withColumn("composing_key", F.concat_ws("_", txn_sdf["orig_region"], txn_sdf["temp_composing_key"]))

    unique_generators = txn_sdf.select("composing_key").distinct().rdd.flatMap(lambda x: x).collect()


    sample_dict = { generator : sample for generator in unique_generators}

    # For each generator (i.e. client), sample for each txn type (local and global)

    txn_sdf = txn_sdf.sampleBy("composing_key", sample_dict)

    column_drop_list = ["isMH", "composing_key", "temp_composing_key"]

    for column in required_columns:
        if column not in column_list:
            column_drop_list.append(column)

    return txn_sdf.drop(*column_drop_list)


def txn_event_set(spark, p, sample=1.0, **kwargs):
    """
    Obtains the table of txn_id with events
    """
    column_list = ["txn_id", "sent_at", "received_at", "regions", "orig_region"]

    if sample == 1.0:
        final_sdf = txn_sdf_normalized(spark, p, column_list, **kwargs)
    else:
        final_sdf = txn_sdf_sampled(spark, p, column_list, sample, **kwargs)

    events_sdf = common.txn_events_csv(spark, p, start_offset_sec=0, duration_sec=10000000000)

    if events_sdf.count() == 0:
        return None

    events_sdf = events_sdf.dropDuplicates()

    events_sdf = events_sdf.groupby("txn_id").pivot("event").agg(F.max("time"))

    total_events = final_sdf.join(events_sdf, on="txn_id")

    return total_events

def collapse_columns(events_sdf, columns, final_column):
    """
    When a given event can span multiple event types, make them into a single event
    """
    existing_columns = set(events_sdf.columns)
    valid_dispatch_cols = [c for c in columns if c in existing_columns]
    numerator = sum([F.coalesce(col(c), lit(0)) for c in valid_dispatch_cols])
    denominator = sum([F.when(col(c).isNotNull(), 1).otherwise(0) for c in valid_dispatch_cols])

    total_events = events_sdf.withColumn(final_column, numerator / denominator)

    return total_events

def remove_non_existing_columns(events_sdf, modules):
    # All events of the sdf
    event_columns = set(events_sdf.columns)

    # Modules to be removed for lack of event existance
    remove_labels = []

    def collapse_events(record):
        if isinstance(record, list):
            for r in record:
                if r not in event_columns:
                    record.remove(r)

            return len(record) == 0
        else:
            return record not in event_columns

    for (label, records) in modules.items():
        record0_exists = collapse_events(records[0])
        record1_exists = collapse_events(records[1])

        if record0_exists and record1_exists:
            remove_labels.append(label)
        else:
            if record0_exists:
                records[0] = records[1]
            elif record1_exists:
                records[1] = records[0]
    for label in remove_labels:
        modules.pop(label)

def get_column_expr(record):
    """
    For a module, a given application point may be the latest arrival between multiple events
    In this case, we calculate the greatest between them
    """
    if isinstance(record, list):
        # Check if both events exist
        if len(record) == 1:
            return col(record[0])

        return F.greatest(*[col(r) for r in record])
    else:
        return col(record)

def txn_event_diff_set(spark, p, **kwargs):
    """
    Obtains the table of txn_id with the difference between event execution in different machines
    """
    column_list = ["txn_id", "sent_at", "received_at", "regions"]

    final_sdf = txn_sdf_normalized(spark, p, column_list, **kwargs)

    events_sdf = common.txn_events_csv(spark, p, start_offset_sec=0, duration_sec=10000000000)
    events_sdf = events_sdf.dropDuplicates()

    events_sdf = events_sdf.groupby("txn_id").pivot("event").agg(F.max("time")-F.min("time"))

    total_events = final_sdf.join(events_sdf, on="txn_id").cache()
    total_events.count()
    return total_events

"""
Profiling functions
"""
def general_profiling_modules(spark, prefixes, checkpoint_label, modules, collapsing_columns, sample=1.0, **kwargs):
    """
    Returns a Spark SDF with the execution time at each of the system modules

    :param spark: Spark session
    :param prefixes: Location of event logs
    :param modules: Map of modules to profile. The modules must be a dictionary in the form of:
        {
        MODULE: [EVENTS, EVENTS]
        }

        Where MODULE is module name and EVENTS can be either one event or a list of events,
        from which the maximum between them will be used

    :param collapsing_columns: Map of events to be collapsed into one.
        Some application events may be in the form of multiple events
        (like DISPATCH, which can be _FAST, _SLOW or _DEADLOCKED).
        This map presents a mapping of multiple events to be collapsed into one.
        It must be in the form of:
        {
            NEW_EVENT: [EVENTS]
        }
    :param sample: Sampling of transactions per generator node
    :return: Spark SDF with profilling
    """
    profile_aggregated_sdf = None

    CHECKPOINT_PATH = f"/tmp/checkpoint_{checkpoint_label}.csv"
    if os.path.exists(CHECKPOINT_PATH):
        processed_prefixes = spark.read.parquet(CHECKPOINT_PATH).select("prefix").distinct().rdd.map(
            lambda r: r[0]).collect()
    else:
        processed_prefixes = []

    results = []

    for p in prefixes:

        # Make copy of modules as we will modify this dictionary
        prefix_modules = modules.copy()

        path = urlparse(p).path
        last_part = os.path.basename(path)
        if last_part in processed_prefixes:
            print(f"Already processed prefix: {p}")
            continue

        txn_events_sdf = txn_event_set(spark, p, sample, **kwargs)

        if txn_events_sdf is None:
            print("No txn_events_sdf")
            continue

        txn_sdf = common.transactions_csv(spark, p, **kwargs) \
            .select("txn_id", "sent_at", "received_at", "regions", "orig_region")
        txn_sdf = txn_sdf.withColumn("prefix", lit(p))
        txn_sdf = txn_sdf.withColumn("is_mh", F.size(col("regions")) > 1)


        for (new_label, columns) in collapsing_columns.items():
            txn_events_sdf = collapse_columns(txn_events_sdf, columns, new_label)

        remove_non_existing_columns(txn_events_sdf, prefix_modules)

        tmp_join_dfs = []

        for (label, records) in prefix_modules.items():
            col1_expr = get_column_expr(records[0])
            col2_expr = get_column_expr(records[1])

            tmp_sdf = txn_events_sdf.select("txn_id",
                                          F.when(col1_expr.isNotNull() & col2_expr.isNotNull(),
                                                 F.greatest((col2_expr - col1_expr) / 1000000, F.lit(0))).otherwise(
                                              0).alias(label))
            tmp_join_dfs.append(tmp_sdf)
            #txn_sdf = txn_sdf.join(tmp_sdf, on="txn_id")

        combined_tmp_sdf = reduce(lambda df1, df2: df1.join(df2, on="txn_id", how="outer"), tmp_join_dfs)

        txn_sdf = txn_sdf.join(combined_tmp_sdf, on="txn_id")

        # Write per-prefix result to temporary output
        txn_sdf.write.mode("overwrite").parquet(f"/tmp/result_{last_part}.parquet")

        # Append to checkpoint
        spark.createDataFrame([(last_part,)], ["prefix"]).write.mode("append").parquet(CHECKPOINT_PATH)

        print(f"Finished prefix {last_part}")

    result_paths = glob("/tmp/result_*.parquet")
    all_results = [spark.read.parquet(path) for path in result_paths]
    if len(all_results) == 0:
        return spark.createDataFrame([], StructType([]))

    profile_aggregated_sdf = reduce(lambda df1, df2: df1.unionByName(df2, allowMissingColumns=True), all_results)

    return profile_aggregated_sdf

def general_profiling_events(spark, prefixes, events, **kwargs):
    """
        Returns a Spark SDF with the difference in time of event execution between machines

        :param spark: Spark session
        :param prefixes: Location of event logs
        :param kwargs: additional arguments for transaction_csv, such as limiting transaction scope
        :return: Spark SDF
    """

    event_aggregated_sdf = txn_event_diff_set(spark, prefixes[0], **kwargs)

    for p in prefixes[1:]:
        event_sdf = txn_event_diff_set(spark, p, **kwargs)
        event_aggregated_sdf = event_aggregated_sdf.union(event_sdf)
        event_sdf.unpersist()

    return event_aggregated_sdf


def profiling_modules(experiments, graph_config, sample=1.0):
    common.set_plot_style()

    profilings = dict()
    for prefix, experiment in experiments.items():
        ord_overhead_index_df = common.from_cache_or_compute(
            f'{prefix}/ord_index_{experiment.label}.parquet',
            lambda: common.get_index(graph_config.spark, prefix).toPandas().convert_dtypes().astype({
                "wl:hot": "int32",
                "wl:mh": "int32",
                "wl:mp": "int32",
                "clients": "int32"
            })[common.filtered_columns],
            ignore_cache=graph_config.IGNORE_INDEX_CACHE
        )
        ord_overhead_index_df
        local_offset = graph_config.offset + experiment.epoch_delay
        """
        if "Detock" in experiment.label:
            detock_paxos_overhead(graph_config.spark, ord_overhead_index_df["prefix"],
                                  start_offset_sec=local_offset, duration_sec=graph_config.duration)
        """

        txn_profiling_df = common.from_cache_or_compute(
            f'{prefix}/txn_profiling_quartils_{experiment.label}.parquet',
            lambda: general_profiling_modules(
                graph_config.spark,
                ord_overhead_index_df["prefix"],
                experiment.label,
                experiment.modules,
                experiment.collapsing_columns,
                sample,
                start_offset_sec=local_offset,
                duration_sec=graph_config.duration
            ) \
                .toPandas() \
                .merge(ord_overhead_index_df, on="prefix"),
            ignore_cache=graph_config.IGNORE_RESULT_CACHE,
        )
        txn_profiling_df
        checkpoint_paths = glob("/tmp/checkpoint_*.csv")
        for path in checkpoint_paths:
            if os.path.isdir(path):
                shutil.rmtree(path)

        result_paths = glob("/tmp/result_*.parquet")
        for path in result_paths:
            if os.path.isdir(path):
                shutil.rmtree(path)
        profilings[experiment.label] = [txn_profiling_df, experiment]
    return profilings

def quantil_box(row, quartils, label):

    box = {'label': label}

    for key, value in quartils.items():
        assert value in row, f'Row does not have value {value}'
        box[key] = row[value]

    return box

def empty_quantil_box(quartils, label):
    box = {'label': label}

    for key, value in quartils.items():
        box[key] = 0

    return box


def quartil_calc(dataframe, quantiles, column, label):
    """
    Calculates the quantil and corresponding box for a boxplot

    :param dataframe: dataframe of a given experiment
    :param quantiles: quantiles to calculate
    :param column: column to which calculate the quartil
    :param label: label to acompany the box
    """

    q_values = [q for q in quantiles.values()]
    if column not in dataframe.columns:
        return empty_quantil_box(quantiles, label)

    quantiles_df = dataframe[column].quantile(q_values)

    return quantil_box(quantiles_df, quantiles, label)
        #, experiment_configs.colors.profile_color[j % len(experiment_configs.colors.profile_color)]))
        #positions[k].append(i - width / len(hue_values))

def draw_quartil_box(dataframe, ax, quantiles, column, experiment_configs, x_axis, x_values, hue_axis, hue_values):
    """
    Draw the quartil boxes for a given column of a dataframe in a MatPlotLib axis ax

    :param dataframe: dataframe of a given experiment
    :param ax: axis of matplotlib to which to draw to
    :param quantiles: quantiles to calculate
    :param experiment_configs: experiment config regarding colors/plot drawing
    :param column: column to which calculate the quartil
    :param x_axis: column for x_axis of plot
    :param x_values: values to map x_axis to
    :param hue_axis: multiple columns for the same x value
    :param hue_values: values for the hue_axis
    """
    boxes = list()
    positions = list()
    width = 0.8
    box_width = width / len(hue_values)
    x_values.sort()
    for i, x in enumerate(x_values):

        if experiment_configs.cutoff and x_axis in experiment_configs.cutoff:
            if x > experiment_configs.cutoff[x_axis]:
                break

        x_value_filter = dataframe[dataframe[x_axis] == x]


        if not x_value_filter.empty:
            for j, hue in enumerate(hue_values):
                evo_values = x_value_filter[x_value_filter[hue_axis] == hue]
                if not evo_values.empty:
                    box = quartil_calc(evo_values, quantiles, column, f"{x}_{hue}")
                    box['facecolor'] = experiment_configs.colors.profile_color[j % len(experiment_configs.colors.profile_color)]

                    boxes.append(box)
                    position = i - width / 2 + (j + 0.5) * box_width
                    positions.append(position)

    medianprops = dict(color='red', linewidth=2)

    if len(boxes) > 0:
        bp = ax.bxp(boxes, positions=positions, showfliers=False, widths=box_width * 0.9,
                    patch_artist=True, medianprops=medianprops)

        for patch, box_data in zip(bp['boxes'], boxes):
            patch.set_facecolor(box_data['facecolor'])

    ax.set_ylabel("Latency (ms)")
    ax.set_xlabel(x_axis)

    ax.set_title(f"{column} Quartils {type}")


def module_profiling_quartils_per_type(experiments, graph_config, x_axis, x_values, hue_axis, hue_values, sample=1.0):
    """
    Obtains the boxes required to draw a quartil graph.

    :param experiments: experiments
    :param ax: axis of matplotlib to which to draw to
    :param quantiles: quantiles to calculate
    :param experiment_configs: experiment config regarding colors/plot drawing
    :param columns: columns of the dataframe to which calculate the quartils of
    :param x_axis:
    """

    quartil_box_map = {
        'med': 0.5,
        'q1': 0.25,
        'q3': 0.75,
        'whislo': 0.0,
        'whishi': 0.9,
        'fliers': 0.99
    }

    # Split in terms of single home and multiple home_txn
    common.set_plot_style()
    profilings = profiling_modules(experiments, graph_config, sample)


    for experiment_df, experiment in profilings.values():
        modules = list(experiment.modules.keys())

        fig, axes = plt.subplots(len(modules), 2, figsize=(22, 7 * len(modules)))

        # First, restrict to single home and multi_home
        sh_region_data = experiment_df[experiment_df["regions"].apply(lambda x: len(x) == 1 )]

        if not sh_region_data.empty:
            for i, module in enumerate(modules):
                if len(modules) == 1:
                    draw_quartil_box(sh_region_data, axes[0], quartil_box_map,  module, experiment, x_axis, x_values, hue_axis, hue_values)
                else:
                    draw_quartil_box(sh_region_data, axes[i][0], quartil_box_map, module, experiment, x_axis, x_values, hue_axis, hue_values)

        mh_region_data = experiment_df[experiment_df["regions"].apply(lambda x: len(x) > 1 )]

        if not mh_region_data.empty:
            for i, module in enumerate(modules):
                if len(modules) == 1:
                    draw_quartil_box(mh_region_data, axes[1], quartil_box_map, module, experiment, x_axis, x_values,
                                     hue_axis, hue_values)
                else:
                    draw_quartil_box(mh_region_data, axes[i][1], quartil_box_map, module, experiment, x_axis, x_values,
                                     hue_axis, hue_values)

        fig.tight_layout()
        fig.savefig(f'{graph_config.output_path}/module_profile_quartils_{experiment.label}.pdf',
                    bbox_inches='tight')
        fig.savefig(f'{graph_config.output_path}/module_profile_quartils_{experiment.label}.jpg',
                    bbox_inches='tight')

def debug_region(experiments, graph_config, x_axis, x_values, hue_axis, hue_values, sample=1.0):
    profilings = profiling_modules(experiments, graph_config, sample)

    detock_df, experiment = profilings["DETOCK"]

    region_2 = detock_df[detock_df["orig_region"] == 2]

    modules = list(experiment.modules.keys())

    region_2_columns = region_2[modules]

    filtered = region_2_columns[(region_2_columns > 1000).any(axis=1)]

    print(filtered)

def per_region_module_profiling_quartils(dataframe, label, output_path, quartil_box_map, experiment, x_axis, x_values, hue_axis, hue_values):
    # Get unique regions
    unique_regions = dataframe["orig_region"].unique()
    unique_regions.sort()

    modules = list(experiment.modules.keys())

    fig, axes = plt.subplots(len(modules), len(unique_regions), figsize=(44, 7 * len(modules)), sharey='row')

    for r, region in enumerate(unique_regions):

        region_data = dataframe[dataframe["orig_region"] == region]

        if not region_data.empty:
            for i, module in enumerate(modules):
                if len(modules) == 1:
                    draw_quartil_box(region_data, axes[r], quartil_box_map, module, experiment, x_axis, x_values,
                                     hue_axis, hue_values)
                else:
                    draw_quartil_box(region_data, axes[i][r], quartil_box_map, module, experiment, x_axis, x_values,
                                     hue_axis, hue_values)

    fig.tight_layout()
    fig.savefig(f'{output_path}/per_region_profile_quartils_{label}.pdf',
                bbox_inches='tight')
    fig.savefig(f'{output_path}/per_region_profile_quartils_{label}.jpg',
                bbox_inches='tight')


def per_txn_type_per_region_module_profiling_quartils(experiments, graph_config, x_axis, x_values, hue_axis, hue_values, sample=1.0):
    """
    Obtains the boxes required to draw a quartil graph.

    :param experiments: experiments
    :param ax: axis of matplotlib to which to draw to
    :param quantiles: quantiles to calculate
    :param experiment_configs: experiment config regarding colors/plot drawing
    :param columns: columns of the dataframe to which calculate the quartils of
    :param x_axis:
    """

    quartil_box_map = {
        'med': 0.5,
        'q1': 0.25,
        'q3': 0.75,
        'whislo': 0.0,
        'whishi': 0.99,
        'fliers': 1
    }

    # Split in terms of single home and multiple home_txn
    common.set_plot_style()
    profilings = profiling_modules(experiments, graph_config, sample)

    def plot_txn_type(dataframe, exp_label):
        sh_region_data = dataframe[dataframe["regions"].apply(lambda x: len(x) == 1)]
        if not sh_region_data.empty:
            per_region_module_profiling_quartils(sh_region_data, f"{exp_label}_SH", graph_config.output_path, quartil_box_map,
                                            experiment, x_axis, x_values, hue_axis, hue_values)

        mh_region_data = dataframe[dataframe["regions"].apply(lambda x: len(x) > 1)]
        if not mh_region_data.empty:
            per_region_module_profiling_quartils(mh_region_data, f"{exp_label}_MH", graph_config.output_path, quartil_box_map,
                                            experiment, x_axis, x_values, hue_axis, hue_values)

    for experiment_df, experiment in profilings.values():
        if experiment.mask is not None:
            for mask, values in experiment.mask.items():
                for i, value in enumerate(values):
                    experiment_mask_df = experiment_df[experiment_df[mask]==value]
                    label = f"{experiment.label}_{value}"

                    plot_txn_type(experiment_mask_df, label)

        else:
            plot_txn_type(experiment_df, experiment.label)

def per_region_profiling_graph(experiments, output_path):
    for e in experiments:
        if "per_region_profiling" in experiments[e]:
            assert e in exp_dict, f"Missing information regarding {e} modules on modules/exp_dict"

            df = experiments[e]["per_region_profiling"]

            exp_modules = exp_dict[e].modules

            quartil_box_map = {
                'med': 0.5,
                'q1': 0.25,
                'q3': 0.75,
                'whislo': 0.0,
                'whishi': 0.99,
                'fliers': 1
            }

            def quantil_box(dataframe, quartils, column, label):

                box = {'label': label}

                for key, value in quartils.items():
                    quantile_df = dataframe[dataframe["quantile"] == value].drop_duplicates()
                    assert not quantile_df.empty, f'Quantile {value} not found in dataframe'

                    assert quantile_df.shape[0] == 1, f'Dataframe should not have more than 1 row'
                    box[key] = quantile_df[column].values[0]

                return box

            def empty_quantil_box(quartils, label):
                box = {'label': label}

                for key, value in quartils.items():
                    box[key] = 0

                return box

            def quartil_calc(dataframe, quantiles, column, label):
                """
                Calculates the quantil and corresponding box for a boxplot

                :param dataframe: dataframe of a given experiment
                :param quantiles: quantiles to calculate
                :param column: column to which calculate the quartil
                :param label: label to acompany the box
                """

                if column not in dataframe.columns:
                    return empty_quantil_box(quantiles, label)

                return quantil_box(dataframe, quantiles, column, label)
                # , experiment_configs.colors.profile_color[j % len(experiment_configs.colors.profile_color)]))
                # positions[k].append(i - width / len(hue_values))

            def draw_quartil_box(dataframe, ax, quantiles, column, experiment_configs, x_axis, hue_axis, label):
                """
                Draw the quartil boxes for a given column of a dataframe in a MatPlotLib axis ax

                :param dataframe: dataframe of a given experiment
                :param ax: axis of matplotlib to which to draw to
                :param quantiles: quantiles to calculate
                :param experiment_configs: experiment config regarding colors/plot drawing
                :param column: column to which calculate the quartil
                :param x_axis: column for x_axis of plot
                :param hue_axis: multiple columns for the same x value
                """
                boxes = list()
                positions = list()
                width = 0.8

                x_values = dataframe[x_axis].unique().tolist()
                hue_values = dataframe[hue_axis].unique().tolist()

                box_width = width / len(hue_values)
                x_values.sort()

                for i, x in enumerate(x_values):
                    x_value_filter = dataframe[dataframe[x_axis] == x]

                    if not x_value_filter.empty:
                        for j, hue in enumerate(hue_values):

                            evo_values = x_value_filter[x_value_filter[hue_axis] == hue]
                            if not evo_values.empty:
                                box = quartil_calc(evo_values, quantiles, column, f"{x}_{hue}")
                                box['facecolor'] = experiment_configs.colors.profile_color[
                                    j % len(experiment_configs.colors.profile_color)]

                                boxes.append(box)
                                position = i - width / 2 + (j + 0.5) * box_width
                                positions.append(position)

                medianprops = dict(color='red', linewidth=2)

                if len(boxes) > 0:
                    bp = ax.bxp(boxes, positions=positions, showfliers=False, widths=box_width * 0.9,
                                patch_artist=True, medianprops=medianprops)

                    for patch, box_data in zip(bp['boxes'], boxes):
                        patch.set_facecolor(box_data['facecolor'])

                ax.set_ylabel("Latency (ms)")
                ax.set_xlabel(x_axis)

                ax.set_title(label)

            def per_region_module_profiling_quartils(dataframe, fig, quartil_box_map, experiment, x_axis, hue_axis):
                # Get unique regions
                unique_regions = dataframe["region"].unique().tolist()

                if "all" in unique_regions:
                    unique_regions.remove("all")

                unique_regions.sort()

                modules = list(exp_modules.keys())

                fig.set_size_inches(44, 14 * len(modules))
                outer = gridspec.GridSpec(len(modules), 1, figure=fig, hspace=0.4)  # two stacked blocks

                for m, module in enumerate(modules):
                    gs = gridspec.GridSpecFromSubplotSpec(2, 4, subplot_spec=outer[m])

                    axes = []
                    initial_axis = None
                    for i in range(2):
                        row = []
                        for j in range(4):
                            if i == 0 and j == 0:
                                ax = fig.add_subplot(gs[i, j])  # reference axis
                                initial_axis = ax
                            else:
                                ax = fig.add_subplot(gs[i, j], sharex=initial_axis, sharey=initial_axis)
                            row.append(ax)
                        axes.append(row)

                    for r, region in enumerate(unique_regions):
                        if r > 7:
                            break
                        ax = axes[r // 4][r % 4]
                        region_data = dataframe[dataframe["region"] == region]

                        draw_quartil_box(region_data, ax, quartil_box_map, module, experiment, x_axis, hue_axis,
                                         f"Region {region}")

                    # Get the position of this block
                    pos = outer[m].get_position(fig)
                    y_top = pos.y1  # top of the block

                    # Place the title slightly above the block
                    fig.text(0.5, y_top + 0.02, f"Module: {module}", ha="center", fontsize=16, fontweight="bold")

            def plot_txn_type(dataframe, exp_label, x_axis, hue_axis):
                sh_region_data = dataframe[dataframe["txn_type"] == "sh"]
                if not sh_region_data.empty:
                    fig = plt.figure(figsize=(20, 15))

                    per_region_module_profiling_quartils(sh_region_data, fig,
                                                         quartil_box_map,
                                                         exp_dict[e], x_axis, hue_axis)

                    fig.savefig(f'{output_path}/per_region_profile_quartils_{exp_label}_SH.pdf',
                                bbox_inches='tight')
                    fig.savefig(f'{output_path}/per_region_profile_quartils_{exp_label}_SH.jpg',
                                bbox_inches='tight')

                mh_region_data = dataframe[dataframe["txn_type"] == "mh"]
                if not mh_region_data.empty:
                    fig = plt.figure(figsize=(20, 15))
                    per_region_module_profiling_quartils(mh_region_data, fig,
                                                         quartil_box_map,
                                                         exp_dict[e], x_axis, hue_axis)

                    fig.savefig(f'{output_path}/per_region_profile_quartils_{exp_label}_MH.pdf',
                                bbox_inches='tight')
                    fig.savefig(f'{output_path}/per_region_profile_quartils_{exp_label}_MH.jpg',
                                bbox_inches='tight')

            plot_txn_type(df, e, "clients", "wl:zipf")
    print("Per Region Profiling Complete")
