import os
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.lines import Line2D
import plot_generators
from plot_generators import PlotFunction
from experiments import convoy_effect, mh_scaling, scalability, tpcc, glog_variants, jitter, costs, clocks

from profiling import per_region_profiling_graph
from helper import helper_plots

pd.set_option('display.max_colwidth', None)
pd.set_option("display.max_columns", None)


class Experiment:
    def __init__(self, index_path: str, label: str, color: str, replication: str, marker: str):
        self.index_path = index_path
        self.label = label
        self.color = color
        self.replication = replication
        self.marker = marker


def create_index(experiment_path):
    # Get all directories from the path
    new_index_parquet = None
    dirs = [os.path.join(experiment_path, d) for d in os.listdir(experiment_path) if
            os.path.isdir(os.path.join(experiment_path, d))]

    for d in dirs:
        exp_index_path = os.path.join(d, "index.parquet")
        if not os.path.exists(exp_index_path):
            print(f"WARNING: Experiment {d} does not have an index")
            continue

        if new_index_parquet is None:
            new_index_parquet = pd.read_parquet(exp_index_path)
        else:
            new_index_parquet = pd.concat([new_index_parquet, pd.read_parquet(exp_index_path)])

    if "wl:fi" in new_index_parquet.columns:
        new_index_parquet["wl:fi"] = new_index_parquet["wl:fi"].astype(str)
    new_index_parquet.to_parquet(os.path.join(experiment_path, "index.parquet"), index=False)

    return new_index_parquet


# output_path = "/home/rasoares/results/plots"
output_path = "results/plots"

print("Obtaining Parquets")

def obtain_results(experiments, parquets):
    dataframes = {}
    for label, experiment in experiments.items():
        if not os.path.exists(f"{experiment.index_path}/index.parquet"):
            index_parquet = create_index(experiment.index_path)
        else:
            index_parquet = pd.read_parquet(f"{experiment.index_path}/index.parquet", engine="pyarrow")

        index_exp = index_parquet[index_parquet["system"] == experiment.label]
        index_exp = index_exp[index_exp["replication"] == experiment.replication]

        # index_exp = index_parquet
        if not index_exp.empty:
            dataframes[label] = {}

            prefixes = list(index_exp["prefix"])
            local_prefixes = [os.path.basename(p) for p in prefixes]

            tput_df = None
            if "latency" in parquets:
                file = parquets["latency"]

                path_to_prefixes = [os.path.join(experiment.index_path, p, file)
                                    for p in local_prefixes
                                    if os.path.exists(os.path.join(experiment.index_path, p, file))
                                    ]

                dataframes[label]["latency"] = pd.concat([pd.read_parquet(f) for f in path_to_prefixes],
                                                         ignore_index=True).merge(index_parquet, on="prefix")
                print(f"Latency {label}")
                tput_df = dataframes[label]["latency"][["tput", "prefix"]]
                tput_df = tput_df.rename(columns={"tput": "total_tput"})
                tput_df = tput_df.drop_duplicates(subset="prefix", keep="first")


            for df_type, file in parquets.items():

                if df_type != "latency":
                    print(f" {df_type} Parquets")

                    path_to_prefixes = [os.path.join(experiment.index_path, p, file)
                                        for p in local_prefixes
                                        if os.path.exists(os.path.join(experiment.index_path, p, file))
                                        ]

                    dfs = []
                    for f in path_to_prefixes:
                        df = pd.read_parquet(f)
                        if df_type == "per_region_cdf":
                            df = df[(df["txn_type"] != "all") & (df["op_type"] == "all")]
                        dfs.append(df)

                    dataframes[label][df_type] = pd.concat(dfs,
                                                           ignore_index=True, copy=False).merge(index_parquet, on="prefix")
                    if tput_df is not None:
                        dataframes[label][df_type] = dataframes[label][df_type].merge(tput_df, on="prefix", how="left")
            dataframes[label]["config"] = experiment

    return dataframes


latency_cdf_plot_func = PlotFunction(plot_generators.plot_cdf, "per_type_cdf")

latency_tput_plot_func = PlotFunction(plot_generators.plot_latency_tput, "per_type_latency")

tput_plot_func_type = PlotFunction(plot_generators.plot_client_tput, "per_type_latency")
latency_client_plot_func_type = PlotFunction(plot_generators.plot_latency_clients, "per_type_latency")

tput_plot_func_all = PlotFunction(plot_generators.plot_client_tput, "latency")
latency_client_plot_func_all = PlotFunction(plot_generators.plot_latency_clients, "latency")

mh_peak_tput_func_type = PlotFunction(plot_generators.plot_peak_tput, "latency")
mh_latency_peak_tput_func_type = PlotFunction(plot_generators.plot_latency_tput, "per_type_latency")

mh_scaling_func_type = PlotFunction(plot_generators.plot_peak_tput_per_value, "latency")
mh_scaling_normalized_func_type = PlotFunction(plot_generators.plot_peak_tput_per_value_normalized_bar, "latency")


# MICROBENCHMARK


def convoy_effect_exp():
    experiments = {
        "GenuineDB": Experiment("results/microbenchmarks/mh", "GLOG", "#2ca02c",
                             "PARTIAL", "o"),

        "SLOG": Experiment("results/microbenchmarks/mh", "SLOG", "#1f77b4",
                           "PARTIAL", "s"),
        "Detock": Experiment("results/microbenchmarks/mh", "DETOCK", "#d62728",
                             "PARTIAL", "^"),
        "Tiga": Experiment("results/microbenchmarks/mh", "TIGA", "#E69F00",
                           "PARTIAL", "D"),
    }

    p = {
        "latency": "latency_quantiles.parquet",
        "per_type_latency": "per_type_latency_quantiles.parquet",
        "per_region_cdf": "per_region_cdf.parquet"

    }

    percentile_cols = {
        "p50": [0.5, "-"],
        "p99": [0.99, "--"]
    }

    dataframes = obtain_results(experiments, p)

    convoy_effect.convoy_effect_result_graph(dataframes, "convoy_effect", percentile_cols, output_path, mask={
        "op_type": "all",
        "wl:mh": 5
    }, txn_types=["0"])


def convoy_single_region_cdf_exp(ax = None):
    experiments = {
        "GenuineDB": Experiment("results/microbenchmarks/mh", "GLOG", "#2ca02c",
                             "PARTIAL", "o"),

        "SLOG": Experiment("results/microbenchmarks/mh", "SLOG", "#1f77b4",
                           "PARTIAL", "s"),
        "Detock": Experiment("results/microbenchmarks/mh", "DETOCK", "#d62728",
                             "PARTIAL", "^"),
        "Tiga": Experiment("results/microbenchmarks/mh", "TIGA", "#E69F00",
                           "PARTIAL", "o"),
    }

    p = {
        "latency": "latency_quantiles.parquet",
        "per_type_latency": "per_type_latency_quantiles.parquet",
        "per_region_cdf": "per_region_cdf.parquet"
    }

    dataframes = obtain_results(experiments, p)

    convoy_effect.convoy_effect_single_region_local_txn_cdf(dataframes, output_path, {
        "op_type": "all",
        "jitter": 1,
        "wl:mh": 5,
        "wl:fi": "0",
        "wl:zipf": 1.0
    }, {
                                                                "Detock": 256,
                                                                "GenuineDB": 768,
                                                                "SLOG": 1280,
                                                            "Tiga": 8192

    }, ax)

def convoy_effect_metric(ax = None):
    experiments = {
        "GenuineDB": Experiment("results/microbenchmarks/latency_genuine_degree", "GLOG", "#2ca02c",
                             "PARTIAL", "o"),

        "SLOG": Experiment("results/microbenchmarks/latency_genuine_degree", "SLOG", "#1f77b4",
                           "PARTIAL", "s"),
        "Detock": Experiment("results/microbenchmarks/latency_genuine_degree", "DETOCK", "#d62728",
                             "PARTIAL", "^"),
        "Tiga": Experiment("results/microbenchmarks/latency_genuine_degree", "TIGA", "#E69F00",
                           "PARTIAL", "o"),
    }

    p = {
        "latency": "latency_quantiles.parquet",
        "latency_genuine_degree_cdf": "latency_genuine_degree.parquet",
    }

    percentile_cols = {
        "p50": [0.5, "-"],
        "p99": [0.99, "--"]
    }

    dataframes = obtain_results(experiments, p)

    convoy_effect.latency_genuine_degree_cdf(dataframes, output_path, ax)

def convoy_effect_cdf_with_metric():
    experiments = {
        "GenuineDB": Experiment("results/microbenchmarks/latency_genuine_degree", "GLOG",
                                "#2ca02c",
                                "PARTIAL", "o"),

        "SLOG": Experiment("results/microbenchmarks/latency_genuine_degree", "SLOG", "#1f77b4",
                           "PARTIAL", "s"),
        "Detock": Experiment("results/microbenchmarks/latency_genuine_degree", "DETOCK",
                             "#d62728",
                             "PARTIAL", "^"),
        "Tiga": Experiment("results/microbenchmarks/latency_genuine_degree", "TIGA", "#E69F00",
                           "PARTIAL", "o"),
    }
    fig, axes = plt.subplots(1, 3, figsize=(3.5*3, 2.7), sharey="all")

    systems_legend = []

    exp_linestyles = ["-", "--", "-.", ":"]

    for i, e in enumerate(experiments):
        systems_legend += [
            Line2D([0], [0], color=experiments[e].color, lw=2, label=e, linestyle=exp_linestyles[i])]



    convoy_single_region_cdf_exp(axes)
    convoy_effect_metric(axes[2])
    labels = ['a)', 'b)', 'c)']

    for i in range(3):
        axes[i].set_xlim(left=0)
        axes[i].set_ylim(bottom=0)
        axes[i].text(
            0.5, -0.3, labels[i],
            transform=axes[i].transAxes,
            ha='center', va='top',
            fontsize=12
        )

    fig.legend(
        handles=systems_legend,
        loc='lower left',
        bbox_to_anchor=(0, 1, 1, 0),
        mode='expand',
        ncol=3,
        fontsize=11
    )

    fig.tight_layout()
    fig.savefig(f'{output_path}/convoy_effect_cdfs.pdf', bbox_inches='tight')
    fig.savefig(f'{output_path}/convoy_effect_cdfs.jpg', bbox_inches='tight')


def region_scaling_exp(ax=None):
    experiments = {
        "SLOG": Experiment("results/microbenchmarks/region_scaling", "SLOG", "#1f77b4",
                           "PARTIAL", "s"),
        "GenuineDB": Experiment("results/microbenchmarks/region_scaling", "GLOG", "#2ca02c",
                                "PARTIAL", "o"),
        "Detock": Experiment("results/microbenchmarks/region_scaling", "DETOCK", "#d62728",
                             "PARTIAL", "^"),
        "Tiga": Experiment("results/microbenchmarks/partition_scaling", "TIGA", "#E69F00",
                           "PARTIAL", "D"),
    }

    p = {
        "latency": "latency_quantiles.parquet",
    }
    dataframes = obtain_results(experiments, p)

    scalability.region_scalability(dataframes, "region_scaling", output_path, mask = {"wl:mh" : 25}, ax=ax)


def partition_scaling_exp(ax=None):


    experiments = {
        "SLOG": Experiment("results/microbenchmarks/partition_scaling", "SLOG",
                           "#1f77b4",
                           "PARTIAL", "s"),
        "GenuineDB": Experiment("results/microbenchmarks/partition_scaling", "GLOG",
                                "#2ca02c",
                                "PARTIAL", "o"),
        "Detock": Experiment("results/microbenchmarks/partition_scaling", "DETOCK",
                             "#d62728",
                             "PARTIAL", "^"),
        "Tiga": Experiment("results/microbenchmarks/partition_scaling", "TIGA", "#E69F00",
                           "PARTIAL", "D"),
    }

    p = {
        "latency": "latency_quantiles.parquet",
        "per_type_latency": "per_type_latency_quantiles.parquet",
    }
    dataframes = obtain_results(experiments, p)

    scalability.partition_scalability(dataframes, "partition_scaling", output_path, ax=ax)


def region_partition_scaling_exp():
    experiments_label = {
        "GenuineDB": Experiment("results/microbenchmarks/partition_scaling", "GLOG",
                                "#2ca02c",
                                "PARTIAL", "o"),

        "SLOG": Experiment("results/microbenchmarks/partition_scaling", "SLOG",
                           "#1f77b4",
                           "PARTIAL", "s"),
        "Detock": Experiment("results/microbenchmarks/partition_scaling", "DETOCK",
                             "#d62728",
                             "PARTIAL", "^"),
        "Tiga": Experiment("results/microbenchmarks/partition_scaling", "TIGA",
                           "#E69F00",
                           "PARTIAL", "D"),
    }
    fig, axes = plt.subplots(1, 2, figsize=(3.5*2, 2.6), sharey="all")

    systems_legend = []

    for i, e in enumerate(experiments_label):
        systems_legend += [
            Line2D([0], [0], color=experiments_label[e].color, lw=2, label=e, marker=experiments_label[e].marker)]

    fig.legend(
        handles=systems_legend,
        loc='lower left',
        bbox_to_anchor=(0, 1, 1, 0),
        mode='expand',
        ncol=3,
        fontsize=11
    )
    region_scaling_exp(axes[0])
    partition_scaling_exp(axes[1])
    axes[0].set_xlim(left=0)
    axes[0].set_ylim(bottom=0)
    axes[1].set_xlim(left=0)
    axes[1].set_ylim(bottom=0)
    fig.tight_layout()
    fig.savefig(f'{output_path}/region_partition_scaling.pdf', bbox_inches='tight')
    fig.savefig(f'{output_path}/region_partition_scaling.jpg', bbox_inches='tight')

def tpcc_exp():
    experiments = {
        "GenuineDB": Experiment("results/aws/tpcc", "GLOG", "#2ca02c",
                             "PARTIAL", "o"),

        "SLOG": Experiment("results/aws/tpcc", "SLOG", "#1f77b4",
                           "PARTIAL", "s"),
        "Detock": Experiment("results/aws/tpcc", "DETOCK", "#d62728", "PARTIAL", "^"),
        "Tiga": Experiment("results/aws/tpcc", "TIGA", "#E69F00",
                           "PARTIAL", "D"),
    }

    p = {
        "latency": "latency_quantiles.parquet",
        "per_type_latency": "per_type_latency_quantiles.parquet"
    }

    percentile_cols = {
        "p50": [0.5, "-"],
        "p99": [0.99, "--"]
    }

    dataframes = obtain_results(experiments, p)

    tpcc.tpcc_result_graph(dataframes, "tpcc", percentile_cols, output_path, exp_mask={"GenuineDB": {"clients" : ["<", 512]}})

def glog_variants_exp():
    experiments = {
        "GenuineDB": Experiment("results/microbenchmarks/glog_variants", "GLOG", "#2ca02c",
                             "PARTIAL", "o"),
        "GenuineDB_Furthest": Experiment("results/microbenchmarks/glog_variants", "GLOG_FURTHEST", "#1f77b4", "PARTIAL", "s"),
        "GenuineDB_DM": Experiment("results/microbenchmarks/glog_variants", "GLOG_DM", "#d62728", "PARTIAL", "^"),
    }

    p = {
        "per_type_cdf": "per_type_cdf.parquet",
    }
    dataframes = obtain_results(experiments, p)

    glog_variants.mh_latency_cdf(dataframes, output_path, {"op_type" : "all", "wl:zipf": 0.75, "wl:fi": "0"}, {
        "GenuineDB" : 128,
    "GenuineDB_Furthest": 128,
    "GenuineDB_DM": 128,
})

def jitter_exp():
    experiments = {
        "GenuineDB": Experiment("results/microbenchmarks/jitter_final", "GLOG", "#2ca02c",
                             "PARTIAL", "o"),

        "SLOG": Experiment("results/microbenchmarks/jitter_final", "SLOG", "#1f77b4",
                           "PARTIAL", "s"),
        "Detock": Experiment("results/microbenchmarks/jitter_final", "DETOCK", "#d62728",
                             "PARTIAL", "^"),
        "Tiga": Experiment("results/microbenchmarks/jitter_final", "TIGA", "#E69F00",
                           "PARTIAL", "D"),
    }

    p = {
        "latency": "latency_quantiles.parquet",
        "per_type_latency": "per_type_latency_quantiles.parquet"
    }

    percentile_cols = {
        "p50": [0.5, "-"],
        "p99": [0.99, "--"]
    }

    dataframes = obtain_results(experiments, p)

    jitter.jitter_scaling(dataframes, "jitter", percentile_cols, output_path, mask={
        "op_type": "all",
        "wl:fi": "0",
        "wl:zipf" : 1.0
    }, exp_mask={
        "GeoZip" : {"clients" : 512},
        "SLOG": {"clients": 512},
        "Tiga": {"clients": 512},
        "Detock": {"clients": 32},
    })

def costs_exp():
    experiments = {
        "GenuineDB": Experiment("results/microbenchmarks/costs/single_exp", "GLOG", "#2ca02c",
                             "PARTIAL", "o")
    }

    p = {
        "latency": "latency_quantiles.parquet",
        "per_type_latency": "per_type_latency_quantiles.parquet",
        "stub_goodput": "stub_goodput.parquet",
        "stub_queue": "stub_queue.parquet",
    }

    #dataframes = obtain_results(experiments, p)
    #costs.costs_txt(dataframes, 25, 30)

    percentile_cols = {
        "p50": [0.5, "-"],
        "p99": [0.99, "--"]
    }

    costs.costs_graph("cost_plot", output_path, experiments["GenuineDB"])

convoy_effect_exp()
glog_variants_exp()
convoy_effect_cdf_with_metric()
costs_exp()
jitter_exp()
region_partition_scaling_exp()
tpcc_exp()