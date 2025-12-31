from common import apply_mask
import matplotlib.pyplot as plt
from matplotlib.lines import Line2D
import plot_generators

def convoy_effect_result_graph(experiments, plot_name, percentile_cols, output_path, mask=None, exp_mask=None, txn_types=["0", "100"]):
    masked_dataframes = {}

    if mask is None:
        mask = {}

    if exp_mask is None:
        exp_mask = {}

    for txn_type in txn_types:

        masked_dataframes[txn_type] = {}

        for e in experiments:
            assert "latency" in experiments[e], "Graph requires latency quantile cdf"
            assert "per_type_latency" in experiments[e], "Graph requires per_type_latency quantile cdf"

            masked_dataframes[txn_type][e] = {}

            for df_name in ["latency", "per_type_latency"]:
                local_df = experiments[e][df_name]

                mask["wl:fi"] = txn_type
                local_df = apply_mask(local_df, mask=mask)
                if e in exp_mask:
                    local_df = apply_mask(local_df, exp_mask[e])
                if not local_df.empty:
                    masked_dataframes[txn_type][e][df_name] = local_df

            if not masked_dataframes[txn_type][e]:
                masked_dataframes[txn_type].pop(e)
            else:
                masked_dataframes[txn_type][e]["config"] = experiments[e]["config"]

        if not masked_dataframes[txn_type]:
            masked_dataframes.pop(txn_type)

    for txn_type in txn_types:
        label = ["One-Shot", "2FI"][txn_type == "100"]
        if txn_type in masked_dataframes:
            single_exec_convoy_effect_result_graph(masked_dataframes[txn_type], f"{plot_name}_{label}", percentile_cols, output_path)

def single_exec_convoy_effect_result_graph(experiments, plot_name, percentile_cols, output_path):

    systems_legend = []

    # Dry run to obtain the values for rows and axis and obtain dataframes
    for e in experiments:
        systems_legend += [Line2D([0], [0], color=experiments[e]["config"].color, lw=2, label=e, marker=experiments[e]["config"].marker)]

    # fig, axes = plt.subplots(len(row_axis_values), len(column_axis_values), figsize=(5*len(column_axis_values), 3*len(row_axis_values)), sharey="all", sharex="all")


    fig, axes = plt.subplots(1, 3, figsize=(3.5 * 3, 2.6), sharex="all")

    latency_legend = [Line2D([0], [0], color='black', linestyle=linestyle[1], lw=2, label=latency_name)
                      for latency_name, linestyle in percentile_cols.items()]
    # Real Plot
    for i, e in enumerate(experiments):

        exp_config = experiments[e]["config"]
        df_latency = experiments[e]["per_type_latency"]

        mh_df_latency = df_latency[df_latency["txn_type"] == "mh"]

        plot_generators.plot_zipf_latency_peak_tput(mh_df_latency, axes[0], exp_config, "",
                                                    percentile_cols, "Global Txn Latency (ms)")

        sh_df_latency = df_latency[df_latency["txn_type"] == "sh"]

        plot_generators.plot_zipf_latency_peak_tput(sh_df_latency, axes[1], exp_config, "",
                                                    percentile_cols, "Local Txn Latency (ms)")

        axes[1].sharey(axes[0])

        df_tput = experiments[e]["latency"]

        plot_generators.plot_zipf_peak_tput(df_tput, axes[2], exp_config, "")

    for a in axes:
        a.tick_params(labelleft=True)
        a.yaxis.get_label().set_visible(True)
        a.spines['left'].set_visible(True)

    fig.legend(
        handles=systems_legend + latency_legend,
        loc='lower left',
        bbox_to_anchor=(0, 1, 1, 0),
        mode='expand',
        ncol=3,
        fontsize=11
    )

    fig.tight_layout()
    fig.savefig(f'{output_path}/{plot_name}.pdf', bbox_inches='tight')
    fig.savefig(f'{output_path}/{plot_name}.jpg', bbox_inches='tight')

def latency_genuine_degree_cdf(experiments, output_path, ax):
    systems_legend = []
    exp_linestyles = ["-", "--", "-.", ":"]

    fig, axes = None, None

    if ax is None:
        fig, axes = plt.subplots(1, 1, figsize=(3.5, 2.6), sharey="all", sharex="all")
    else:
        axes = ax

    for i, e in enumerate(experiments):
        systems_legend += [Line2D([0], [0], color=experiments[e]["config"].color, lw=2, label=e, linestyle=exp_linestyles[i])]
        assert "latency_genuine_degree_cdf" in experiments[e], "Dataframes must include the latency genuine degree cdf"

        df = experiments[e]["latency_genuine_degree_cdf"]

        df_sorted = df.sort_values(by='cdf')
        df_sorted.plot(x='latency', y='cdf', ax=axes, label=e, color=experiments[e]["config"].color, linestyle=exp_linestyles[i], legend=False)

    axes.set_xlabel("Global Txn Latency Gap (RTTs)")
    axes.set_ylabel("CDF")
    if ax is None:
        axes.set_xlim(left=0)
        axes.set_ylim(bottom=0)
    axes.set_xticks([1, 10, 20, 30, 40], labels=['1', '10', '20', '30', '40'])
    if fig is not None:
        fig.legend(
            handles=systems_legend,
            loc='lower left',
            bbox_to_anchor=(0, 1, 1, 0),
            mode='expand',
            ncol=3,
            fontsize=10
        )
        fig.tight_layout()
        fig.savefig(f'{output_path}/latency_genuine_degree.pdf', bbox_inches='tight')
        fig.savefig(f'{output_path}/latency_genuine_degree.jpg', bbox_inches='tight')

def convoy_effect_region_cdf(experiments, output_path, exp_mask, client_mask):
    sh_experiments = {}
    mh_experiments = {}

    regions = set()

    systems_legend = []
    exp_linestyles = ["-", "--", "-."]

    region_maping = {
        0: "us-east-1",
        1: "us-east-2",
        2: "eu-west-1",
        3: "eu-west-2",
        4: "ap-northeast-1",
        5: "ap-northeast-2",
        6: "ap-southeast-1",
        7: "ap-southeast-2",
    }

    for i, e in enumerate(experiments):
        systems_legend += [Line2D([0], [0], color=experiments[e]["config"].color, lw=2, label=e, linestyle=exp_linestyles[i])]

        assert "per_region_cdf" in experiments[e], "Dataframes must include the region cdf"

        exp_mask["clients"] = client_mask[e]
        df = apply_mask(experiments[e]["per_region_cdf"], mask=exp_mask)

        if not df.empty:
            sh_df = df[df["txn_type"] == "sh"]
            if not sh_df.empty:
                sh_experiments[e] = sh_df
                regions.update(set(sh_df["region"].unique().tolist()))

            mh_df = df[df["txn_type"] == "mh"]
            if not mh_df.empty:
                mh_experiments[e] = mh_df
                regions.update(set(mh_df["region"].unique().tolist()))

    if "all" in regions:
        regions.remove("all")

    regions = list(regions)
    regions.sort(reverse=True)

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

    def plot_cdf(df, ax, e, r, line, color):

        region_df = df[df["region"] == r]

        region_df_sorted = region_df.sort_values(by='cdf')

        region_df_sorted.plot(x='latency', y='cdf', ax=ax, label=e, color=color, linestyle=line, legend=False)


    for j, txn_type in enumerate([sh_experiments, mh_experiments]):

        fig, axes = plt.subplots(2, 4, figsize=(3.5 * 4, 2.6 * 2), sharey="all", sharex="all")

        for i, e in enumerate(experiments):
            exp_config = experiments[e]["config"]
            sh_df = txn_type[e]

            for k, r in enumerate(regions):
                plot_cdf(sh_df, axes[k // 4][k % 4], e, r, exp_linestyles[i], exp_config.color)

        for k, r in enumerate(regions):
            axes[k // 4][k % 4].set_xlabel("Global Txn Latency (ms)")
            axes[k // 4][k % 4].set_ylabel("CDF")
            axes[k // 4][k % 4].set_title(f"{region_maping[r]}")
            axes[k // 4][k % 4].set_xlim(left=0)
            axes[k // 4][k % 4].set_ylim(bottom=0)
            axes[k // 4][k % 4].tick_params(labelbottom=True, labelleft=True)

        fig.legend(
            handles=systems_legend,
            loc='lower left',
            bbox_to_anchor=(0, 1, 1, 0),
            mode='expand',
            ncol=3,
            fontsize=10
        )

        label = ["local_txn_cdf", "global_txn_cdf"][j]
        fig.tight_layout()
        fig.savefig(f'{output_path}/{label}.pdf', bbox_inches='tight')
        fig.savefig(f'{output_path}/{label}.jpg', bbox_inches='tight')

def convoy_effect_single_region_local_txn_cdf(experiments, output_path, exp_mask, client_mask, ax = None):
    sh_experiments = {}
    mh_experiments = {}



    region_maping = {
        0: "us-east-1",
        1: "us-east-2",
        2: "eu-west-1",
        3: "eu-west-2",
        4: "ap-northeast-1",
        5: "ap-northeast-2",
        6: "ap-southeast-1",
        7: "ap-southeast-2",
    }

    systems_legend = []
    exp_linestyles = ["-", "--", "-.", ":"]

    for i, e in enumerate(experiments):
        systems_legend += [Line2D([0], [0], color=experiments[e]["config"].color, lw=2, label=e, linestyle=exp_linestyles[i])]

        assert "per_region_cdf" in experiments[e], "Dataframes must include the region cdf"

        exp_mask["clients"] = client_mask[e]
        df = apply_mask(experiments[e]["per_region_cdf"], mask=exp_mask)

        if not df.empty:
            sh_df = df[df["txn_type"] == "sh"]
            if not sh_df.empty:
                sh_experiments[e] = sh_df

            mh_df = df[df["txn_type"] == "mh"]
            if not mh_df.empty:
                mh_experiments[e] = mh_df

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

    regions = [
        "us-east",
        "us-west",
        "ap-northeast",
        "ap-southeast"
    ]

    def plot_cdf(df, ax, e, r, line, add_line):

        region_df = df[df["region"] == r]

        region_df_sorted = region_df.sort_values(by='cdf')

        region_df_sorted.plot(x='latency', y='cdf', ax=ax, label=e, color=experiments[e]["config"].color, linestyle=line, legend=False)

    fig, axes = None, None

    if ax is None:
        fig, axes = plt.subplots(1, 2, figsize=(3.5 * 2, 3), sharey="all", sharex="all")
    else:
        axes = ax

    for j, txn_type in enumerate([mh_experiments, sh_experiments]):


        for i, e in enumerate(experiments):
            plot_cdf(txn_type[e], axes[j], e, 7, exp_linestyles[i], j==1 and i==0)

        local_label = ["Global", "Local"][j]

        axes[j].set_xlabel(f"{local_label} Txn Latency (ms)")
        axes[j].set_ylabel("CDF")
        #axes[j].set_title(f"{region_maping[7]}")
        if ax is None:
            axes[j].set_xlim(0)
            axes[j].set_ylim(bottom=0)
        axes[j].tick_params(labelbottom=True, labelleft=True)

    if fig is not None:
        fig.legend(
            handles=systems_legend,
            loc='lower left',
            bbox_to_anchor=(0, 1, 1, 0),
            mode='expand',
            ncol=4,
            fontsize=11
        )

        fig.tight_layout()
        fig.savefig(f'{output_path}/{region_maping[7]}_latency_cdf.pdf', bbox_inches='tight')
        fig.savefig(f'{output_path}/{region_maping[7]}_latency_cdf.jpg', bbox_inches='tight')