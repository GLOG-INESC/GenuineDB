from common import apply_mask
import matplotlib.pyplot as plt
from matplotlib.lines import Line2D
import plot_generators

def multi_home_scaling_tput(experiments, plot_name, output_path, mask=None, exp_mask=None):
    masked_dataframes = {}

    systems_legend = []

    if mask is None:
        mask = {}

    if exp_mask is None:
        exp_mask = {}

    zipf_scaling = set()

    xticks = set()
    # Dry run to obtain the values for rows and axis and obtain dataframes
    for e in experiments:
        assert "latency" in experiments[e], "Graph requires latency quantile cdf"

        masked_dataframes[e] = {}

        for df_name in ["latency"]:

            df = apply_mask(experiments[e][df_name], mask=mask)
            if e in exp_mask:
                df = apply_mask(df, exp_mask[e])
            if not df.empty:
                masked_dataframes[e][df_name] = df

                zipf_scaling.update(df["wl:zipf"].unique())
                xticks.update(df["wl:mh"].unique())

    # fig, axes = plt.subplots(len(row_axis_values), len(column_axis_values), figsize=(5*len(column_axis_values), 3*len(row_axis_values)), sharey="all", sharex="all")

    zipf_scaling = list(zipf_scaling)
    zipf_scaling.sort()

    xticks = list(xticks)
    xticks.sort()
    fig, axes = plt.subplots(1, 1, figsize=(3.5, 2.6), sharex="all")

    # Real Plot
    for i, e in enumerate(experiments):
        if not masked_dataframes[e]:
            continue

        exp_config = experiments[e]["config"]
        tput_df = masked_dataframes[e]["latency"]


        for j, zipf in enumerate(zipf_scaling):

            zipf_df = tput_df[tput_df["wl:zipf"] == zipf]


            if not zipf_df.empty:
                systems_legend += [
                    Line2D([0], [0], color=exp_config.color, lw=2, label=f"{e}", marker=exp_config.marker)]

                plot_generators.plot_peak_tput_per_value(zipf_df, axes, exp_config, f"",
                                                         "wl:mh")

    axes.tick_params(labelleft=True)
    axes.yaxis.get_label().set_visible(True)
    axes.spines['left'].set_visible(True)
    axes.set_xlabel("Global Transaction %")
    axes.set_xticks(xticks)
    axes.set_xlim(left=0)
    axes.set_ylim(bottom=0)
    fig.legend(
        handles=systems_legend,
        loc='lower left',
        bbox_to_anchor=(0, 1, 1, 0),
        mode='expand',
        ncol=3,
        fontsize=7
    )

    fig.tight_layout()
    fig.savefig(f'{output_path}/{plot_name}.pdf', bbox_inches='tight')
    fig.savefig(f'{output_path}/{plot_name}.jpg', bbox_inches='tight')


def multi_home_scaling_latency(experiments, plot_name, output_path, mask=None, exp_mask=None):
    masked_dataframes = {}

    systems_legend = []
    percentile_cols = {
        "p50": [0.5, "-"],
        "p99": [0.99, "--"]
    }
    if mask is None:
        mask = {}

    if exp_mask is None:
        exp_mask = {}

    zipf_scaling = set()

    xticks = set()
    # Dry run to obtain the values for rows and axis and obtain dataframes
    for e in experiments:
        assert "per_type_latency" in experiments[e], "Graph requires per_type_latency quantile cdf"

        masked_dataframes[e] = {}

        for df_name in ["per_type_latency"]:

            df = apply_mask(experiments[e][df_name], mask=mask)
            if e in exp_mask:
                df = apply_mask(df, exp_mask[e])
            if not df.empty:
                masked_dataframes[e][df_name] = df

                zipf_scaling.update(df["wl:zipf"].unique())
                xticks.update(df["wl:mh"].unique())

    # fig, axes = plt.subplots(len(row_axis_values), len(column_axis_values), figsize=(5*len(column_axis_values), 3*len(row_axis_values)), sharey="all", sharex="all")

    zipf_scaling = list(zipf_scaling)
    zipf_scaling.sort()

    xticks = list(xticks)
    xticks.sort()
    fig, axes = plt.subplots(1, 1, figsize=(3.5, 2.6), sharex="all")

    # Real Plot
    for i, e in enumerate(experiments):
        if not masked_dataframes[e]:
            continue

        exp_config = experiments[e]["config"]

        latency_df = masked_dataframes[e]["per_type_latency"]

        latency_df = latency_df[latency_df["txn_type"] == "mh"]

        #markers = get_markers(len(zipf_scaling))
        #colors = ["#efa9a9", "#e67d7e", "#de5253", "#d62728"]

        for j, zipf in enumerate(zipf_scaling):

            zipf_df = latency_df[latency_df["wl:zipf"] == zipf]


            if not zipf_df.empty:
                systems_legend += [
                    Line2D([0], [0], color=exp_config.color, lw=2, label=f"{e}", marker=exp_config.marker)]

                plot_generators.plot_latency_peak_tput_per_value(zipf_df, axes, exp_config, f"",
                                                                 "wl:mh", percentile_cols, "Global Txn Latency (ms)")

    axes.tick_params(labelleft=True)
    axes.yaxis.get_label().set_visible(True)
    axes.spines['left'].set_visible(True)
    axes.set_xlabel("Global Transaction %")
    axes.set_xticks(xticks)
    axes.set_xlim(left=0)
    axes.set_ylim(bottom=0)
    fig.legend(
        handles=systems_legend,
        loc='lower left',
        bbox_to_anchor=(0, 1, 1, 0),
        mode='expand',
        ncol=3,
        fontsize=7
    )
    """
    fig.legend(
        handles=systems_legend,
    fontsize=7,          # smaller text
        loc='lower left',
        bbox_to_anchor=(0, 1, 1, 0),
        ncol=4,
        mode='expand',
    handlelength=1.2,    # shorter line samples
    handletextpad=0.3,   # less padding between marker and text
    labelspacing=0.2,    # tighter spacing between entries
    )
    """

    fig.tight_layout()
    fig.savefig(f'{output_path}/{plot_name}.pdf', bbox_inches='tight')
    fig.savefig(f'{output_path}/{plot_name}.jpg', bbox_inches='tight')
