from common import apply_mask
import matplotlib.pyplot as plt
from matplotlib.lines import Line2D
import plot_generators

def region_scalability(experiments, plot_name, output_path, mask=None, exp_mask=None, ax=None):
    masked_dataframes = {}

    systems_legend = []

    if mask is None:
        mask = {}

    if exp_mask is None:
        exp_mask = {}

    regions_scaling = set()
    xticks = set()

    # Dry run to obtain the values for rows and axis and obtain dataframes
    for e in experiments:
        assert "latency" in experiments[e], "Graph requires latency quantile cdf"

        masked_dataframes[e] = {}
        exp_config = experiments[e]["config"]
        masked_dataframes[e]["config"] = exp_config
        for df_name in ["latency"]:
            df = apply_mask(experiments[e][df_name], mask=mask)
            if e in exp_mask:
                df = apply_mask(df, exp_mask[e])
            if not df.empty:
                systems_legend += [
                    Line2D([0], [0], color=exp_config.color, lw=2, label=e, marker=exp_config.marker)]

                masked_dataframes[e][df_name] = df

                regions_scaling.update(df["num_regions"].unique())
            else:
                print(f"Exp {e} no latency")
    # fig, axes = plt.subplots(len(row_axis_values), len(column_axis_values), figsize=(5*len(column_axis_values), 3*len(row_axis_values)), sharey="all", sharex="all")

    regions_scaling = list(regions_scaling)
    regions_scaling.sort()

    fig, axes = None, None

    if ax is None:
        fig, axes = plt.subplots(1, 1, figsize=(3.5, 2.6), sharex="all")
    else:
        axes = ax

    # Real Plot
    for i, e in enumerate(experiments):
        if not masked_dataframes[e]:
            continue

        exp_config = experiments[e]["config"]

        tput_df = masked_dataframes[e]["latency"]

        plot_generators.plot_peak_tput_per_value(tput_df, axes, exp_config, f"",
                                                 "num_regions")

    axes.tick_params(labelleft=True)
    axes.yaxis.get_label().set_visible(True)
    axes.spines['left'].set_visible(True)
    axes.set_xlabel("Number of Regions")
    axes.set_xticks(regions_scaling)
    if ax is None:
        axes.set_xlim(left=0)
        axes.set_ylim(bottom=0)

    if fig is not None:
        fig.legend(
            handles=systems_legend,
            loc='lower left',
            bbox_to_anchor=(0, 1, 1, 0),
            mode='expand',
            ncol=3,
            fontsize=11
        )

        fig.tight_layout()
        fig.savefig(f'{output_path}/{plot_name}.pdf', bbox_inches='tight')
        fig.savefig(f'{output_path}/{plot_name}.jpg', bbox_inches='tight')

def partition_scalability(experiments, plot_name, output_path, mask=None, exp_mask=None, ax=None):
    masked_dataframes = {}

    systems_legend = []

    if mask is None:
        mask = {}

    if exp_mask is None:
        exp_mask = {}

    partition_scaling = set()
    xticks = set()
    # Dry run to obtain the values for rows and axis and obtain dataframes
    for e in experiments:
        assert "latency" in experiments[e], "Graph requires latency quantile cdf"

        masked_dataframes[e] = {}
        exp_config = experiments[e]["config"]
        masked_dataframes[e]["config"] = exp_config

        for df_name in ["latency"]:

            df = apply_mask(experiments[e][df_name], mask=mask)
            if e in exp_mask:
                df = apply_mask(df, exp_mask[e])
            if not df.empty:
                systems_legend += [
                    Line2D([0], [0], color=exp_config.color, lw=2, label=e, marker=exp_config.marker)]

                masked_dataframes[e][df_name] = df

                partition_scaling.update(df["num_partitions"].unique())

    # fig, axes = plt.subplots(len(row_axis_values), len(column_axis_values), figsize=(5*len(column_axis_values), 3*len(row_axis_values)), sharey="all", sharex="all")

    partition_scaling = list(partition_scaling)
    partition_scaling.sort()

    fig, axes = None, None

    if ax is None:
        fig, axes = plt.subplots(1, 1, figsize=(3.5, 2.6), sharex="all")
    else:
        axes = ax
    # Real Plot
    for i, e in enumerate(experiments):
        if not masked_dataframes[e]:
            continue

        exp_config = experiments[e]["config"]

        tput_df = masked_dataframes[e]["latency"]

        plot_generators.plot_peak_tput_per_value(tput_df, axes, exp_config, f"",
                                                 "num_partitions")

    axes.tick_params(labelleft=True)
    axes.yaxis.get_label().set_visible(True)
    axes.spines['left'].set_visible(True)
    axes.set_xlabel("Number of Partitions")
    axes.set_xticks(partition_scaling)
    if ax is None:
        axes.set_xlim(left=0)
        axes.set_ylim(bottom=0)

    if fig is not None:
        fig.legend(
            handles=systems_legend,
            loc='lower left',
            bbox_to_anchor=(0, 1, 1, 0),
            mode='expand',
            ncol=3,
            fontsize=11
        )

        fig.tight_layout()
        fig.savefig(f'{output_path}/{plot_name}.pdf', bbox_inches='tight')
        fig.savefig(f'{output_path}/{plot_name}.jpg', bbox_inches='tight')

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



