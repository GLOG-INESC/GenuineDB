from common import apply_mask
import matplotlib.pyplot as plt
from matplotlib.lines import Line2D
import plot_generators

def jitter_scaling(experiments, plot_name, percentile_cols, output_path, mask=None, exp_mask=None):
    masked_dataframes = {}

    systems_legend = []

    if mask is None:
        mask = {}

    if exp_mask is None:
        exp_mask = {}

    jitter_scale = set()
    xticks = set()

    # Dry run to obtain the values for rows and axis and obtain dataframes
    for e in experiments:
        assert "latency" in experiments[e], "Graph requires latency quantile cdf"
        assert "per_type_latency" in experiments[e], "Graph requires per_type_latency quantile cdf"

        systems_legend += [Line2D([0], [0], color=experiments[e]["config"].color, lw=2, label=e, marker=experiments[e]["config"].marker)]

        masked_dataframes[e] = {}

        for df_name in ["latency", "per_type_latency"]:

            df = apply_mask(experiments[e][df_name], mask=mask)
            if e in exp_mask:
                df = apply_mask(df, exp_mask[e])
            if not df.empty:
                masked_dataframes[e][df_name] = df
                xticks.update(df["jitter"].unique())

    # fig, axes = plt.subplots(len(row_axis_values), len(column_axis_values), figsize=(5*len(column_axis_values), 3*len(row_axis_values)), sharey="all", sharex="all")

    xticks = list(xticks)
    xticks.sort()
    fig, axes = plt.subplots(1, 1, figsize=(3.5, 2.6), sharex="all")

    latency_legend = [Line2D([0], [0], color='black', linestyle=linestyle[1], lw=2, label=latency_name)
                      for latency_name, linestyle in percentile_cols.items()]
    # Real Plot
    for i, e in enumerate(experiments):
        if not masked_dataframes[e]:
            continue

        exp_config = experiments[e]["config"]

        latency_df = masked_dataframes[e]["per_type_latency"]

        mh_df_latency = latency_df[latency_df["txn_type"] == "mh"]

        plot_generators.plot_latency_peak_tput_per_value(mh_df_latency, axes, exp_config,
                                                         "", "jitter",
                                                         percentile_cols, "Global Txn Latency (ms)")

    axes.tick_params(labelleft=True)
    axes.yaxis.get_label().set_visible(True)
    axes.spines['left'].set_visible(True)
    axes.set_xlim(left=0)
    axes.set_ylim(bottom=0)
    axes.set_xticks([0.0, 0.05, 0.1, 0.25, 0.5, 0.75], [0, 5, 10, 25, 50, 75])
    axes.set_xlabel("Jitter (%)")

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
