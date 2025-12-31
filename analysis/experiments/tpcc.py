from common import apply_mask
import matplotlib.pyplot as plt
from matplotlib.lines import Line2D
import plot_generators

def tpcc_result_graph(experiments, plot_name, percentile_cols, output_path, mask=None, exp_mask=None):
    masked_dataframes = {}

    systems_legend = []

    if mask is None:
        mask = {}

    if exp_mask is None:
        exp_mask = {}

    # Dry run to obtain the values for rows and axis and obtain dataframes
    for e in experiments:


        assert "latency" in experiments[e], "Graph requires latency quantile cdf"
        assert "per_type_latency" in experiments[e], "Graph requires per_type_latency quantile cdf"

        systems_legend += [Line2D([0], [0], color=experiments[e]["config"].color, lw=2, label=e, marker=experiments[e]["config"].marker)]

        required_dfs = {}

        for df_name in ["latency", "per_type_latency"]:
            df = experiments[e][df_name]

            df = apply_mask(df, mask=mask)
            if e in exp_mask:
                df = apply_mask(df, exp_mask[e])
            if not df.empty:
                required_dfs[df_name] = df

        masked_dataframes[e] = required_dfs
    # fig, axes = plt.subplots(len(row_axis_values), len(column_axis_values), figsize=(5*len(column_axis_values), 3*len(row_axis_values)), sharey="all", sharex="all")

    fig, axes = plt.subplots(1, 2, figsize=(3.5*2, 2.6), sharex="all")

    latency_legend = [Line2D([0], [0], color='black', linestyle=linestyle[1], lw=2, label=latency_name)
                      for latency_name, linestyle in percentile_cols.items()]
    # Real Plot
    for i, e in enumerate(experiments):

        if not masked_dataframes[e]:
            continue

        exp_config = experiments[e]["config"]
        df_latency = masked_dataframes[e]["per_type_latency"]

        mh_df_latency = df_latency[df_latency["txn_type"] == "mh"]

        print("MH PLOT")
        plot_generators.plot_latency_tput(mh_df_latency, axes[0], exp_config, "", percentile_cols, "Global Txn Latency (ms)")

        sh_df_latency = df_latency[df_latency["txn_type"] == "sh"]
        print("SH PLOT")

        plot_generators.plot_latency_tput(sh_df_latency, axes[1], exp_config, "", percentile_cols, "Local Txn Latency (ms)")

        axes[1].sharey(axes[0])

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
