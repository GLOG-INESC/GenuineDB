from common import apply_mask
import matplotlib.pyplot as plt
from matplotlib.lines import Line2D
import plot_generators


def clock_schew_cdf_graph(experiments, plot_name, output_path, mask=None, exp_mask=None):

    if mask is None:
        mask = {}

    if exp_mask is None:
        exp_mask = {}


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

    regions = set()
    systems_legend = []
    exp_linestyles = ["-", "--", "-."]

    fig, axes = plt.subplots(2, 4, figsize=(3.5 * 4, 2.6 * 2), sharey="all", sharex="all")

    for i, e in enumerate(experiments):
        assert "per_region_cdf" in experiments[e], "Graph requires per_type_latency quantile cdf"

        local_df = apply_mask(experiments[e]["per_region_cdf"], mask=mask)
        if e in exp_mask:
            local_df = apply_mask(local_df, exp_mask[e])

        if local_df.empty:
            continue

        exp_config = experiments[e]["config"]
        systems_legend += [Line2D([0], [0], color=exp_config.color, lw=2, label=e, linestyle=exp_linestyles[i])]

        for r in range(8):

            ax = axes[r // 4][r % 4]
            region_df = local_df[local_df["region"] == r]

            region_df_sorted = region_df.sort_values(by='cdf')

            region_df_sorted.plot(x='latency', y='cdf', ax=ax, label=e, color=exp_config.color, legend=False)


    for r in range(8):
        axes[r // 4][r % 4].set_xlabel("Global Txn Latency (ms)")
        axes[r // 4][r % 4].set_ylabel("CDF")
        axes[r // 4][r % 4].set_title(f"{region_maping[r]}")
        axes[r // 4][r % 4].set_xlim(left=0)
        axes[r // 4][r % 4].set_ylim(bottom=0)
        axes[r // 4][r % 4].tick_params(labelbottom=True, labelleft=True)

    fig.legend(
        handles=systems_legend,
        loc='lower left',
        bbox_to_anchor=(0, 1, 1, 0),
        mode='expand',
        ncol=3,
        fontsize=10
    )

    fig.tight_layout()
    fig.savefig(f'{output_path}/{plot_name}.pdf', bbox_inches='tight')
    fig.savefig(f'{output_path}/{plot_name}.jpg', bbox_inches='tight')