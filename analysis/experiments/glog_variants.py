from common import apply_mask
import matplotlib.pyplot as plt
from matplotlib.lines import Line2D
import plot_generators

def mh_latency_cdf(experiments, output_path, exp_mask, client_mask):
    mh_experiments = {}


    systems_legend = []
    exp_linestyles = ["-", "--", "-."]

    for i, e in enumerate(experiments):
        systems_legend += [Line2D([0], [0], color=experiments[e]["config"].color, lw=2, label=e, linestyle=exp_linestyles[i])]

        assert "per_type_cdf" in experiments[e], "Dataframes must include the per_type cdf"

        exp_mask["clients"] = client_mask[e]
        df = apply_mask(experiments[e]["per_type_cdf"], mask=exp_mask)

        if not df.empty:

            mh_df = df[df["txn_type"] == "mh"]
            if not mh_df.empty:
                mh_experiments[e] = mh_df




    def plot_cdf(df, ax, e, r, line, color):

        region_df = df[df["region"] == r]

        region_df_sorted = region_df.sort_values(by='cdf')

        region_df_sorted.plot(x='latency', y='cdf', ax=ax, label=e, color=color, linestyle=line, legend=False)



    fig, axes = plt.subplots(1, 1, figsize=(3.5, 2.6), sharey="all", sharex="all")

    for i, e in enumerate(experiments):
        mh_df = mh_experiments[e]
        mh_df_sorted = mh_df.sort_values(by='cdf')
        exp_config = experiments[e]["config"]
        mh_df_sorted.plot(x='latency', y='cdf', ax=axes, label=e, color=exp_config.color, linestyle=exp_linestyles[i], legend=False)

    axes.set_xlabel("Global Transaction Latency (ms)")
    axes.set_ylabel("CDF")

    axes.set_xlim(left=0)
    axes.set_ylim(bottom=0)
    axes.tick_params(labelbottom=True, labelleft=True)

    fig.legend(
        handles=systems_legend,
        loc='lower left',
        bbox_to_anchor=(0, 1, 1, 0),
        mode='expand',
        ncol=2,
        fontsize=11
    )

    label = ["local_txn_cdf_all", "global_txn_cdf_all"][1]
    fig.tight_layout()
    fig.savefig(f'{output_path}/{label}.pdf', bbox_inches='tight')
    fig.savefig(f'{output_path}/{label}.jpg', bbox_inches='tight')
