import common
import matplotlib
import matplotlib.pyplot as plt
from pyspark.sql.functions import col, lit
from functools import reduce



def deadlock_per_hot_graph(deadlocks_df, label, output_path, x_axis, hots):
    fig, axes = plt.subplots(2, len(hots), figsize=(22, 15))

    for i, hot in enumerate(hots):

        hot_deadlocks_df = deadlocks_df[deadlocks_df["wl:hot"] == hot]

        if not hot_deadlocks_df.empty:
            # Get Number of deadlocks

            grouped_x_axis = hot_deadlocks_df.groupby(x_axis).size().reset_index(name="count")
            if len(hots) > 1:
                plot_ax = axes[0][i]
            else:
                plot_ax = axes[0]

            grouped_x_axis.plot(x=x_axis, y="count", kind="bar", legend=False, ax=plot_ax)
            plot_ax.set_xlabel("Clients")
            plot_ax.set_title("Number of Deadlocks")
            plot_ax.set_ylabel("Deadlocks")

            if len(hots) > 1:
                vertices_ax = axes[1][i]
            else:
                vertices_ax = axes[1]

            hot_deadlocks_df.boxplot(column="vertices", by=x_axis, grid=False, ax=vertices_ax, whis=[0, 99])
            vertices_ax.set_yscale("log")
            vertices_ax.set_xlabel("Clients")
            vertices_ax.set_ylabel("Size of Deadlocks")
            vertices_ax.set_title("Size of Deadlocks")

        if len(hots) > 1:
            ax = axes[0][0]
        else:
            ax = axes[0]

        handles, labels = ax.get_legend_handles_labels()

        fig.legend(handles, labels, bbox_to_anchor=(0, 1, 1, 0), loc='lower left', mode='expand', ncol=1)
        fig.savefig(f'{output_path}/{label}_deadlocks.pdf', bbox_inches='tight')
        fig.savefig(f'{output_path}/{label}_deadlocks.pdf', bbox_inches='tight')

def detock_deadlock_graph(experiments, graph_config, x_axis, hots=[]):
    common.set_plot_style()

    for prefix, experiment in experiments.items():

        if "detock" not in experiment.label.lower():
            continue

        index_df = common.from_cache_or_compute(
            f'{prefix}/index_{experiment.label}.parquet',
            lambda: common.get_index(graph_config.spark, prefix).toPandas().convert_dtypes().astype({
                "wl:hot": "int32",
                "wl:mh": "int32",
                "wl:mp": "int32",
                "clients": "int32"
            }),
            ignore_cache=graph_config.IGNORE_INDEX_CACHE,
        )
        index_df


        deadlocks_df = common.from_cache_or_compute(
            f'{prefix}/deadlocks_{experiment.label}.parquet',
            lambda: deadlocks_dfs(graph_config.spark, index_df["prefix"]) \
                .toPandas() \
                .merge(index_df, on="prefix"),
            ignore_cache = graph_config.IGNORE_RESULT_CACHE)

        if experiment.mask is not None:
            for mask, values in experiment.mask.items():
                for i, value in enumerate(values):
                    deadlock_mask_df = deadlocks_df[deadlocks_df[mask]==value]
                    label = f"{experiment.label}_{value}"

                    deadlock_per_hot_graph(deadlock_mask_df, label, graph_config.output_path, x_axis, hots)

        else:
            deadlock_per_hot_graph(deadlocks_df, experiment.label, graph_config.output_path, x_axis, hots)