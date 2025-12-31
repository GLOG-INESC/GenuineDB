from common import apply_mask
import matplotlib.pyplot as plt
from matplotlib.lines import Line2D
import plot_generators
def row_column_graph(experiments, plot_name, output_path, plot_function, plot_function_args=None, mask=None, exp_mask=None,
                     row_axis="wl:mp", column_axis="wl:hot"):
    masked_dataframes = {}

    row_axis_values = set()
    column_axis_values = set()

    systems_legend = []

    if mask is None:
        mask = {}

    if exp_mask is None:
        exp_mask = {}

    # Dry run to obtain the values for rows and axis and obtain dataframes
    for e in experiments:
        if plot_function.df_name in experiments[e]:
            df = experiments[e][plot_function.df_name]
            systems_legend += [Line2D([0], [0], color=experiments[e]["config"].color, lw=2, label=e)]

            df = apply_mask(df, mask=mask)
            if e in exp_mask:
                df = apply_mask(df, exp_mask[e])
            if not df.empty:
                masked_dataframes[e] = df
                row_axis_values.update(set(df[row_axis].unique().tolist()))
                column_axis_values.update(set(df[column_axis].unique().tolist()))

    print(row_axis_values, column_axis_values)

    # fig, axes = plt.subplots(len(row_axis_values), len(column_axis_values), figsize=(5*len(column_axis_values), 3*len(row_axis_values)), sharey="all", sharex="all")
    fig, axes = plt.subplots(len(row_axis_values), len(column_axis_values),
                             figsize=(8 * len(column_axis_values), 3 * len(row_axis_values)), sharey="all",
                             sharex="all")

    row_axis_values = list(row_axis_values)
    row_axis_values.sort()
    column_axis_values = list(column_axis_values)
    column_axis_values.sort()

    # Real Plot
    for i, e in enumerate(experiments):

        exp_config = experiments[e]["config"]
        for r, row_df in enumerate(row_axis_values):

            for c, column_df in enumerate(column_axis_values):
                if e in masked_dataframes:
                    df = masked_dataframes[e][(masked_dataframes[e][row_axis] == row_df) & (
                                masked_dataframes[e][column_axis] == column_df)].copy()
                    if not df.empty:
                        print(e, row_df, column_df)
                        # Generalize this ugly mess later
                        if len(row_axis_values) > 1:
                            ax = axes[r]
                        else:
                            ax = axes

                        if len(column_axis_values) > 1:
                            ax = ax[c]
                        if plot_function_args is None:
                            plot_function(df, ax, exp_config, f"{row_axis}=={row_df}, {column_axis}=={column_df}")
                        else:
                            plot_function(df, ax, exp_config, f"{row_axis}=={row_df}, {column_axis}=={column_df}",
                                          *plot_function_args)

    fig.tight_layout()
    fig.legend(
        handles=systems_legend,
        loc='lower left',
        bbox_to_anchor=(0, 1, 1, 0),
        mode='expand',
        ncol=len(systems_legend)
    )
    fig.tight_layout()
    fig.savefig(f'{output_path}/{plot_name}.pdf', bbox_inches='tight')
    fig.savefig(f'{output_path}/{plot_name}.jpg', bbox_inches='tight')