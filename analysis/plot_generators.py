import pandas as pd

class PlotFunction:
    # Plot function class
    def __init__(self, function, required_df):
        self.plot_func = function
        self.df_name = required_df
        self.legend = []

    def __call__(self, df, axis, experiment, label, *args, **kwargs):
        return self.plot_func(df, axis, experiment, label, *args, **kwargs)


def plot_peak_tput(df, ax, exp, label, x_axis, threshold):
    assert "tput" in df.columns, f"Dataframe must have tput column"
    assert "clients" in df.columns, f"Dataframe must have clients column"
    assert x_axis in df.columns, f"Dataframe must have {x_axis} column"

    tput_column = "tput"
    if "total_tput" in df.columns:
        tput_column = "total_tput"

    df[tput_column] = df[tput_column].apply(lambda x: x/ 1000)

    x_axis_values = df[x_axis].unique().tolist()
    x_axis_values.sort()

    final_df_schema = {
        tput_column: pd.Series(dtype='double'),
        x_axis: pd.Series(dtype='int')
    }

    final_df = pd.DataFrame(final_df_schema)

    for x_value in x_axis_values:
        x_df = df[df[x_axis] == x_value]

        clients = x_df["clients"].unique().tolist()
        clients.sort()

        tput = None
        prev_row = None
        for i, num_clients in enumerate(clients):
            client_df = x_df[x_df["clients"] == num_clients]
            if len(client_df) > 1:
                print("Warning: more than an experiment detected. Averaging out")


            # Throughput should remain as the first entry in the map
            row = []
            for column in final_df_schema:
                row.append(client_df[column].mean())

            # Define a threshold to stop increase
            if tput is None:
                tput = row[0]
                prev_row = row
            else:
                # Use this value for final graph
                if row[0] < tput or row[0]/tput <= threshold:
                    final_df.loc[len(final_df)] = prev_row
                    break
                else:
                    tput = row[0]
                    prev_row = row
            # If by the end of the loop we have not find the peak tput,
            # insert the last row
            if i == len(clients)-1:
                final_df.loc[len(final_df)] = row

    # After obtaining all rows, plot them

    # Tput plot
    final_df.plot(
        ax=ax,
        x=x_axis,
        y=tput_column,
        label=label,
        legend=False,
        color=exp.color,
        marker="."
    )

    ax.set_xlabel(x_axis)
    ax.set_ylabel("Throughput (kTxn/s)")
    ax.set_title(f"{label} Peak Tput")


def plot_peak_tput_lantency(df, ax, exp, label, x_axis, threshold, percentile_cols):
    assert "tput" in df.columns, f"Dataframe must have tput column"

    assert "clients" in df.columns, f"Dataframe must have clients column"
    assert x_axis in df.columns, f"Dataframe must have {x_axis} column"

    tput_column = "tput"
    if "total_tput" in df.columns:
        tput_column = "total_tput"

    df[tput_column] = df[tput_column].apply(lambda x: x/ 1000)

    x_axis_values = df[x_axis].unique().tolist()
    x_axis_values.sort()

    final_df_schema = {
        tput_column: pd.Series(dtype='double'),
        x_axis: pd.Series(dtype='int')
    }

    for percentile, _ in percentile_cols.values():
        final_df_schema[f"p{int(percentile*100)}"] = pd.Series(dtype='double')

    final_df = pd.DataFrame(final_df_schema)
    for x_value in x_axis_values:
        x_df = df[df[x_axis] == x_value]

        clients = x_df["clients"].unique().tolist()
        clients.sort()

        tput = None
        prev_row = None

        for i, num_clients in enumerate(clients):
            client_df = x_df[x_df["clients"] == num_clients]
            if len(client_df) > 1:
                print("Warning: more than an experiment detected")

            # Throughput should remain as the first entry in the map
            row = []
            for column in final_df_schema:
                row.append(client_df[column].iloc[0])

            # Define a threshold to stop increase
            if tput is None:
                tput = row[0]
                prev_row = row
            else:
                print(f"{tput} vs {row[0]}")
                # Use this value for final graph
                if row[0] < tput or row[0]/tput <= threshold:
                    final_df.loc[len(final_df)] = prev_row
                    break
                else:
                    tput = row[0]
                    prev_row = row

            # If by the end of the loop we have not find the peak tput,
            # insert the last row
            if i == len(clients)-1:
                final_df.loc[len(final_df)] = row

    # After obtaining all rows, plot them

    for p, line_style in percentile_cols.values():
        final_df.plot(
            ax=ax,
            x=x_axis,
            y=f'p{int(p * 100)}',
            marker=".",
            linestyle=line_style,
            legend=False,
            color=exp.color
        )

    ax.set_xlabel(x_axis)
    ax.set_ylabel("Latency (ms)")
    ax.set_title(f"{label} Peak Tput")


def plot_cdf(df, ax, exp, label, op_type):
    assert "cdf" in df.columns, f"Dataframe must have cdf column"

    df = df[df["op_type"] == op_type]
    sorted_df = df.sort_values(by="cdf")

    sorted_df.plot(x='latency', y='cdf', ax=ax, label=exp.label, color=exp.color, legend=False)

    ax.set_title(label)
    ax.set_ylabel(f"CDF")
    ax.set_xlabel(f"Latency (ms)")
    ax.xaxis.set_tick_params(which='both', labelbottom=True)
    ax.yaxis.set_tick_params(which='both', labelleft=True)


def plot_client_tput(df, ax, exp, label):
    assert "tput" in df.columns, f"Dataframe must have tput column"
    assert "clients" in df.columns, f"Dataframe must have clients column"

    df["tput"] = df["tput"].apply(lambda x: x/ 1000)


    n_rows = df.shape[0]

    print(df[["exp_name", "clients", "tput", "p50", "p99"]])

    avg_df = df.groupby('clients', as_index=False)['tput'].mean()

    if avg_df.shape[0] != n_rows:
        print(f"Warning: Experiment {exp.label} with tag {label} had more than the expected rows")


    df = avg_df.sort_values(by="clients")
    df.plot(
        ax=ax,
        x="clients",
        y="tput",
        marker=".",
        legend=False,
        color=exp.color
    )

    ax.set_title(label)
    #ax.set_yscale("log")
    ax.set_ylabel(f"Tput (kTxns / s)")
    ax.set_xlabel(f"Clients")
    ax.xaxis.set_tick_params(which='both', labelbottom=True)
    ax.yaxis.set_tick_params(which='both', labelleft=True)

def plot_jitter_tput(df, ax, exp, label):
    assert "tput" in df.columns, f"Dataframe must have tput column"
    assert "jitter" in df.columns, f"Dataframe must have clients column"

    df["tput"] = df["tput"].apply(lambda x: x/ 1000)


    n_rows = df.shape[0]

    avg_df = df.groupby(['clients', 'jitter'], as_index=False)['tput'].mean()

    if avg_df.shape[0] != n_rows:
        print(f"Warning: Experiment {exp.label} with tag {label} had more than the expected rows")
        #print(df)


    df = avg_df.sort_values(by="jitter")
    df.plot(
        ax=ax,
        x="jitter",
        y="tput",
        marker=".",
        legend=False,
        color=exp.color
    )

    ax.set_title(label)
    #ax.set_yscale("log")
    ax.set_ylabel(f"Tput (kTxns / s)")
    ax.set_xlabel(f"Jitter")
    ax.xaxis.set_tick_params(which='both', labelbottom=True)
    ax.yaxis.set_tick_params(which='both', labelleft=True)

def deadlocks_number(df, ax, column="clients"):

    df = df[df["replica"] == 0]
    df = df.groupby(column).size().reset_index(name="count")

    df.plot(x=column, y="count", kind="bar", legend=False, ax=ax)

    ax.set_xlabel(column)
    ax.set_ylabel("Deadlock Occurrences")
    ax.set_title("Number of Deadlocks")



def deadlocks_size(df, ax, column="clients"):
    df = df[df["replica"] == 0]

    df.boxplot(column="vertices", by=column, grid=False, ax=ax, whis=[0, 99])

    ax.set_xlabel(column)
    ax.set_ylabel("Transactions in Deadlocks")
    ax.set_title("Size of Deadlocks")

def plot_latency_tput(df, ax, exp, label, percentile_cols, ylabel="Latency (ms)"):
    assert "total_tput" in df.columns, f"Tput column does not exist in dataframe of experiment {exp.label}"
    df = df.sort_values(by="clients")

    #print(df)

    df = df.groupby(['clients'], as_index=False).mean(numeric_only=True)

    df["total_tput"] = df["total_tput"] / 1000

    x_axis_values = df["clients"].unique().tolist()
    x_axis_values.sort()

    final_df_schema = {
        "total_tput": pd.Series(dtype='double'),
        "clients": pd.Series(dtype='int')
    }

    for percentile, _ in percentile_cols.values():
        final_df_schema[f"p{int(percentile * 100)}"] = pd.Series(dtype='double')

    tput = None
    prev_row = None
    final_df = pd.DataFrame(final_df_schema)

    broke = False
    print(f"{exp.label}")
    for client in x_axis_values:
        client_df = df[df["clients"] == client]
        if len(client_df) > 1:
            print("Warning: more than an experiment detected")

        # Throughput should remain as the first entry in the map
        row = []
        for column in final_df_schema:
            row.append(client_df[column].iloc[0])

        print(row)
        if tput is None:
            tput = row[0]
            prev_row = row
        else:
            if row[0] < tput or row[0] / tput < 1.01:
                #print(f"{exp.label} peak: {prev_row}")
                final_df.loc[len(final_df)] = row

                if not broke:
                    print(f"{exp.label} peak: {prev_row}")

                broke = True

                #break
            else:
                tput = row[0]
                prev_row = row
        final_df.loc[len(final_df)] = row

    for p in percentile_cols:
        final_df.plot(
            ax=ax,
            x="total_tput",
            y=f'p{int(percentile_cols[p][0] * 100)}',
            label=p,
            marker=exp.marker,
            linestyle=percentile_cols[p][1],
            legend=False,
            color=exp.color
        )

    if label:
        ax.set_title(label)
    ax.set_ylabel(ylabel)
    ax.set_xlabel(f"Throughput (kTxns / s)")
    ax.xaxis.set_tick_params(which='both', labelbottom=True)
    ax.yaxis.set_tick_params(which='both', labelleft=True)

def plot_zipf_latency_peak_tput(df, ax, exp, label, percentile_cols, y_label="Latency (ms)"):
    assert "total_tput" in df.columns, f"Tput column does not exist in dataframe of experiment {exp.label}"

    df["total_tput"] = df["total_tput"] / 1000

    zipf_values = df["wl:zipf"].unique().tolist()
    zipf_values.sort()

    final_df_schema = {
        "total_tput": pd.Series(dtype='double'),
        "wl:zipf": pd.Series(dtype='double'),
        "clients": pd.Series(dtype='int')
    }
    averaging_columns = ["total_tput"]

    for percentile, _ in percentile_cols.values():
        perc = f"p{int(percentile * 100)}"
        final_df_schema[perc] = pd.Series(dtype='double')
        averaging_columns.append(perc)

    final_df = pd.DataFrame(final_df_schema)

    for zipf in zipf_values:
        zipf_df = df[df["wl:zipf"] == zipf]
        x_axis_values = zipf_df["clients"].unique().tolist()
        x_axis_values.sort()
        tput = None
        prev_row = None
        broke = False
        for client in x_axis_values:
            client_df = zipf_df[zipf_df["clients"] == client]
            if len(client_df) > 1:
                print("Warning: more than an experiment detected in LATENCY")
                print("Making the average between them")
                client_df = client_df[averaging_columns].mean().to_frame().T
                client_df["clients"] = client
                client_df["wl:zipf"] = zipf

            # Throughput should remain as the first entry in the map
            row = []
            for column in final_df_schema:
                row.append(client_df[column].iloc[0])

            if tput is None:
                tput = row[0]
                prev_row = row
            else:
                if row[0] < tput or row[0] / tput < 1.01:
                    print(f"{exp.label} peak: {prev_row}")

                    broke = True
                    break
                else:
                    tput = row[0]
                    prev_row = row

        if not broke:
            print(f"{exp.label} peak: {prev_row}")

        final_df.loc[len(final_df)] = prev_row

    for p in percentile_cols:
        final_df.plot(
            ax=ax,
            x="wl:zipf",
            y=f'p{int(percentile_cols[p][0] * 100)}',
            label=p,
            marker=exp.marker,
            linestyle=percentile_cols[p][1],
            legend=False,
            color=exp.color
        )

    if label:
        ax.set_title(label)
    ax.set_ylabel(y_label)
    ax.set_xlabel(f"Zipf")
    ax.xaxis.set_tick_params(which='both', labelbottom=True)
    ax.yaxis.set_tick_params(which='both', labelleft=True)


def plot_zipf_peak_tput(df, ax, exp, label):
    assert "tput" in df.columns, f"Tput column does not exist in dataframe of experiment {exp.label}"

    df["tput"] = df["tput"] / 1000

    zipf_values = df["wl:zipf"].unique().tolist()
    zipf_values.sort()

    final_df_schema = {
        "tput": pd.Series(dtype='double'),
        "wl:zipf": pd.Series(dtype='double'),
        "clients" : pd.Series(dtype='int')
    }

    final_df = pd.DataFrame(final_df_schema)

    for zipf in zipf_values:
        zipf_df = df[df["wl:zipf"] == zipf]

        x_axis_values = zipf_df["clients"].unique().tolist()
        x_axis_values.sort()
        tput = None
        prev_row = None
        broke = False
        for client in x_axis_values:
            client_df = zipf_df[zipf_df["clients"] == client]
            client_df = client_df.mean(numeric_only=True).to_frame().T

            if len(client_df) > 1:
                print("Warning: more than an experiment detected in TPUT")
            # Throughput should remain as the first entry in the map
            row = []
            for column in final_df_schema:
                row.append(client_df[column].iloc[0])

            if tput is None:
                tput = row[0]
                prev_row = row
            else:
                if row[0] < tput or row[0] / tput < 1.0:
                    print(f"{exp.label} peak: {prev_row}")
                    broke = True
                    break
                else:
                    tput = row[0]
                    prev_row = row

        if not broke:
            print(f"{exp.label} peak: {prev_row}")

        final_df.loc[len(final_df)] = prev_row

    final_df.plot(
        ax=ax,
        x="wl:zipf",
        y=f'tput',
        label=exp.label,
        marker=exp.marker,
        linestyle="-",
        legend=False,
        color=exp.color
    )

    if label:
        ax.set_title(label)
    ax.set_ylabel(f"Throughput (kTxns / s)")
    ax.set_xlabel(f"Zipf")
    ax.xaxis.set_tick_params(which='both', labelbottom=True)
    ax.yaxis.set_tick_params(which='both', labelleft=True)

def plot_jitter_peak_tput(df, ax, exp, label):
    assert "tput" in df.columns, f"Tput column does not exist in dataframe of experiment {exp.label}"

    df["tput"] = df["tput"] / 1000

    jitter_values = df["jitter"].unique().tolist()
    jitter_values.sort()

    final_df_schema = {
        "tput": pd.Series(dtype='double'),
        "jitter": pd.Series(dtype='double'),
        "clients" : pd.Series(dtype='int')
    }

    final_df = pd.DataFrame(final_df_schema)

    for jitter in jitter_values:
        jitter_df = df[df["jitter"] == jitter]

        x_axis_values = jitter_df["clients"].unique().tolist()
        x_axis_values.sort()
        tput = None
        prev_row = None
        broke = False
        for client in x_axis_values:
            client_df = jitter_df[jitter_df["clients"] == client]
            if len(client_df) > 1:
                print("Warning: more than an experiment detected in TPUT")
            # Throughput should remain as the first entry in the map
            row = []
            for column in final_df_schema:
                row.append(client_df[column].iloc[0])

            if tput is None:
                tput = row[0]
                prev_row = row
            else:
                if row[0] < tput or row[0] / tput < 1.0:
                    print(f"{exp.label} peak: {prev_row}")
                    broke = True
                    break
                else:
                    tput = row[0]
                    prev_row = row

        if not broke:
            print(f"{exp.label} peak: {prev_row}")

        final_df.loc[len(final_df)] = prev_row

    final_df.plot(
        ax=ax,
        x="jitter",
        y=f'tput',
        label=exp.label,
        marker=".",
        linestyle="-",
        legend=False,
        color=exp.color
    )


    ax.set_title(label)
    ax.set_ylabel(f"Throughput (kTxns / s)")
    ax.set_xlabel(f"Jitter")
    ax.xaxis.set_tick_params(which='both', labelbottom=True)
    ax.yaxis.set_tick_params(which='both', labelleft=True)

def plot_jitter_latency_peak_tput(df, ax, exp, label, percentile_cols):
    assert "total_tput" in df.columns, f"Tput column does not exist in dataframe of experiment {exp.label}"

    df["total_tput"] = df["total_tput"] / 1000

    jitter_values = df["jitter"].unique().tolist()
    jitter_values.sort()

    final_df_schema = {
        "total_tput": pd.Series(dtype='double'),
        "jitter": pd.Series(dtype='double'),
        "clients": pd.Series(dtype='int')
    }
    for percentile, _ in percentile_cols.values():
        final_df_schema[f"p{int(percentile * 100)}"] = pd.Series(dtype='double')

    final_df = pd.DataFrame(final_df_schema)

    for jitter in jitter_values:
        jitter_df = df[df["jitter"] == jitter]
        x_axis_values = jitter_df["clients"].unique().tolist()
        x_axis_values.sort()
        tput = None
        prev_row = None
        broke = False
        for client in x_axis_values:
            client_df = jitter_df[jitter_df["clients"] == client]
            if len(client_df) > 1:
                print("Warning: more than an experiment detected in LATENCY")
                print(client_df)
                raise Exception()

            # Throughput should remain as the first entry in the map
            row = []
            for column in final_df_schema:
                row.append(client_df[column].iloc[0])

            if tput is None:
                tput = row[0]
                prev_row = row
            else:
                if row[0] < tput or row[0] / tput < 1.05:
                    print(f"{exp.label} peak: {prev_row}")

                    broke = True
                    break
                else:
                    tput = row[0]
                    prev_row = row

        if not broke:
            print(f"{exp.label} peak: {prev_row}")

        final_df.loc[len(final_df)] = prev_row

    for p in percentile_cols:
        final_df.plot(
            ax=ax,
            x="jitter",
            y=f'p{int(percentile_cols[p][0] * 100)}',
            label=p,
            marker=".",
            linestyle=percentile_cols[p][1],
            legend=False,
            color=exp.color
        )

    ax.set_title(label)
    ax.set_ylabel(f"Latency (ms)")
    ax.set_xlabel(f"Jitter")
    ax.xaxis.set_tick_params(which='both', labelbottom=True)
    ax.yaxis.set_tick_params(which='both', labelleft=True)


def plot_latency_clients(df, ax, exp, label, percentile_cols):
    df = df.sort_values(by="clients")

    df = df.drop_duplicates(subset="clients", keep="first")
    for p in percentile_cols:
        df.plot(
            ax=ax,
            x="clients",
            y=f'p{int(percentile_cols[p][0] * 100)}',
            label=p,
            marker=".",
            linestyle=percentile_cols[p][1],
            legend=False,
            color=exp.color
        )

    ax.set_title(label)
    #ax.set_yscale("log")
    ax.set_ylabel(f"Latency (ms)")
    ax.set_xlabel(f"Clients per Region")
    ax.xaxis.set_tick_params(which='both', labelbottom=True)
    ax.yaxis.set_tick_params(which='both', labelleft=True)

def plot_peak_tput_per_value(df, ax, exp, label, column):
    assert "tput" in df.columns, f"Tput column does not exist in dataframe of experiment {exp.label}"
    assert column in df.columns, f"{column} column does not exist in dataframe of experiment {exp.label}"

    df["tput"] = df["tput"] / 1000

    column_values = df[column].unique().tolist()
    column_values.sort()

    final_df_schema = {
        "tput": pd.Series(dtype='double'),
        column: pd.Series(dtype='double'),
        "clients" : pd.Series(dtype='int')
    }

    final_df = pd.DataFrame(final_df_schema)

    for v in column_values:
        column_df = df[df[column] == v]


        x_axis_values = column_df["clients"].unique().tolist()
        x_axis_values.sort()
        tput = None
        prev_row = None
        broke = False

        for client in x_axis_values:
            client_df = column_df[column_df["clients"] == client]
            if len(client_df) > 1:
                print("Warning: more than an experiment detected in TPUT")
            # Throughput should remain as the first entry in the map
            row = []
            for c in final_df_schema:
                row.append(client_df[c].iloc[0])

            if tput is None:
                tput = row[0]
                prev_row = row
            else:
                if row[0] < tput or row[0] / tput < 1.0:
                    print(f"{exp.label} peak: {prev_row}")
                    broke = True
                    break
                else:
                    tput = row[0]
                    prev_row = row

        if not broke:
            print(f"{exp.label} peak: {prev_row}")

        final_df.loc[len(final_df)] = prev_row


    final_df.plot(
        ax=ax,
        x=column,
        y=f'tput',
        label=exp.label,
        marker=exp.marker,
        linestyle="-",
        legend=False,
        color=exp.color
    )


    ax.set_title(label)
    ax.set_ylabel(f"Throughput (kTxns / s)")
    ax.set_xlabel(f"{column}")
    ax.xaxis.set_tick_params(which='both', labelbottom=True)
    ax.yaxis.set_tick_params(which='both', labelleft=True)

def plot_peak_tput_per_value_normalized_bar(df, ax, exp, label, column):
    assert "tput" in df.columns, f"Tput column does not exist in dataframe of experiment {exp.label}"
    assert column in df.columns, f"{column} column does not exist in dataframe of experiment {exp.label}"

    df["tput"] = df["tput"] / 1000

    column_values = df[column].unique().tolist()
    column_values.sort()

    final_df_schema = {
        "tput": pd.Series(dtype='double'),
        column: pd.Series(dtype='double'),
        "clients" : pd.Series(dtype='int')
    }

    final_df = pd.DataFrame(final_df_schema)

    base_value = None
    for i, v in enumerate(column_values):
        column_df = df[df[column] == v]


        x_axis_values = column_df["clients"].unique().tolist()
        x_axis_values.sort()
        tput = None
        prev_row = None
        broke = False

        for client in x_axis_values:
            client_df = column_df[column_df["clients"] == client]
            if len(client_df) > 1:
                print("Warning: more than an experiment detected in TPUT")
            # Throughput should remain as the first entry in the map
            row = []
            for c in final_df_schema:
                row.append(client_df[c].iloc[0])

            if tput is None:
                tput = row[0]
                prev_row = row
            else:
                if row[0] < tput or row[0] / tput < 1.0:
                    print(f"{exp.label} peak: {prev_row}")
                    broke = True
                    break
                else:
                    tput = row[0]
                    prev_row = row

        if not broke:
            print(f"{exp.label} peak: {prev_row}")

        if i == 0:
            base_value = prev_row[0]

        #Normalize the value to the base one
        prev_row[0] = prev_row[0] / base_value
        final_df.loc[len(final_df)] = prev_row

    final_df.plot(
        ax=ax,
        x=column,
        y=f'tput',
        label=exp.label,
        kind='bar',
        legend=False,
        color=exp.color
    )


    ax.set_title(label)
    ax.set_ylabel(f"Throughput (kTxns / s)")
    ax.set_xlabel(f"{column}")
    ax.xaxis.set_tick_params(which='both', labelbottom=True)
    ax.yaxis.set_tick_params(which='both', labelleft=True)

def plot_latency_peak_tput_per_value(df, ax, exp, label, column, percentile_cols, ylabel="Latency (ms)"):
    assert "total_tput" in df.columns, f"Tput column does not exist in dataframe of experiment {exp.label}"
    assert column in df.columns, f"{column} column does not exist in dataframe of experiment {exp.label}"

    df["total_tput"] = df["total_tput"] / 1000

    column_values = df[column].unique().tolist()
    column_values.sort()

    final_df_schema = {
        "tput": pd.Series(dtype='double'),
        column: pd.Series(dtype='double'),
        "clients": pd.Series(dtype='int')
    }

    for percentile, _ in percentile_cols.values():
        final_df_schema[f"p{int(percentile * 100)}"] = pd.Series(dtype='double')

    final_df = pd.DataFrame(final_df_schema)

    for v in column_values:
        column_df = df[df[column] == v]

        x_axis_values = column_df["clients"].unique().tolist()
        x_axis_values.sort()
        tput = None
        prev_row = None
        broke = False
        for client in x_axis_values:
            client_df = column_df[column_df["clients"] == client]
            client_df = client_df.mean(numeric_only=True).to_frame().T
            if len(client_df) > 1:
                print("Warning: more than an experiment detected in LATENCY")
                print(client_df)
                raise Exception()

            if len(client_df) == 0:
                print(f"Warning: no clients {client} found in dataframe")
                print(client_df)
                raise Exception()

            # Throughput should remain as the first entry in the map
            row = []
            for c in final_df_schema:
                row.append(client_df[c].iloc[0])

            if tput is None:
                tput = row[0]
                prev_row = row
            else:
                if row[0] < tput or row[0] / tput < 1.0:
                    print(f"Found {exp.label} peak: {prev_row}")

                    broke = True
                    break
                else:
                    tput = row[0]
                    prev_row = row

        if not broke:
            print(f"Final {exp.label} peak: {prev_row}")

        final_df.loc[len(final_df)] = prev_row

    for p in percentile_cols:
        final_df.plot(
            ax=ax,
            x=column,
            y=f'p{int(percentile_cols[p][0] * 100)}',
            label=p,
            marker=exp.marker,
            linestyle=percentile_cols[p][1],
            legend=False,
            color=exp.color
        )

    ax.set_title(label)
    ax.set_ylabel(ylabel)
    ax.set_xlabel(f"{column}")
    ax.xaxis.set_tick_params(which='both', labelbottom=True)
    ax.yaxis.set_tick_params(which='both', labelleft=True)
