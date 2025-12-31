from common import apply_mask
import matplotlib.pyplot as plt
from matplotlib.lines import Line2D
import plot_generators
import pandas as pd
from matplotlib.ticker import MultipleLocator

def costs_txt(experiments, start_offset, duration):

    for e in experiments:
        assert "stub_goodput" in experiments[e], "Experiments must have Stub Goodput information"
        assert "stub_queue" in experiments[e], "Experiments must have Stub Queue information"

        goodput_parquet = experiments[e]["stub_goodput"]
        queue_parquet = experiments[e]["stub_queue"]

        filtered_goodput_parquet = goodput_parquet[goodput_parquet["start_timestamp"].between(start_offset, start_offset+duration)]

        avg_queue_time = queue_parquet["avg_latency"][0]
        total_goodput = filtered_goodput_parquet["goodput"].sum()
        total_badput = filtered_goodput_parquet["badput"].sum()

        total_region_slots_used = filtered_goodput_parquet["region_slot_used"].sum()
        total_region_slots_noop = filtered_goodput_parquet["region_slot_null"].sum()

        total_superposition_slots_used = filtered_goodput_parquet["superposition_slot_used"].sum()
        total_superposition_slots_noop = filtered_goodput_parquet["superposition_slot_null"].sum()


        print(f"{e} : Goodput: {total_goodput}, Badput: {total_badput}, Percentage: {total_badput/(total_goodput+total_badput)}")
        print(f"      Sups_GPUT: {total_superposition_slots_used}, Sups_BPUT: {total_superposition_slots_noop}, Percentage : {total_superposition_slots_noop / (total_superposition_slots_noop+total_superposition_slots_used)}")
        print(f"      Entries_GPUT: {total_region_slots_used}, Entries_BPUT: {total_region_slots_noop}, Percentage : {total_region_slots_noop / (total_region_slots_noop+total_region_slots_used)}")
        print(f"      Queue Time: {avg_queue_time}")

def costs_graph(plot_name, output_path, config):

    """data = [
        {"ratio": 0.08374556595135464, "queue": 489506, "rate": 1000},
        {"ratio": 0.03554377630660628, "queue": 982149, "rate": 500},
        {"ratio": 0.01189842972708362, "queue": 1973959, "rate": 250},
        {"ratio": 0.002633350830023109, "queue": 3941118, "rate": 125},
    ]"""

    data = [
        {"ratio": 0.001841268217691394, "queue": 4967697, "rate": 100},
        {"ratio": 0.008502584092957784, "queue": 2455772, "rate": 200},
        {"ratio": 0.02768845332367614,  "queue": 1233574, "rate": 400},
        {"ratio": 0.09099464189993346,  "queue": 493795,  "rate": 1000}]

    df = pd.DataFrame(data)

    # Latency in milliseconds
    df["queue"] = df["queue"] / 1000000
    df["ratio"] = df["ratio"] * 100


    fig, ax = plt.subplots(1, 1, figsize=(3.5, 2.6), sharex="all")

    x_value = 2455772/1000000
    y_value = 0.008502584092957784 * 100

    plt.vlines(x=x_value, color='red', linestyle='--', linewidth=1, ymin=0, ymax=y_value)
    plt.hlines(y=y_value, color='red', linestyle='--', linewidth=1, xmin=0, xmax=x_value)


    df.plot(
        ax=ax,
        x="queue",
        y="ratio",
        linestyle="-",
        legend=False,
        marker="o",
        color="#2ca02c"
    )

    systems_legend = [
        Line2D([0], [0], color=config.color, lw=2, label="GenuineDB", marker=config.marker)]

    fig.legend(
        handles=systems_legend,
        loc='lower left',
        bbox_to_anchor=(0, 1, 1, 0),
        mode='expand',
        ncol=3,
        fontsize=11
    )

    ax.set_ylabel("SKIP Network Overhead (%)")
    ax.set_xlabel("Stub Queueing Delay (ms)")
    ax.set_xlim(left=0)
    ax.set_ylim(bottom=0)
    #ax.yaxis.set_major_locator(MultipleLocator(10))
    fig.tight_layout()
    fig.savefig(f'{output_path}/{plot_name}.pdf', bbox_inches='tight')
    fig.savefig(f'{output_path}/{plot_name}.jpg', bbox_inches='tight')