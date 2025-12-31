# Result Processing

The `analysis` directory includes all the required scripts to process the obtained experimental results and plot.

## Setup and Configuration

Assuming you have already created a python virtual environment during GenuineDB setup, install the remaining required packages by running the command:

``` 
pip3 install -r analysis/requirements.txt
```

Then, configure the `analysis/result_processing.py` script by updating the following variables:
- `result_path` -  Path to where experimental results are written
- `tmp_path` - Path where experimental results can be temporarily moved to and processed
- `parquet_paths` - Path to write the statistical analysis results


## Running the script

After modifying the result processing script, run it in parallel with experiment execution, as experiments logs can strain storage space:

``` 
python3 result_processing.py -c -l
```

## Plotting the results

For each experiment, you can plot its designated graph by updating the "results paths" in each function in `analysis/graph_processing.py`.

Example below:
``` 
    experiments = {
        "GenuineDB": Experiment("results/microbenchmarks/mh", "GLOG", "#2ca02c",
                             "PARTIAL", "o"),

        "SLOG": Experiment("results/microbenchmarks/mh", "SLOG", "#1f77b4",
                           "PARTIAL", "s"),
        "Detock": Experiment("results/microbenchmarks/mh", "DETOCK", "#d62728",
                             "PARTIAL", "^"),
        "Tiga": Experiment("results/microbenchmarks/mh", "TIGA", "#E69F00",
                           "PARTIAL", "D"),
    }
```

We suggest that after running each experiment, the user should isolate the resulting statistics by moving the `parquet` files located in the previously defined `parquet_paths`. 