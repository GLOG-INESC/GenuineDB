import json
import subprocess
import argparse
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Obtains the list of machines of current job")

    parser.add_argument(
        "--user", "-u", help="Username in Emulab"
    )

    args = parser.parse_args()

    job_info_txt = subprocess.run(f"oarstat -u {args.user} -J", shell=True, capture_output=True, text=True)

    # Parse output as JSON
    try:
        job_info_json = json.loads(job_info_txt.stdout)
    except json.JSONDecodeError as e:
        print("Failed to parse JSON:", e)
        exit(-1)

    assert len(job_info_json) == 1, "Assuming only a single job exists in the json"
    job_id = list(job_info_json.keys())[0]

    host_machines = job_info_json[job_id]["assigned_network_address"]

    print(host_machines)