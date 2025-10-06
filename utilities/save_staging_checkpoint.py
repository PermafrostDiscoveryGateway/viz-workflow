# 1. Read head log. Parse for success and failured input paths.
# 2. Write success_paths.txt and failure_paths.txt
# 3. THEN YOU HAVE TO MANUALLY SPECIFY THIS CHECKPOINT succes_paths.txt file in the workflow.

"""
Usage: 

python save_checkpoint.py \
  <path_to_ray_log ray_client_server_23000.err> \
  <output_path for results>

Usage example: 

python save_checkpoint.py \
  /scratch/bbki/kastanday/maple_data_xsede_bridges2/outputs/viz_output/july_12_v0_tmp_resumable/node_cn034 \
  /scratch/bbki/kastanday/maple_data_xsede_bridges2/outputs/viz_output/july_12_v0_tmp_resumable/node_cn034
"""

import time
from argparse import ArgumentParser


def main():
    parser = ArgumentParser(prog="cli")
    parser.add_argument(
        "path_to_log", help="Path to ray_client_server_23000.err log file"
    )
    parser.add_argument(
        "output_path", help="Path to write success_paths.txt and failure_paths.txt"
    )
    args = parser.parse_args()

    # globals
    failure_write_path = f"{args.output_path}/failure_paths.txt"
    success_write_path = f"{args.output_path}/success_paths.txt"

    # read in logs
    with open(f"{args.path_to_log}", "r") as f:
        data = f.readlines()

    ####
    # Parse logs for messy filepath entries
    ####

    raw_success_paths = []
    raw_failure_paths = []
    for line in data:
        if "✅ successful_staging_input_path" in line:
            raw_success_paths.append(line)
        elif "❌ failed_staging_input_path" in line:
            raw_failure_paths.append(line)

    ####
    # Get clean path lists
    ####

    clean_failure_paths = []
    for h in raw_failure_paths:
        clean_failure_paths.append(
            h.strip("\n").split("failed_staging_input_path: ")[1]
        )

    clean_success_paths = []
    for h in raw_success_paths:
        clean_success_paths.append(
            h.strip("\n").split("successful_staging_input_path: ")[1]
        )

    ####
    # Save path list to file
    ####

    if clean_failure_paths:
        print(f"❌ There were failures! Saving list to: {failure_write_path}")
        with open(failure_write_path, "w") as f:
            for line in clean_failure_paths:
                f.write("%s\n" % line)
    else:
        print("👍 No failures detected.")

    if clean_success_paths:
        print(f"✅ There were Successes! Saving list to: {success_write_path}")
        with open(success_write_path, "w") as f:
            for line in clean_success_paths:
                f.write("%s\n" % line)
    else:
        print("👎 No successes detected...")

        ####
        # verify success_paths results
        ####
        print("Veifying files written correctly...")
        with open(success_write_path, "r") as f:
            check_data = f.readlines()

        print(f"Number of success paths: {len(check_data)}")
        print("First 3 exampels...")
        for h in check_data[:3]:
            print(h)


if __name__ == "__main__":
    main()

# BASE_VIZ_OUTPUT_PATH = '/scratch/bbki/kastanday/maple_data_xsede_bridges2/outputs/viz_output/july_12_v0_tmp_resumable/'
