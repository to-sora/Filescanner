# merge.py

import json
import sys
import glob

def merge_json(file_list, output_file="merged.json"):
    try:
        merged_data = []
        
        # Iterate over each file in the file list
        for file in file_list:
            with open(file, 'r') as f:
                data = json.load(f)
                if not isinstance(data, list):
                    raise ValueError(f"File {file} does not contain a JSON list at the root level.")
                merged_data.extend(data)
        
        # Write the merged data to an output file
        with open(output_file, 'w') as fout:
            json.dump(merged_data, fout, indent=4)
        
        print(f"Successfully merged JSON files into {output_file}")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python merge.py <file1.json> <file2.json> ... or python merge.py *.json")
        sys.exit(1)
    
    # Use glob to expand wildcard patterns
    files = []
    for arg in sys.argv[1:]:
        files.extend(glob.glob(arg))
    
    if not files:
        print("No JSON files found.")
        sys.exit(1)
    
    merge_json(files)
