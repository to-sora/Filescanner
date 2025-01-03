# program_b.py
from tqdm import tqdm
import json
import argparse
from collections import defaultdict
import hashlib
import json
import argparse
from collections import defaultdict
import hashlib
import os
from multiprocessing import Pool, cpu_count
def load_file_info(input_file):
    try:
        with open(input_file, 'r', encoding='utf-8') as f:
            return json.load(f)
    except IOError as e:
        print(f"Error reading file {input_file}: {e}")
        return []
TEMP = "|"
def generate_key(name, size):
    # Create a unique string key by concatenating name and size
    # Alternatively, use a hash for more uniqueness
    unique_string = f"{name}{TEMP}{size}"
    # Optionally, hash the unique_string for a fixed-length key
    # key_hash = hashlib.sha256(unique_string.encode()).hexdigest()
    # return key_hash
    return unique_string  # Using concatenated string as key

def find_duplicates(file_info_list):
    duplicates = defaultdict(list)
    for file_info in tqdm(file_info_list):
        key = generate_key(file_info['name'], file_info['size'])
        duplicates[key].append(file_info['path'])
    # Filter out entries that have duplicates (more than one file)
    duplicates = {k: v for k, v in duplicates.items() if len(v) > 1}
    return duplicates

def save_duplicates(duplicates, output_file):
    try:
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(duplicates, f, indent=4)
        print(f"Duplicate information successfully saved to {output_file}")
    except IOError as e:
        print(f"Error writing to file {output_file}: {e}")


def process_chunk(chunk):
    """
    Process a chunk of duplicate groups to calculate partial duplicate sizes.

    Args:
        chunk (list): List of (key, paths) tuples.

    Returns:
        int: Partial duplicate size for the chunk.
    """
    partial_duplicate_size = 0
    for key, paths in chunk:
        try:
            _, size = key.rsplit(TEMP, 1)
            size = int(size)
            partial_duplicate_size += size * (len(paths) - 1)
        except ValueError:
            print(f"Skipping invalid key format: {key}")
            continue
    print(f"Processed chunk with {len(chunk)} entries by process {os.getpid()}")
    return partial_duplicate_size

def compute_sizes_parallel(duplicates):
    """
    Compute duplicate_size in parallel using multiprocessing.
    Each process handles a subset of the duplicates.
    """

    # Convert duplicates.items() to a list for splitting
    duplicates_items = list(duplicates.items())
    num_processes = cpu_count()
    print(f"Number of processes: {num_processes}")
    chunk_size = len(duplicates_items) // num_processes + 1
    print(f"Chunk size: {chunk_size} from {len(duplicates_items)} total entries")
    chunks = [duplicates_items[i:i + chunk_size] for i in range(0, len(duplicates_items), chunk_size)]

    with Pool(processes=num_processes) as pool:
        results = pool.map(process_chunk, chunks)

    duplicate_size = sum(results)
    return duplicate_size

def compute_sizes(file_info_list, duplicates, use_parallel=False):
    total_size = sum(file['size'] for file in file_info_list)
    if use_parallel:
        print("Computing duplicate size using parallel processing...")
        duplicate_size = compute_sizes_parallel(duplicates)
    else:
        duplicate_size = 0
        for key, paths in duplicates.items():
            try:
                _, size = key.rsplit(TEMP, 1)
                size = int(size)
                duplicate_size += size * (len(paths) - 1)
            except ValueError:
                print(f"Skipping invalid key format: {key}")
                continue
    unique_size = total_size - duplicate_size
    return total_size, unique_size

def find_paths_with_many_duplicates(duplicates, threshold=10):
    result = {}
    for key, paths in tqdm(duplicates.items()):
        if len(paths) > threshold:
            result[key] = paths
    return result

def save_extra_files(duplicates, all_duplicates_file, unique_files_file):
    all_duplicate_paths = []
    unique_file_paths = []

    for key, paths in duplicates.items():
        unique_file_paths.append(paths[0])  # First occurrence
        all_duplicate_paths.extend(paths[1:])  # Remaining duplicates

    # Save all duplicate paths
    try:
        with open(all_duplicates_file, 'w', encoding='utf-8') as f:
            json.dump(all_duplicate_paths, f, indent=4)
        print(f"All duplicate paths saved to {all_duplicates_file}")
    except IOError as e:
        print(f"Error writing to file {all_duplicates_file}: {e}")

    # Save unique file paths
    try:
        with open(unique_files_file, 'w', encoding='utf-8') as f:
            json.dump(unique_file_paths, f, indent=4)
        print(f"Unique file paths saved to {unique_files_file}")
    except IOError as e:
        print(f"Error writing to file {unique_files_file}: {e}")
import json
from collections import defaultdict
from tqdm import tqdm

def find_directory_subset_relations(file_info_list, duplicates, threshold=0.8):
    """
    Identify directories that are subsets (~⊆) or exact subsets (⊆) of parent directories
    based on the proportion of duplicated files.

    Args:
        file_info_list (list): List of dictionaries containing file information.
                               Each dictionary has keys: 'path', 'name', 'size'.
        duplicates (dict): Dictionary of duplicates where keys are 'name|size' and
                           values are lists of file paths that are duplicates.
        threshold (float): Proportion threshold (0 < threshold ≤ 1.0) to determine
                           subset relationships. Defaults to 0.8.

    Returns:
        list: A list of relationship dictionaries with keys:
              'child', 'relationship', 'parent'.
              
              Example:
              [
                  {"child": "dir_B", "relationship": "~⊆", "parent": "dir_A"},
                  {"child": "dir_C", "relationship": "⊆", "parent": "dir_A"},
                  ...
              ]

    Usage:
        # Example file_info_list
        file_info_list = [
            {"path": "/dir_A/file1.txt", "name": "file1.txt", "size": 1024},
            {"path": "/dir_A/file2.txt", "name": "file2.txt", "size": 2048},
            {"path": "/dir_B/file1.txt", "name": "file1.txt", "size": 1024},
            {"path": "/dir_B/file3.txt", "name": "file3.txt", "size": 3072},
            {"path": "/dir_C/file1.txt", "name": "file1.txt", "size": 1024},
            {"path": "/dir_C/file4.txt", "name": "file4.txt", "size": 4096},
            # ... more files ...
        ]

        # Example duplicates dictionary from Program B
        duplicates = {
            "file1.txt|1024": ["/dir_A/file1.txt", "/dir_B/file1.txt", "/dir_C/file1.txt"],
            # ... more duplicates ...
        }

        relationships = find_directory_subset_relations(file_info_list, duplicates, threshold=0.8)

        # Save relationships to JSON
        with open('subset_relationships.json', 'w', encoding='utf-8') as f:
            json.dump(relationships, f, indent=4)
    """
    import random
    
    # Step 1: Map each 'name|size' to its parent directory (first occurrence)
    dir_to_name_size = defaultdict(list)
    for file_info in file_info_list:
        dir_name = os.path.dirname(file_info['path'])
        filekey = generate_key(file_info['name'], file_info['size'])
        dir_to_name_size[dir_name].append(filekey)
    # # print 5 random items
    # print("5 random items from dir_to_name_size:")
    # for _ in range(5):
    #     key, value = random.choice(list(dir_to_name_size.items()))
    #     print(f"{key}: {value}")
    
    dub_file_to_dir = defaultdict(list)
    for key, paths in duplicates.items():
        for path in paths:
            dir_name = os.path.dirname(path)
            dub_file_to_dir[key].append(dir_name)
    # print("5 random items from dub_file_to_dir:")
    # for _ in range(5):
    #     key, value = random.choice(list(dub_file_to_dir.items()))
    #     print(f"{key}: {value}")
    
    relationships = []

    for key, paths in tqdm(dir_to_name_size.items()):
        freq_dict = defaultdict(int)
        total_files = len(paths)
        if total_files < 100:
            continue
        for path in paths:
            for dir_name in dub_file_to_dir[path]:
                freq_dict[dir_name] += 1
        for dir_name, freq in freq_dict.items():
            if freq / total_files >= threshold:
                if dir_name != key:
                    relationship = "⊆" if freq == total_files else "~⊆"
                    relationships.append({"child": key, "relationship": relationship, "parent": dir_name})
    with open('subset_relationships.json', 'w', encoding='utf-8') as f:
        json.dump(relationships, f, indent=4)
    return relationships
        





def main():
    parser = argparse.ArgumentParser(description="Identify duplicate files and provide summary information.")
    parser.add_argument('input', help="Input JSON file from Program A.")
    parser.add_argument('--duplicates_output', default='duplicates.json', help="Output JSON file for duplicates.")
    parser.add_argument('--all_duplicates_file', default='all_duplicate_paths.json', help="Output JSON file for all duplicate paths.")
    parser.add_argument('--unique_files_file', default='unique_file_paths.json', help="Output JSON file for unique file paths.")
    parser.add_argument('--threshold', type=int, default=10, help="Threshold for number of duplicates.")
    args = parser.parse_args()

    print(f"Loading file information from {args.input}")
    file_info = load_file_info(args.input)
    if not file_info:
        print("No file information to process.")
        return

    print("Identifying duplicates...")
    duplicates = find_duplicates(file_info)
    print(f"Total duplicate groups found: {len(duplicates)}")

    save_duplicates(duplicates, args.duplicates_output)

    print(f"Finding files with more than {args.threshold} duplicates...")
    many_duplicates = find_paths_with_many_duplicates(duplicates, threshold=args.threshold)
    print(f"Number of files with more than {args.threshold} duplicates: {len(many_duplicates)}")
    with open('many_duplicates.json', 'w', encoding='utf-8') as f:
        json.dump(many_duplicates, f, indent=4)

    print("Saving all duplicate paths and unique file paths...")
    save_extra_files(duplicates, args.all_duplicates_file, args.unique_files_file)


    print("Computing total and unique file sizes...")
    total_size, unique_size = compute_sizes(file_info, duplicates,use_parallel=True)
    print(f"Total file size: {total_size} bytes , {total_size/1024/1024/1024/1024} TB")
    print(f"Unique file size: {unique_size} bytes , {unique_size/1024/1024/1024/1024} TB")
    print(f"Duplicate file size: {total_size - unique_size} bytes")
    print("Dulicate ratio: {:.2f}%".format((total_size - unique_size) / total_size * 100))

    print("Finding directory subset relationships...")
    print(file_info[0])
    relationships = find_directory_subset_relations(file_info_list=file_info, duplicates=duplicates, threshold=0.8)

    # Save relationships to a JSON file
    with open('subset_relationships.json', 'w', encoding='utf-8') as f:
        json.dump(relationships, f, indent=4)

    print("Subset relationships have been saved to 'subset_relationships.json'")

if __name__ == "__main__":
    main()
