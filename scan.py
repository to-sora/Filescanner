# scan.py

import os
import json
import argparse

import os

def scan_directory(directory):
    file_info_list = []
    visited_inodes = set()

    for root, dirs, files in os.walk(directory, followlinks=False):
        for file in files:
            filepath = os.path.join(root, file)
            try:
                st = os.stat(filepath, follow_symlinks=False)
                # The combination (st.st_dev, st.st_ino) uniquely identifies a file on Unix
                file_id = (st.st_dev, st.st_ino)
                
                if file_id in visited_inodes:
                    # We've already encountered this exact physical file
                    continue
                
                visited_inodes.add(file_id)
                
                size = st.st_size
                file_info = {
                    "path": filepath,
                    "name": file,
                    "size": size
                }
                file_info_list.append(file_info)
            except OSError as e:
                print(f"Error accessing file {filepath}: {e}")

    return file_info_list


def save_to_json(data, output_file):
    try:
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=4)
        print(f"File information successfully saved to {output_file}")
    except IOError as e:
        print(f"Error writing to file {output_file}: {e}")

def main():
    parser = argparse.ArgumentParser(description="Scan a directory and store file information.")
    parser.add_argument('directory', help="The directory to scan.")
    parser.add_argument('--output', default='file_info.json', help="Output JSON file name.")
    args = parser.parse_args()

    if not os.path.isdir(args.directory):
        print(f"The provided path '{args.directory}' is not a directory or does not exist.")
        return

    print(f"Scanning directory: {args.directory}")
    file_info = scan_directory(args.directory)
    print(f"Total files found: {len(file_info)}")

    save_to_json(file_info, args.output)

if __name__ == "__main__":
    main()
