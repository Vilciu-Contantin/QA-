import os
import shutil
import hashlib
import argparse
import logging
import schedule
import time

# Function to calculate MD5 hash of a file
def calculate_md5(file_path):
    with open(file_path, 'rb') as f:
        md5_hash = hashlib.md5()
        while True:
            data = f.read(8192)
            if not data:
                break
            md5_hash.update(data)
    return md5_hash.hexdigest()

# Function to perform one-way synchronization between source and replica folders
def synchronize_folders(source_folder, replica_folder):
    # Get list of files
    source_files = set(os.listdir(source_folder))
    replica_files = set(os.listdir(replica_folder))

    # Delete files
    for file_name in replica_files - source_files:
        file_path = os.path.join(replica_folder, file_name)
        if os.path.isfile(file_path):
            os.remove(file_path)
            logging.info(f'Removed file: {file_path}')

    # Copy new/modified
    for file_name in source_files:
        source_file_path = os.path.join(source_folder, file_name)
        replica_file_path = os.path.join(replica_folder, file_name)

        if os.path.isfile(source_file_path):
            source_md5 = calculate_md5(source_file_path)
            replica_md5 = None
            if os.path.isfile(replica_file_path):
                replica_md5 = calculate_md5(replica_file_path)

            if not os.path.isfile(replica_file_path) or source_md5 != replica_md5:
                shutil.copy2(source_file_path, replica_file_path)
                logging.info(f'Copied file: {source_file_path} -> {replica_file_path}')
            elif source_md5 == replica_md5:
                logging.info(f'Skipped file (already synced): {source_file_path}')

    logging.info('Synchronization completed.')

# Function
def start_synchronization(source_folder, replica_folder, interval, log_file):
    logging.basicConfig(filename=log_file, level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    logging.info('Starting synchronization...')
    synchronize_folders(source_folder, replica_folder)
    schedule.every(interval).minutes.do(synchronize_folders, source_folder, replica_folder)
    while True:
        schedule.run_pending()
        time.sleep(1)

if __name__ == '_main_':
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='One-way folder synchronization')
    parser.add_argument('source_folder', type=str, help='Path to the source folder')
    parser.add_argument('replica_folder', type=str, help='Path to the replica folder')
    parser.add_argument('interval', type=int, help='Synchronization interval in minutes')
    parser.add_argument('log_file', type=str, help='Path to the log file')
    args = parser.parse_args()

    # Start periodic synchronization
    start_synchronization(args.source_folder, args.replica_folder, args.interval, args.log_file)