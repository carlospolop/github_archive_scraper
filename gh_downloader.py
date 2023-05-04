import argparse
import os

from queue import Queue
from threading import Thread, Lock
from tqdm import tqdm

from lib.functions import download_file, decompress_gz, read_urls_from_file, splitlines_generator


PROGRESS_BAR_LOCK = Lock()

def write_content_to_file(decompressed_content, output_folder, file_prefix):
    """
    Write the decompressed content to files in the output folder, with each file having a maximum size of 100 MB.
    The files are split by complete lines if necessary.

    :param decompressed_content: The decompressed content to be written to files.
    :param output_folder: The folder path where the output files will be generated.
    :param file_prefix: The prefix to be used for the output file names.
    """
    
    # Create the output folder if it doesn't exist
    os.makedirs(output_folder, exist_ok=True)

    # Set the maximum file size to 100 MB
    max_size = 100 * 1024 * 1024

    # Initialize variables for file part number, output content, and size
    part_num = 1
    output = ""
    size = 0

    # Iterate over each line in the decompressed content
    for line in splitlines_generator(decompressed_content):
        # Calculate the size of the current line in bytes
        line_size = len(line.encode('utf-8'))

        # If the accumulated size exceeds the max size, write the content to a file and reset the output and size
        if size + line_size > max_size:
            file_name = f"{file_prefix}_{part_num}.json"
            output_path = os.path.join(output_folder, file_name)
            with open(output_path, 'w') as f:
                f.write(output)
            part_num += 1
            output = ""
            size = 0

        # Append the current line to the output and update the size
        output += line + "\n"
        size += line_size

    # Write any remaining content to a file
    if output:
        file_name = f"{file_prefix}_{part_num}.json"
        output_path = os.path.join(output_folder, file_name)
        with open(output_path, 'w') as f:
            f.write(output)


def worker(queue, progress_bar, output_folder):
    """
    Worker function for threads. Continuously processes URLs from the queue until a sentinel value (None) is encountered.

    :param queue: A queue containing GitHub Archive URLs to process.
    :param progress_bar: A tqdm progress bar object to update as tasks are completed.
    """

    global PROGRESS_BAR_LOCK

    while True:
        url = queue.get()
        if url is None:
            break
        
        content = download_file(url)
        if not content:
            return
        
        decompressed_content = decompress_gz(content)
        del content
        if decompressed_content:
            file_prefix = os.path.splitext(os.path.basename(url))[0]
            write_content_to_file(decompressed_content.decode('utf-8'), output_folder, file_prefix)
        
        with PROGRESS_BAR_LOCK:
            progress_bar.update()

        queue.task_done()
        


def process_github_archive(urls, output_folder, num_threads):
    """
    Process a list of GitHub Archive URLs using multi-threading, extract unique repositories and users,
    and write the results to CSV files in the specified output folder.

    :param urls: A list of GitHub Archive URLs.
    :param output_folder: The folder path where the final CSV files will be generated.
    :param num_threads: The number of threads to use for processing URLs.
    """

    queue = Queue()
    for url in urls:
        queue.put(url)

    # Create and start worker threads with a progress bar
    with tqdm(total=len(urls), desc="Processing URLs") as progress_bar:
        threads = []
        for _ in range(num_threads):
            t = Thread(target=worker, args=(queue, progress_bar, output_folder))
            t.start()
            threads.append(t)

        # Wait for all tasks in the queue to be completed
        queue.join()

        # Signal worker threads to exit by adding sentinel values (None) to the queue
        for _ in range(num_threads):
            queue.put(None)

        # Wait for all worker threads to finish
        for t in threads:
            t.join()


def main(urls_file_path, output_folder, num_threads, one_file_name):
    """
    Main function to download all github archive log files.
    
    :param urls_file_path: The path of the file containing the GitHub Archive URLs.
    :param output_folder: The folder path where the final CSV files will be generated.
    :param num_threads: The number of threads to use for processing URLs.
    :param one_file_name: The file name of the final json file.
    """
    
    urls = read_urls_from_file(urls_file_path)
    process_github_archive(urls, output_folder, num_threads)

    # Get the list of JSON files in the source directory
    if one_file_name:
        json_files = [
            os.path.join(output_folder, file_name)
            for file_name in os.listdir(output_folder)
            if file_name.endswith('.json')
        ]
        
        # Open the output file and write the contents of all JSON files into it
        with open(os.path.join(output_folder, one_file_name), 'w') as output_file:
            for json_file_path in json_files:
                with open(json_file_path, 'r') as input_file:
                    for line in input_file:
                        output_file.write(line)
            os.remove(json_file_path)



if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process GitHub Archive URLs and generate unique repositories and users CSV files.")
    parser.add_argument('-i', '--urls-file', type=str, help="The path of the file containing the GitHub Archive URLs.")
    parser.add_argument('-o', '--output-folder', type=str, help="The path of the folder where the CSV files will be generated.")
    parser.add_argument('-t', '--threads', type=int, default=4, help="Number of threads to use for processing URLs.")
    parser.add_argument('-1', '--one', type=str, default=4, help="Reduce the content to one file indicating the file name.")

    args = parser.parse_args()
    main(args.urls_file, args.output_folder, args.threads, args.one)