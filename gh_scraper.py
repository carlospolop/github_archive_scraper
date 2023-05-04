import argparse
import csv
import json
import os

from io import StringIO
from queue import Queue
from threading import Thread, Lock
from tqdm import tqdm

from lib.classes import Repository, User
from lib.functions import download_file, decompress_gz, read_urls_from_file, write_csv_files


UNIQUE_REPOS = dict()
UNIQUE_USERS = dict()
REPO_LOCK = Lock()
USER_LOCK = Lock()
PROGRESS_BAR_LOCK = Lock()


def check_repo_in_event(event):
    """
    Get the repository from the event and update the UNIQUE_REPOS dictionary accordingly.

    :param event: The event to check.
    :return: None
    """

    global UNIQUE_REPOS

    if 'repo' in event:
        repo_full_name = event['repo']['name']

        if repo_full_name in UNIQUE_REPOS:
            repo = UNIQUE_REPOS[repo_full_name]

        else:
            repo = Repository(
                full_name=repo_full_name,
                stars=-1,
                forks=-1,
                watchers=-1,
                deleted=False,
                private=False,
                archived=False,
                disabled=False
            )
            UNIQUE_REPOS[repo_full_name] = repo
            
        if event["type"] == "DeleteEvent":
            # If aparently private, and deleted main/master branch -> not private, just deleted
            if repo.private == True and event["payload"]["ref_type"] == "branch" and event["payload"]["ref"] in ["master", "main"]:
                repo.deleted = True
                repo.private = False
                UNIQUE_REPOS[repo_full_name] = repo


def check_user_in_event(event):
    """
    Get the repository from the event and update the UNIQUE_REPOS dictionary accordingly.

    :param event: The event to check.
    :return: None
    """

    global UNIQUE_USERS

    if 'actor' in event or event.get("pull_request", {}).get("user", None):
        username = event['actor']['login'] if 'actor' in event else event['pull_request']['user']['login']

        if username in UNIQUE_USERS:
            user = UNIQUE_USERS[username]
            
        else:

            user = User(
                username=username,
                repos_collab=set(),
                deleted=False,
                site_admin=False,
                hireable=False,
                github_star=False,
                email='',
                company='',
            )
            UNIQUE_USERS[username] = user


        if "pull_request" in event and event["pull_request"]["merged_at"]:
            user.repos_collab.add(event['repo']['name'])
            UNIQUE_USERS[username] = user
            
        elif event["type"] == "PushEvent":
            user.repos_collab.add(event['repo']['name'])
            UNIQUE_USERS[username] = user


def parse_github_archive(url):
    """
    Parse a single GitHub Archive URL and extract unique repositories and users.

    :param url: The URL of a GitHub Archive file.
    :return: A tuple containing two sets: unique repositories and unique users.
    """

    global REPO_LOCK, USER_LOCK

    content = download_file(url)
    decompressed_content = decompress_gz(content)
    if decompressed_content is None:
        return

    for line in StringIO(decompressed_content.decode('utf-8')):
        event = json.loads(line)
        with REPO_LOCK:
            check_repo_in_event(event)
        
        with USER_LOCK:
            check_user_in_event(event)

def worker(queue, progress_bar):
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
        parse_github_archive(url)
        
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
    global UNIQUE_REPOS, UNIQUE_USERS

    queue = Queue()
    for url in urls:
        queue.put(url)

    # Create and start worker threads with a progress bar
    with tqdm(total=len(urls), desc="Processing URLs") as progress_bar:
        threads = []
        for _ in range(num_threads):
            t = Thread(target=worker, args=(queue, progress_bar))
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

    write_csv_files(UNIQUE_REPOS, UNIQUE_USERS, output_folder)



def main(urls_file_path, output_folder, num_threads):
    """
    Main function to process a file containing GitHub Archive URLs using multi-threading and write the results to CSV files.

    :param urls_file_path: The path of the file containing the GitHub Archive URLs.
    :param output_folder: The folder path where the final CSV files will be generated.
    :param num_threads: The number of threads to use for processing URLs.
    """
    
    urls = read_urls_from_file(urls_file_path)
    process_github_archive(urls, output_folder, num_threads)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process GitHub Archive URLs and generate unique repositories and users CSV files.")
    parser.add_argument('-i', '--urls-file', type=str, help="The path of the file containing the GitHub Archive URLs.")
    parser.add_argument('-o', '--output-folder', type=str, help="The path of the folder where the CSV files will be generated.")
    parser.add_argument('-t', '--threads', type=int, default=10, help="Number of threads to use for processing URLs.")

    args = parser.parse_args()
    main(args.urls_file, args.output_folder, args.threads)