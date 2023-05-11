import argparse
import json
import os
import subprocess

from tqdm import tqdm

from lib.classes import Repository, User
from lib.functions import write_csv_files


UNIQUE_REPOS = dict()
UNIQUE_USERS = dict()



def check_repo_in_event(event):
    """
    Get the repository from the event and update the UNIQUE_REPOS dictionary accordingly.

    :param event: The event to check.
    :return: None
    """

    global UNIQUE_REPOS

    if 'repo' in event:
        repo_full_name = event['repo']['name']

        s = repo_full_name.split("/")
        if len(s) > 2:
            print(f"Error: repo_full_name has more than 2 slashes: {repo_full_name}. Changed to {s[1]}/{s[2]}")
            repo_full_name = f"{s[1]}/{s[2]}"
                

        if not repo_full_name in UNIQUE_REPOS:
            UNIQUE_REPOS[repo_full_name] = Repository(
                full_name=repo_full_name,
                stars=0,
                forks=0,
                watchers=0,
                deleted=False,
                private=False,
                archived=False,
                disabled=False
            )
            
        if event["type"] == "DeleteEvent":
            # If aparently private, and deleted main/master branch -> not private, just deleted
            if event["payload"]["ref_type"] == "branch" and event["payload"]["ref"] in ["master", "main"]:
                UNIQUE_REPOS[repo_full_name].deleted = True
                UNIQUE_REPOS[repo_full_name].private = False


def check_user_in_event(event):
    """
    Get the repository from the event and update the UNIQUE_REPOS dictionary accordingly.

    :param event: The event to check.
    :return: None
    """

    global UNIQUE_USERS

    if 'actor' in event or event.get("pull_request", {}).get("user", None):
        username = event['actor']['login'] if 'actor' in event else event['pull_request']['user']['login']

        if not username in UNIQUE_USERS:            
            UNIQUE_USERS[username] = User(
                username=username,
                repos_collab=list(),
                deleted=False,
                site_admin=False,
                hireable=False,
                github_star=False,
                email='',
                company='',
            )

        if "pull_request" in event and event["pull_request"]["merged_at"]:
            repo_name = event['repo']['name']
            user = UNIQUE_USERS[username]
            if len(user.repos_collab) < user.max_repos and not repo_name in user.repos_collab:
                UNIQUE_USERS[username].repos_collab.append(repo_name)
            
        elif event["type"] == "PushEvent":
            repo_name = event['repo']['name']
            user = UNIQUE_USERS[username]
            if len(user.repos_collab) < user.max_repos and not repo_name in user.repos_collab:
                UNIQUE_USERS[username].repos_collab.append(repo_name)


def parse_github_archive(file_path):
    """
    Parse a single GitHub Archive log file and update the UNIQUE_REPOS and UNIQUE_USERS dictionaries accordingly.

    :param file_path: The path to a GitHub Archive log file.
    """

    # Open and read the log file line by line
    with open(file_path, 'rb') as f:
        for line in f:
            # Load the event as a JSON object
            try:
                event = json.loads(line)
            except json.decoder.JSONDecodeError:
                print(f"Error decoding JSON in file {file_path} on line {line}.")
                continue

            # Check the repository in the event and update the UNIQUE_REPOS dictionary
            check_repo_in_event(event)

            # Check the user in the event and update the UNIQUE_USERS dictionary
            check_user_in_event(event)


def process_files_github_archive(logs_files, output_folder):
    """
    Process a list of GitHub Archive log files and write the results to CSV files in the specified output folder.

    :param logs_files: A list of paths to GitHub Archive log files.
    :param output_folder: The folder path where the final CSV files will be generated.
    """

    # Iterate over each log file with a progress bar
    with tqdm(total=len(logs_files), desc="Processing Log Files") as progress_bar:
        for file_path in logs_files:
            parse_github_archive(file_path)
            progress_bar.update()

    # Write the final results to CSV files
    write_csv_files(UNIQUE_REPOS, UNIQUE_USERS, output_folder)

def process_urls_github_archive(urls, output_folder):
    """
    Process a list of GitHub Archive log files and write the results to CSV files in the specified output folder.

    :param urls: A list of urls to process
    :param output_folder: The folder path where the final CSV files will be generated.
    """

    # Iterate over each log file with a progress bar
    with tqdm(total=len(urls), desc="Processing URLs") as progress_bar:
        for url in urls:
            file_name = url.split("/")[-1]
            file_path = os.path.join("/tmp", file_name).replace(".gz", "")
            cmd = f'curl -s "{url}" | gzip -d 2>/dev/null > "{file_path}"'
            try:
                subprocess.run(cmd, shell=True, check=True)
            except subprocess.CalledProcessError:
                pass

            if os.path.isfile(file_path) and os.path.getsize(file_path) > 0:
                parse_github_archive(file_path)
                print(f"Parsed: {url}")
            else:
                print(f"Bad logs: {url}")
            
            try:
                os.remove(file_path)
            except:
                pass
            progress_bar.update()


    # Write the final results to CSV files
    write_csv_files(UNIQUE_REPOS, UNIQUE_USERS, output_folder)
        

def main(urls_file_path, logs_folder, logs_file, output_folder):
    """
    Main function to process a folder containing GitHub Archive log files and write the results to CSV files.

    :param urls_file_path: The file path containing the list of urls to process.
    :param logs_folder: The folder path containing the GitHub Archive log files.
    :param logs_file: The file path containing all the GitHub Archive logs.
    :param output_folder: The folder path where the final CSV files will be generated.
    """

    if urls_file_path:
        # Read the URLs file and get the list of log files
        with open(urls_file_path, "r") as f:
            log_urls = f.read().splitlines()

        # Process the log files and generate the output CSV files
        process_urls_github_archive(log_urls, output_folder)
    
    else:
        # Get the list of log files in the logs_folder with a .json extension
        if logs_folder:
            logs_files = [
                os.path.join(logs_folder, file_name)
                for file_name in os.listdir(logs_folder)
                if file_name.endswith(".json")
            ]
        else:
            logs_files = [logs_file]

        # Process the log files and generate the output CSV files
        process_files_github_archive(logs_files, output_folder)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process GitHub Archive URLs and generate unique repositories and users CSV files.")
    
    # Create a mutually exclusive group for input options
    input_group = parser.add_mutually_exclusive_group(required=True)
    input_group.add_argument('-u', '--urls-file', type=str, help="The path of the file containing the log URLs to parse.")
    input_group.add_argument('-i', '--logs-folder', type=str, help="The path of the folder containing the GitHub Archive logs.")
    input_group.add_argument('-f', '--logs-file', type=str, help="The path of the file containing the GitHub Archive logs.")
    
    parser.add_argument('-o', '--output-folder', type=str, help="The path of the folder where the CSV files will be generated.")

    args = parser.parse_args()
    main(args.urls_file, args.logs_folder, args.logs_file, args.output_folder)