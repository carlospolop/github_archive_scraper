import argparse
import json
import gc
import os

from threading import Lock
from tqdm import tqdm

from lib.classes import Repository, User
from lib.functions import decompress_gz, write_csv_files, splitlines_generator


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

        if not repo_full_name in UNIQUE_REPOS:
            UNIQUE_REPOS[repo_full_name] = Repository(
                full_name=repo_full_name,
                stars=-1,
                forks=-1,
                watchers=-1,
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
    global REPO_LOCK, USER_LOCK

    with open(file_path, 'rb') as f:
        for line in f:
            event = json.loads(line)
            with REPO_LOCK:
                check_repo_in_event(event)

            with USER_LOCK:
                check_user_in_event(event)


def process_github_archive(logs_files, output_folder):
    with tqdm(total=len(logs_files), desc="Processing Log Files") as progress_bar:
        for file_path in logs_files:
            parse_github_archive(file_path)
            progress_bar.update()

    write_csv_files(UNIQUE_REPOS, UNIQUE_USERS, output_folder)
        

def main(logs_folder, output_folder):
    logs_files = [
        os.path.join(logs_folder, file_name)
        for file_name in os.listdir(logs_folder)
        if file_name.endswith(".json")
    ]
    process_github_archive(logs_files, output_folder)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process GitHub Archive URLs and generate unique repositories and users CSV files.")
    parser.add_argument('-i', '--logs-folder', type=str, help="The path of the fodlder containing the GitHub Archive logs.")
    parser.add_argument('-o', '--output-folder', type=str, help="The path of the folder where the CSV files will be generated.")

    args = parser.parse_args()
    main(args.logs_folder, args.output_folder)