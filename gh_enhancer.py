import argparse

from queue import Queue
from threading import Thread, Lock
from tqdm import tqdm
from typing import List

from lib.classes import Repository, User
from lib.functions import get_repos_info, get_users_info, load_csv_repo_file, load_csv_user_file, write_csv_files


UNIQUE_REPOS = dict()
UNIQUE_USERS = dict()
REPO_LOCK = Lock()
USER_LOCK = Lock()
PROGRESS_BAR_LOCK = Lock()



def check_repos(repos:List[Repository], gh_token):
    """
    Get info about the Github repo

    :param repo: The Repository object to check.
    :return: None
    """

    global UNIQUE_REPOS, REPO_LOCK

    repos_info = get_repos_info(repos, gh_token)

    if not repos_info:
        return

    with REPO_LOCK:
        for repo_full_name, repo_info in repos_info.items():
            repo = UNIQUE_REPOS[repo_full_name]
            if repo_info["inexistent"]:
                repo.stars = -1
                repo.forks = -1
                repo.watchers = -1
                repo.archived = False
                repo.disabled = False
                repo.deleted = repo.deleted
                repo.private = False if repo.deleted else True

            else:
                repo.stars = repo_info['stargazers_count'],
                repo.forks = repo_info['forks_count'],
                repo.watchers = repo_info['watchers_count'],
                repo.archived = repo_info['archived'],
                repo.disabled = repo_info['disabled'],
                repo.deleted = False
                repo.private = False
            
            UNIQUE_REPOS[repo.full_name] = repo


def check_users(users: List[User], gh_token):
    """
    Get info about the Github usernames

    :param users: A lis of User objects to check.
    :return: None
    """

    global UNIQUE_USERS, USER_LOCK

    users_info = get_users_info(users, gh_token)

    if not users_info:
        return
    
    with USER_LOCK:
        for username, user_info in users_info.items():
            user = UNIQUE_USERS[username]
            if user_info["inexistent"]:
                user.deleted=True

            else:
                user.site_admin=user_info['site_admin']
                user.hireable=user_info['hireable']
                user.email=user_info['email']
                user.company=user_info['company']
                user.github_star=user_info['github_star']

        UNIQUE_USERS[user.username] = user


def parse_github_assets(assets, gh_token):
    """
    Parse a single GitHub assets and obtain details about it.

    :param assets: The Github assets.
    :return: A tuple containing two sets: unique repositories and unique users.
    """

    users = []
    repos = []
    for asset in assets:
        if isinstance(asset, User):
            users.append(asset)

        elif isinstance(asset, Repository):
            repos.append(asset)
        else:
            print(f"Unknown asset type: {type(asset)}")
        
    if users and repos:
        print(f"Somehow there are users and repos in the same batch. users: {len(users)} repos: {len(repos)}")

    if len(users) > len(repos):
        check_users(users, gh_token)

    else:
        check_repos(repos, gh_token)
        

def worker(queue, progress_bar, gh_token):
    """
    Worker function for threads. Continuously processes URLs from the queue until a sentinel value (None) is encountered.

    :param queue: A queue containing GitHub Archive URLs to process.
    :param progress_bar: A tqdm progress bar object to update as tasks are completed.
    """

    global PROGRESS_BAR_LOCK

    while True:
        asset = queue.get()
        if asset is None:
            break
        parse_github_assets(asset, gh_token)
        
        with PROGRESS_BAR_LOCK:
            progress_bar.update()
            
        queue.task_done()
        


def process_github_assets(output_folder, num_threads, gh_token, assets, title):
    """
    Process a list of GitHub assets using multi-threading, extract details about repositories and users,
    and write the results to CSV files in the specified output folder.

    :param output_folder: The folder path where the final CSV files will be generated.
    :param num_threads: The number of threads to use for processing URLs.
    :param assets: Assets to process.
    :param title: Title for the progress bar.
    """

    queue = Queue()
    batch_size = 200

    for i in range(0, len(assets), batch_size):
        users_batch = list(assets.values())[i:i + batch_size]
        queue.put(users_batch)

    # Create and start worker threads with a progress bar
    with tqdm(total=len(assets)/batch_size, desc=title) as progress_bar:
        threads = []
        for _ in range(num_threads):
            t = Thread(target=worker, args=(queue, progress_bar, gh_token))
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

    if "repo" in title.lower():
        write_csv_files(UNIQUE_REPOS, None, output_folder, only_repos=True)
    else:
        write_csv_files(None, UNIQUE_USERS, output_folder, only_users=True)



def main(output_folder, num_threads, gh_token):
    """
    Main function to process a file containing GitHub Archive URLs using multi-threading and write the results to CSV files.

    :param output_folder: The folder path where the final CSV files will be generated.
    :param num_threads: The number of threads to use for processing URLs.
    :param gh_token: Github token to use for API calls.
    """
    
    global UNIQUE_REPOS, UNIQUE_USERS

    repos = load_csv_repo_file(output_folder)
    UNIQUE_REPOS = repos
    process_github_assets(output_folder, num_threads, gh_token, repos, "Processing repositories")
    del repos

    users = load_csv_user_file(output_folder)
    UNIQUE_USERS = users
    process_github_assets(output_folder, num_threads, gh_token, users, "Processing users")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process GitHub Archive URLs and generate unique repositories and users CSV files.")
    parser.add_argument('output_folder', type=str, help="The path of the folder where the CSV files are generated.")
    parser.add_argument('-t', '--threads', type=int, default=10, help="Number of threads to use for processing Github assets.")
    parser.add_argument('-T', '--token', type=str, help="Github token to use for API calls.", required=True)

    args = parser.parse_args()
    main(args.output_folder, args.threads, args.token)
