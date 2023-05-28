import argparse
import csv
import os
import threading

from typing import List

from lib.classes import Repository, User
from lib.functions import get_repos_info, get_users_info, process_repos_in_batches, process_users_in_batches, count_lines, now_str
from threading import Lock
from time import sleep

# Locks for thread-safe writing to CSV files
REPOS_LOCK = Lock()
USERS_LOCK = Lock()
TOTAL_CHECKED = 0
TOTAL_LOCK = Lock()



def check_repos(repos:List[Repository], gh_token_or_file, csv_path):
    """
    Write delailed info about the Github repos

    :param repos: A list of Repository objects to check.
    :param gh_token_or_file: Github token or file with tokens.
    :param csv_path: The csv path to write the information extracted.
    :return: None
    """

    global TOTAL_CHECKED, TOTAL_LOCK, REPOS_LOCK
    repos_info = get_repos_info(repos, gh_token_or_file)

    if not repos_info:
        return

    with REPOS_LOCK:
        with open(csv_path, 'a', newline='', encoding='utf-8') as repos_csv_file:
            repos_csv_writer = csv.writer(repos_csv_file)
            
            for repo_full_name, repo_info in repos_info.items():

                for repo in repos:
                    if repo.full_name == repo_full_name:
                        break
                
                if repo_info["inexistent"]:
                    repo.stars = -1
                    repo.forks = -1
                    repo.watchers = -1
                    repo.archived = False
                    repo.disabled = False
                    repo.deleted = repo.deleted
                    repo.private = False if repo.deleted else True

                else:
                    repo.stars = repo_info['stargazers_count']
                    repo.forks = repo_info['forks_count']
                    repo.watchers = repo_info['watchers_count']
                    repo.archived = repo_info['archived']
                    repo.disabled = repo_info['disabled']
                    repo.deleted = False
                    repo.private = False
                
                repos_csv_writer.writerow([repo.full_name, repo.stars, repo.forks, repo.watchers, int(repo.deleted), int(repo.private), int(repo.archived), int(repo.disabled)])
    
    with TOTAL_LOCK:
        TOTAL_CHECKED += len(repos_info)
        print(f"{now_str()} Total assets checked: {TOTAL_CHECKED}", end='\r')


def check_users(users: List[User], gh_token_or_file, csv_path):
    """
    Write delailed info about the Github users

    :param users: A lis of User objects to check.
    :param gh_token_or_file: Github token or file with tokens.
    :param csv_path: The csv file to write the information extracted.
    :return: None
    """

    global TOTAL_CHECKED, TOTAL_LOCK, USERS_LOCK
    users_info = get_users_info(users, gh_token_or_file)

    if not users_info:
        return
    
    with USERS_LOCK:
        with open(csv_path, 'a', newline='', encoding='utf-8') as users_csv_file:
            users_csv_writer = csv.writer(users_csv_file)
            
            for username, user_info in users_info.items():
                # Get original csv user
                for user in users:
                    if user.username == username:
                        break
                
                if user_info["inexistent"]:
                    user.deleted=True

                else:
                    user.site_admin=user_info['site_admin']
                    user.hireable=user_info['hireable']
                    user.email=user_info['email']
                    user.company=user_info['company']
                    user.github_star=user_info['github_star']        

                #Filter empty repos in user.repos_collab
                user.repos_collab = list(filter(lambda item: item, user.repos_collab))
                users_csv_writer.writerow([user.username, ','.join(user.repos_collab), int(user.deleted), int(user.site_admin), int(user.hireable), user.email, user.company, int(user.github_star)])
    
    with TOTAL_LOCK:
        TOTAL_CHECKED += len(users_info)
        print(f"{now_str()} Total assets checked: {TOTAL_CHECKED}", end='\r')



def parse_github_assets(assets, gh_token_or_file, csv_path):
    """
    Parse a single GitHub assets and obtain details about it.

    :param assets: The Github assets.
    :param gh_token_or_file: Github token or file with tokens.
    :param csv_path: The csv path to write the information extracted.
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
        check_users(users, gh_token_or_file, csv_path)

    else:
        check_repos(repos, gh_token_or_file, csv_path)



def main(users_file, repos_file, output_folder, gh_token_or_file, file_tokens, batch_size, max_num_threads):
    """
    Main function to process csvs containing GitHub users and repos and write the results to CSV files.

    :param users_file: The folder path with the initial users csvs to load.
    :param repos_file: The folder path with the initial repos csvs to load.
    :param output_folder: The folder path where the final CSV files will be generated.
    :param gh_token_or_file: Github token to use for API calls.
    :param file_tokens: File containing Github tokens to use for API calls.
    :param batch_size: The size of the batch to ask Github graphql API at the same time.
    :param max_num_threads: The number of threads to use.
    :return: None
    """

    gh_token_or_file = gh_token_or_file if gh_token_or_file else file_tokens

    os.makedirs(output_folder, exist_ok=True)

    if repos_file:
        num_lines = count_lines(repos_file)
        print(f"Processing {num_lines} repositories")

        repos_generator = process_repos_in_batches(repos_file, batch_size, skip_header=False)
        repos_csv_path = os.path.join(output_folder, 'repos.csv')

        with open(repos_csv_path, 'a', newline='', encoding='utf-8') as repos_csv_file:
            repos_csv_writer = csv.writer(repos_csv_file)
            repos_csv_writer.writerow(['owner', 'repo', 'stars', 'forks', 'watchers', 'deleted', 'private', 'archived', 'disabled'])

        run_threads = []
        for batch_assets in repos_generator:
            while len(run_threads) >= max_num_threads:
                sleep(1)
                for check_t in run_threads:
                    if not check_t.is_alive():
                        run_threads.remove(check_t)

            x = threading.Thread(target=parse_github_assets, args=(batch_assets, gh_token_or_file, repos_csv_path))
            x.start()
            run_threads.append(x)


    if users_file:
        num_lines = count_lines(users_file)
        print(f"Processing {num_lines} users")

        users_csv_path = os.path.join(output_folder, 'users.csv')

        with open(users_csv_path, 'a', newline='', encoding='utf-8') as users_csv_file:
            users_csv_writer = csv.writer(users_csv_file)
            users_csv_writer.writerow(['user', 'repos_collab', 'deleted', 'site_admin', 'hireable', 'email', 'company', 'github_star'])
            
        run_threads = []
        users_generator = process_users_in_batches(users_file, batch_size, skip_header=False)
        for batch_assets in users_generator:
            while len(run_threads) >= max_num_threads:
                sleep(1)
                for check_t in run_threads:
                    if not check_t.is_alive():
                        run_threads.remove(check_t)

            x = threading.Thread(target=parse_github_assets, args=(batch_assets, gh_token_or_file, users_csv_path))
            x.start()
            run_threads.append(x)
            


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process GitHub Archive URLs and generate unique repositories and users CSV files.")
    parser.add_argument('-o', '--output-folder', type=str, help="The path of the folder where the CSV files are generated.", required=True)
    parser.add_argument('-u', '--users-file', type=str, help="The path of the file containing the users csv files.")
    parser.add_argument('-r', '--repos-file', type=str, help="The path of the file containing the repos csv files.")
    parser.add_argument('-b', '--batch-size', type=int, default=300, help="The size of the batch to ask Github graphql API at the same time.")
    parser.add_argument('-t', '--threads', type=int, default=5, help="The number of threads to use.")
    
    token_group = parser.add_mutually_exclusive_group(required=True)
    token_group.add_argument('-T', '--token', type=str, help="Github token to use for API calls.")
    token_group.add_argument('-f', '--file-tokens', type=str, help="File containing Github tokens to use for API calls.")
    
    args = parser.parse_args()

    if args.users_file is None and args.repos_file is None:
        parser.error("At least one of --users-file or --repos-file is required.")
    
    # If users_file, check the file exists
    if args.users_file is not None and not os.path.isfile(args.users_file):
        parser.error("The file specified by --users-file does not exist.")
    
    # If repos_file, check the file exists
    if args.repos_file is not None and not os.path.isfile(args.repos_file):
        parser.error("The file specified by --repos-file does not exist.")

    main(args.users_file, args.repos_file, args.output_folder, args.token, args.file_tokens, args.batch_size, args.threads)
