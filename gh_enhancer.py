import argparse
import csv
import os

from typing import List

from lib.classes import Repository, User
from lib.functions import get_repos_info, get_users_info, process_repos_in_batches, process_users_in_batches, count_lines



def check_repos(repos:List[Repository], gh_token_or_file, csv_fd):
    """
    Get info about the Github repo

    :param repo: The Repository object to check.
    :return: None
    """


    repos_info = get_repos_info(repos, gh_token_or_file)

    if not repos_info:
        return

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
        
            owner, repo_name = repo.full_name.split('/')
            csv_fd.writerow([owner, repo_name, repo.stars, repo.forks, repo.watchers, int(repo.deleted), int(repo.private), int(repo.archived), int(repo.disabled)])


def check_users(users: List[User], gh_token_or_file, csv_fd):
    """
    Write delailed info about the Github usernames

    :param users: A lis of User objects to check.
    :param gh_token_or_file: Github token or file with tokens.
    :param csv_fd: The fd to write the information extracted.
    :return: None
    """

    users_info = get_users_info(users, gh_token_or_file)

    if not users_info:
        return
    
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

        csv_fd.writerow([user.username, ','.join(user.repos_collab), int(user.deleted), int(user.site_admin), int(user.hireable), user.email, user.company, int(user.github_star)])



def parse_github_assets(assets, gh_token_or_file, csv_fd):
    """
    Parse a single GitHub assets and obtain details about it.

    :param assets: The Github assets.
    :param gh_token_or_file: Github token or file with tokens.
    :param csv_fd: The fd to write the information extracted.
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
        check_users(users, gh_token_or_file, csv_fd)

    else:
        check_repos(repos, gh_token_or_file, csv_fd)
        

def main(users_file, repos_file, output_folder, gh_token_or_file, file_tokens, batch_size):
    """
    Main function to process csvs containing GitHub users and repos and write the results to CSV files.

    :param users_file: The folder path with the initial users csvs to load.
    :param repos_file: The folder path with the initial repos csvs to load.
    :param output_folder: The folder path where the final CSV files will be generated.
    :param gh_token_or_file: Github token to use for API calls.
    """
    
    t = gh_token_or_file if gh_token_or_file else file_tokens

    os.makedirs(output_folder, exist_ok=True)

    if repos_file:
        num_lines = count_lines(repos_file)
        print(f"Processing {num_lines} repositories")
        for batch_repos in process_repos_in_batches(repos_file, batch_size):
            repos_csv_path = os.path.join(output_folder, 'repos.csv')
            
            with open(repos_csv_path, 'w', newline='', encoding='utf-8') as repos_csv_file:
                repos_csv_writer = csv.writer(repos_csv_file)
                repos_csv_writer.writerow(['owner', 'repo', 'stars', 'forks', 'watchers', 'deleted', 'private', 'archived', 'disabled'])
                
                parse_github_assets(batch_repos, t, repos_csv_writer)
        

    if users_file:
        num_lines = count_lines(users_file)
        print(f"Processing {num_lines} users")
        for batch_users in process_users_in_batches(users_file, batch_size):
            users_csv_path = os.path.join(output_folder, 'users.csv')

            with open(users_csv_path, 'w', newline='', encoding='utf-8') as users_csv_file:
                users_csv_writer = csv.writer(users_csv_file)
                users_csv_writer.writerow(['user', 'repos_collab', 'deleted', 'site_admin', 'hireable', 'email', 'company', 'github_star'])    

                parse_github_assets(batch_users, t, users_csv_writer)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process GitHub Archive URLs and generate unique repositories and users CSV files.")
    parser.add_argument('-o', '--output-folder', type=str, help="The path of the folder where the CSV files are generated.", required=True)
    parser.add_argument('-u', '--users-file', type=str, help="The path of the file containing the users csv files.")
    parser.add_argument('-r', '--repos-file', type=str, help="The path of the file containing the repos csv files.")
    parser.add_argument('-b', '--batch-size', type=int, default=200, help="The size of the batch to ask Github graphql API at the same time.")
    
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

    main(args.users_file, args.repos_file, args.output_folder, args.token, args.file_tokens, args.batch_size)
