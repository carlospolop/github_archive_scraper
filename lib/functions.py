import csv
import gzip
import json
import os
import random
import requests
import sys
import time

from typing import List

from .classes import Repository, User


GITHUB_API_BASE_URL = "https://api.github.com"
GITHUB_GRAPHQL_API_URL = "https://api.github.com/graphql"


def download_file(url):
    """
    Download the content of the specified URL.

    :param url: The URL to download the content from.
    :return: The content of the URL as bytes.
    """
    response = requests.get(url)
    return response.content

def decompress_gz(content):
    """
    Decompress the given GZIP content.

    :param content: The GZIP content to decompress.
    :return: The decompressed content as bytes.
    """

    try:
        return gzip.decompress(content)
    except gzip.BadGzipFile:
        if not "NoSuchKey" in content.decode("utf-8"):
            print("Unexpected no gzip content: " + content.decode("utf-8"))
        return None


def get_github_token(gh_token_or_file):
    """
    Get a GitHub token from the provided input.

    :param gh_token_or_file: A GitHub token or the path to a file containing tokens.
    :return: A GitHub token.
    """
    
    if os.path.isfile(gh_token_or_file):
        with open(gh_token_or_file, 'r') as token_file:
            tokens = token_file.readlines()
            return random.choice(tokens).strip()
    else:
        return gh_token_or_file


def get_repos_info(repos: List[Repository], gh_token_or_file, cont=0, old_key=""):
    """
    Fetch the information of multiple GitHub repositories.

    :param repos: A list of Repositories.
    :param gh_token: The GitHub token to use for authentication.
    :return: A list of dictionaries containing the repository information, or None if the request fails.
    """

    gh_token = get_github_token(gh_token_or_file)

    repos_full_names = [repo.full_name for repo in repos]

    query_template = '''
        query{index}: repository(owner: "{owner}", name: "{repo}") {{
            nameWithOwner
            stargazerCount
            forks {{
                totalCount
            }}
            watchers {{
                totalCount
            }}
            isArchived
            isDisabled
        }}
    '''
    query_parts = []

    for index, repo_full_name in enumerate(repos_full_names):
        owner, repo = repo_full_name.split('/')
        query_parts.append(query_template.format(index=index, owner=owner, repo=repo))

    query = 'query {{ {} }}'.format(' '.join(query_parts))

    headers = {"Authorization": f"Bearer {gh_token}"}

    try:
        response = requests.post(GITHUB_GRAPHQL_API_URL, json={"query": query}, headers=headers)
    except (requests.exceptions.ChunkedEncodingError, requests.exceptions.ConnectionError):
        if cont > 3:
            print(f"Too many retries with {repos_full_names}, skipping")
            return None
        time.sleep(30)
        return get_repos_info(repos, gh_token_or_file, cont=cont+1)

    if response.status_code == 200:
        result = json.loads(response.text)
        data = result['data']

        repos_info = dict()
        for repo_key in data:
            repo_data = data[repo_key]
            if repo_data:
                repo_info = {
                    "stargazers_count": repo_data["stargazerCount"],
                    "forks_count": repo_data["forks"]["totalCount"],
                    "watchers_count": repo_data["watchers"]["totalCount"],
                    "archived": repo_data["isArchived"],
                    "disabled": repo_data["isDisabled"],
                    "inexistent": False
                }
            else:
                repo_info = {
                    "inexistent": True
                }
            repo_full_name = repos_full_names[int(repo_key.replace("query",""))]
            repos_info[repo_full_name] = repo_info

        return repos_info
    
    elif response.status_code == 502:
        if cont > 3:
            print(f"Too many 502 with {repos_full_names}, skipping")
            return None
        time.sleep(30)
        return get_users_info(repos, gh_token_or_file, cont=cont+1)
    
    else:
        if "rate limit" in str(response.text):
            if old_key:
                print("Rate limit exceeded, sleeping 5 mins")
                time.sleep(5*60)
            return get_repos_info(repos, gh_token_or_file, old_key=gh_token)
        
        else:
            print(f"Request failed with status code {response.status_code} with text {response.text}")
            return None

def get_users_info(users: List[User], gh_token_or_file, cont=0, old_key=""):
    """
    Fetch the information of a GitHub user.

    :param users: List of Users.
    :param gh_token: The GitHub token to use for authentication.
    :return: A dictionary containing the user information, or None if the request fails.
    """

    gh_token = get_github_token(gh_token_or_file)        

    usernames = [user.username for user in users]

    query_template = '''
        query{index}: user(login: "{username}") {{
            isSiteAdmin
            isHireable
            isGitHubStar
            email
            company
        }}
    '''
    query_parts = []

    for index, username in enumerate(usernames):
        query_parts.append(query_template.format(index=index, username=username))

    query = 'query {{ {} }}'.format(' '.join(query_parts))

    headers = {"Authorization": f"Bearer {gh_token}"}

    try:
        response = requests.post(GITHUB_GRAPHQL_API_URL, json={"query": query}, headers=headers)
    except (requests.exceptions.ChunkedEncodingError, requests.exceptions.ConnectionError):
        if cont > 3:
            print(f"Too many retries with {usernames}, skipping")
            return None
        time.sleep(30)
        return get_users_info(users, gh_token_or_file, cont=cont+1)

    if response.status_code == 200:
        result = json.loads(response.text)
        data = result['data']

        users_info = dict()
        for user_key in data:
            user_data = data[user_key]
            if user_data:
                repo_info = {
                    "site_admin": user_data["isSiteAdmin"],
                    "hireable": user_data["isHireable"],
                    "email": user_data["email"],
                    "company": user_data["company"],
                    "github_star": user_data["isGitHubStar"],
                    "inexistent": False
                }
            else:
                repo_info = {
                    "inexistent": True
                }
            username = usernames[int(user_key.replace("query",""))]
            users_info[username] = repo_info

        return users_info
    
    elif response.status_code == 502:
        if cont > 3:
            print(f"Too many 502 with {usernames}, skipping")
            return None
        time.sleep(30)
        return get_users_info(users, gh_token_or_file, cont=cont+1)
    
    else:
        if "rate limit" in str(response.text):
            if old_key:
                print("Rate limit exceeded, sleeping 5 mins")
                time.sleep(5*60)
            return get_repos_info(users, gh_token_or_file, old_key=gh_token)
        
        else:
            print(f"Request failed with status code {response.status_code} with text {response.text}")
            return None

def read_urls_from_file(file_path):
    """
    Read URLs from a file, where each line in the file contains a single URL.

    :param file_path: The path to the file containing the URLs.
    :return: A list of URLs.
    """
    with open(file_path, 'r') as file:
        urls = [line.strip() for line in file]
    return urls


def write_csv_files(repos, users, output_folder):
    """
    Write the unique repositories and users to CSV files in the specified output folder.

    :param repos: A set of unique repositories.
    :param users: A set of unique users.
    :param output_folder: The folder path where the final CSV files will be generated.
    """
    os.makedirs(output_folder, exist_ok=True)

    if repos:
        repos_csv_path = os.path.join(output_folder, 'repos.csv')
        
        with open(repos_csv_path, 'w', newline='', encoding='utf-8') as repos_csv_file:
            repos_csv_writer = csv.writer(repos_csv_file)
            repos_csv_writer.writerow(['owner', 'repo', 'stars', 'forks', 'watchers', 'deleted', 'private', 'archived', 'disabled'])
            for repo in repos.values():
                owner, repo_name = repo.full_name.split('/')
                repos_csv_writer.writerow([owner, repo_name, repo.stars, repo.forks, repo.watchers, int(repo.deleted), int(repo.private), int(repo.archived), int(repo.disabled)])

    if users:
        users_csv_path = os.path.join(output_folder, 'users.csv')
        
        with open(users_csv_path, 'w', newline='', encoding='utf-8') as users_csv_file:
            users_csv_writer = csv.writer(users_csv_file)
            users_csv_writer.writerow(['user', 'repos_collab', 'deleted', 'site_admin', 'hireable', 'email', 'company', 'github_star'])            

            for user in users.values():
                # CSV raw limit is 131072 characters, so we truncate the repos_collab field
                users_csv_writer.writerow([user.username, ','.join(list(user.repos_collab)[:5000]), int(user.deleted), int(user.site_admin), int(user.hireable), user.email, user.company, int(user.github_star)])

def load_csv_repo_file(output_folder):
    """
    Load repositories and users from CSV files in the specified folder.

    :param output_folder: The folder path where the CSV files are located.
    :param as_generator: Yield each repository instead of returning a list.
    :return: A tuple containing two sets: one for repositories and one for users.
    """
    repos_csv_path = os.path.join(output_folder, 'repos.csv')

    repos = dict()

    csv.field_size_limit(sys.maxsize)

    with open(repos_csv_path, 'r', newline='', encoding='utf-8') as repos_csv_file:
        repos_csv_reader = csv.reader(repos_csv_file)
        next(repos_csv_reader)  # Skip header
        

        for row in repos_csv_reader:
            owner, repo_name, stars, forks, watchers, deleted, private, archived, disabled = row
            full_name = f"{owner}/{repo_name}"
            repo = Repository(full_name, int(stars), int(forks), int(watchers), bool(int(deleted)), bool(int(private)), bool(int(archived)), bool(int(disabled)))
            repos[repo.full_name] = repo

    return repos

def load_csv_repo_file_gen(output_folder):
    """
    Load repositories and users from CSV files in the specified folder.

    :param output_folder: The folder path where the CSV files are located.
    :param as_generator: Yield each repository instead of returning a list.
    :return: A tuple containing two sets: one for repositories and one for users.
    """
    repos_csv_path = os.path.join(output_folder, 'repos.csv')

    repos = dict()

    csv.field_size_limit(sys.maxsize)

    with open(repos_csv_path, 'r', newline='', encoding='utf-8') as repos_csv_file:
        repos_csv_reader = csv.reader(repos_csv_file)
        next(repos_csv_reader)  # Skip header
        

        for row in repos_csv_reader:
            owner, repo_name, stars, forks, watchers, deleted, private, archived, disabled = row
            full_name = f"{owner}/{repo_name}"
            repo = Repository(full_name, int(stars), int(forks), int(watchers), bool(int(deleted)), bool(int(private)), bool(int(archived)), bool(int(disabled)))
            yield repo

def load_csv_user_file(output_folder, as_generator=False):
    """
    Load users from CSV files in the specified folder.

    :param output_folder: The folder path where the CSV files are located.
    :param as_generator: Yield each repository instead of returning a list.
    :return: A tuple containing two sets: one for repositories and one for users.
    """
    users_csv_path = os.path.join(output_folder, 'users.csv')

    repos = dict()
    users = dict()

    csv.field_size_limit(sys.maxsize)

    with open(users_csv_path, 'r', newline='', encoding='utf-8') as users_csv_file:

        users_csv_reader = csv.reader(users_csv_file)
        next(users_csv_reader)  # Skip header

        for row in users_csv_reader:
            username, repos_collab, deleted, site_admin, hireable, email, company, github_star = row
            repos_collab = repos_collab.split(',')
            user = User(username, repos_collab, bool(int(deleted)), bool(int(site_admin)), bool(int(hireable)), email, company, bool(int(github_star)))
            users[user.username] = user

    return users

def load_csv_user_file_gen(output_folder):
    """
    Load users from CSV files in the specified folder.

    :param output_folder: The folder path where the CSV files are located.
    :param as_generator: Yield each repository instead of returning a list.
    :return: A tuple containing two sets: one for repositories and one for users.
    """
    users_csv_path = os.path.join(output_folder, 'users.csv')

    repos = dict()
    users = dict()

    csv.field_size_limit(sys.maxsize)

    with open(users_csv_path, 'r', newline='', encoding='utf-8') as users_csv_file:

        users_csv_reader = csv.reader(users_csv_file)
        next(users_csv_reader)  # Skip header

        for row in users_csv_reader:
            username, repos_collab, deleted, site_admin, hireable, email, company, github_star = row
            repos_collab = repos_collab.split(',')
            user = User(username, repos_collab, bool(int(deleted)), bool(int(site_admin)), bool(int(hireable)), email, company, bool(int(github_star)))
            yield user
