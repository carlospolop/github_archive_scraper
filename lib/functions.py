import csv
import gzip
import json
import os
import random
import requests
import sys
import time

from typing import List
from datetime import datetime

from .classes import Repository, User


GITHUB_API_BASE_URL = "https://api.github.com"
GITHUB_GRAPHQL_API_URL = "https://api.github.com/graphql"

def now_str():
    now = datetime.now()
    current_time = now.strftime("%H:%M:%S")
    return current_time

def splitlines_generator(text):
    start = 0
    for end, char in enumerate(text):
        if char == '\n':
            yield text[start:end]
            start = end + 1
    if start < len(text):
        yield text[start:]

def download_file(url, cont=0):
    """
    Download the content of the specified URL.

    :param url: The URL to download the content from.
    :return: The content of the URL as bytes.
    """
    try:
        response = requests.get(url)
    except Exception:
        time.sleep(20)
        if cont < 5:
            return download_file(url, cont+1)
        else:
            print("Error downloading " + url)
            return None

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

def get_next_token(gh_token_file, token):
    """
    Get a GitHub token from the provided input.

    :param get_next_token: Given a github token file, and a token from that file, return the next token in the file.
    :return: A GitHub token.
    """
    
    with open(gh_token_file, 'r') as token_file:
        tokens = token_file.readlines()
        token_index = tokens.index(token)
        if token_index == len(tokens) - 1:
            return tokens[0].strip()
        else:
            return tokens[token_index + 1].strip()


def get_repos_info(repos: List[Repository], gh_token_or_file, cont=0, old_key="", gh_token=""):
    """
    Fetch the information of multiple GitHub repositories.

    :param repos: A list of Repositories.
    :param gh_token: The GitHub token to use for authentication.
    :return: A list of dictionaries containing the repository information, or None if the request fails.
    """

    # A gh_token will be given if a rate limit error of a different one is thrown
    if not gh_token:
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
            print(f"{now_str()} Too many retries with {repos_full_names}, skipping")
            return None
        time.sleep(30)
        return get_repos_info(repos, gh_token_or_file, cont=cont+1)

    if response.status_code == 200:
        result = json.loads(response.text)

        if "rate limit" in str(result.get("errors", {})).lower():
            # Set initial old key if not set
            if not old_key:
                old_key = gh_token
            
            next_token = get_next_token(gh_token_or_file, gh_token) if "/" in gh_token_or_file else gh_token
            if old_key == next_token:
                print(f"{now_str()} Rate limit exceeded with all tokens in repos, sleeping 15 mins")
                time.sleep(15*60)
            
            return get_repos_info(repos, gh_token_or_file, old_key=old_key, gh_token=next_token)
        
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
            # Set initial old key if not set
            if not old_key:
                old_key = gh_token
            
            next_token = get_next_token(gh_token_or_file, gh_token) if "/" in gh_token_or_file else gh_token
            if old_key == next_token:
                print(f"{now_str()} Rate limit exceeded with all tokens in repos, sleeping 15 mins")
                time.sleep(15*60)
            
            return get_repos_info(repos, gh_token_or_file, old_key=old_key, gh_token=next_token)
        
        else:
            print(f"Request failed with status code {response.status_code} with text {response.text}")
            return None

def get_users_info(users: List[User], gh_token_or_file, cont=0, old_key="", gh_token=""):
    """
    Fetch the information of a GitHub user.

    :param users: List of Users.
    :param gh_token: The GitHub token to use for authentication.
    :return: A dictionary containing the user information, or None if the request fails.
    """

    if not gh_token:
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
            print(f"{now_str()} Too many retries with {usernames}, skipping")
            return None
        time.sleep(30)
        return get_users_info(users, gh_token_or_file, cont=cont+1)

    if response.status_code == 200:
        result = json.loads(response.text)
        
        if "rate limit" in str(result.get("errors", {})).lower():
            # Set initial old key if not set
            if not old_key:
                old_key = gh_token
            
            next_token = get_next_token(gh_token_or_file, gh_token) if "/" in gh_token_or_file else gh_token
            if old_key == next_token:
                print(f"{now_str()} Rate limit exceeded with all tokens in users, sleeping 15 mins")
                time.sleep(15*60)
            
            return get_users_info(users, gh_token_or_file, old_key=old_key, gh_token=next_token)
                
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
            print(f"{now_str()} Too many 502 with {usernames}, skipping")
            return None
        time.sleep(30)
        return get_users_info(users, gh_token_or_file, cont=cont+1)
    
    else:
        if "rate limit" in str(response.text):
            # Set initial old key if not set
            if not old_key:
                old_key = gh_token
            
            next_token = get_next_token(gh_token_or_file, gh_token) if "/" in gh_token_or_file else gh_token
            if old_key == next_token:
                print(f"{now_str()} Rate limit exceeded with all tokens in users, sleeping 15 mins")
                time.sleep(15*60)
            
            return get_users_info(users, gh_token_or_file, old_key=old_key, gh_token=next_token)
        
        else:
            print(f"{now_str()} Request failed with status code {response.status_code} with text {response.text}")
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
                repos_csv_writer.writerow([
                    owner, 
                    repo_name, 
                    repo.stars if repo.stars > 0 else "",
                    repo.forks if repo.forks > 0 else "",
                    repo.watchers if repo.watchers > 0 else "",
                    int(repo.deleted) if repo.deleted else "",
                    int(repo.private) if repo.private else "", 
                    int(repo.archived) if repo.archived else "", 
                    int(repo.disabled) if repo.disabled else ""
                ])

    if users:
        users_csv_path = os.path.join(output_folder, 'users.csv')
        
        with open(users_csv_path, 'w', newline='', encoding='utf-8') as users_csv_file:
            users_csv_writer = csv.writer(users_csv_file)
            users_csv_writer.writerow(['user', 'repos_collab', 'deleted', 'site_admin', 'hireable', 'email', 'company', 'github_star'])            

            for user in users.values():
                users_csv_writer.writerow([
                    user.username, ','.join(user.repos_collab),
                    int(user.deleted) if user.deleted else "",
                    int(user.site_admin) if user.site_admin else "",
                    int(user.hireable) if user.hireable else "",
                    user.email,
                    user.company,
                    int(user.github_star) if user.github_star else "",
                ])


def load_csv_repo_file_gen(file_path, skip_header=True):
    """
    Load repositories and users from CSV files in the specified folder.

    :param file_path: The file path where the repos CSV files are located.
    :return: A tuple containing two sets: one for repositories and one for users.
    """

    csv.field_size_limit(sys.maxsize)

    with open(file_path, 'r', newline='', encoding='utf-8') as repos_csv_file:
        repos_csv_reader = csv.reader(repos_csv_file)

        if skip_header:
            next(repos_csv_reader)  # Skip header
        

        for row in repos_csv_reader:
            owner, repo_name, stars, forks, watchers, deleted, private, archived, disabled = row
            full_name = f"{owner}/{repo_name}"
            repo = Repository(full_name, int(stars), int(forks), int(watchers), bool(int(deleted)), bool(int(private)), bool(int(archived)), bool(int(disabled)))
            yield repo

def process_repos_in_batches(file_path, batch_size=300, skip_header=True):
    cont = 0
    batch_of_repos = []
    for repo in load_csv_repo_file_gen(file_path, skip_header=skip_header):
        batch_of_repos.append(repo)
        
        if len(batch_of_repos) == batch_size:
            cont += 1
            yield batch_of_repos

            batch_of_repos = []  # Create a new list

    if batch_of_repos:
        print("Final batch of repos")
        yield batch_of_repos


def load_csv_user_file_gen(file_path, skip_header=True):
    """
    Load users from CSV files in the specified folder.

    :param file_path: The file path where the users CSV files are located.
    :return: A tuple containing two sets: one for repositories and one for users.
    """

    csv.field_size_limit(sys.maxsize)

    with open(file_path, 'r', newline='', encoding='utf-8') as users_csv_file:

        users_csv_reader = csv.reader(users_csv_file)

        if skip_header:
            next(users_csv_reader)  # Skip header

        for row in users_csv_reader:
            username, repos_collab, deleted, site_admin, hireable, email, company, github_star = row
            repos_collab = repos_collab.split(',')
            user = User(username, repos_collab, bool(int(deleted)), bool(int(site_admin)), bool(int(hireable)), email, company, bool(int(github_star)))
            yield user

def process_users_in_batches(file_path, batch_size=300, skip_header=True):
    cont = 0
    batch_of_users = []
    for user in load_csv_user_file_gen(file_path, skip_header=skip_header):
        batch_of_users.append(user)
        
        if len(batch_of_users) == batch_size:
            cont += 1
            yield batch_of_users

            batch_of_users = []  # Create a new list

    # Process the remaining repos (less than 300) if any
    if batch_of_users:
        print("Final batch of users")
        yield batch_of_users

def count_lines(file_path):
    with open(file_path, 'r') as file:
        lines = 0
        for _ in file:
            lines += 1
    return lines