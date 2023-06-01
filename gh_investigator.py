import argparse
import csv
import os

from lib.functions import load_csv_repo_file_gen, load_csv_user_file_gen

def write_csv(output_folder, file_name, header, data):
    os.makedirs(output_folder,exist_ok=True)
    file_path = os.path.join(output_folder, file_name)

    with open(file_path, 'w', newline='', encoding='utf-8') as csv_file:
        csv_writer = csv.writer(csv_file)
        csv_writer.writerow(header)

        for row in data:
            csv_writer.writerow(row)

def write_sort_repos_by_stars(output_folder, repos):
    repos_with_stars = [repo for repo in repos if repo.stars > 500]
    sorted_repos = sorted(repos_with_stars, key=lambda repo: int(repo.stars), reverse=True)
    header = ['owner', 'repo', 'stars', 'forks', 'watchers', 'deleted', 'private', 'archived', 'disabled']
    data = [
        (owner, repo_name, repo.stars, repo.forks, repo.watchers, repo.deleted, repo.private, repo.archived, repo.disabled)
        for repo in sorted_repos
        for owner, repo_name in [repo.full_name.split('/')]
    ]
    write_csv(output_folder, 'repos_sorted_stars.csv', header, data)


def write_sort_repos_by_forks(output_folder, repos):
    repos_with_forks = [repo for repo in repos if repo.forks > 100]
    sorted_repos = sorted(repos_with_forks, key=lambda repo: int(repo.forks), reverse=True)
    header = ['owner', 'repo', 'stars', 'forks', 'watchers', 'deleted', 'private', 'archived', 'disabled']
    data = [
        (owner, repo_name, repo.stars, repo.forks, repo.watchers, repo.deleted, repo.private, repo.archived, repo.disabled)
        for repo in sorted_repos
        for owner, repo_name in [repo.full_name.split('/')]
    ]
    write_csv(output_folder, 'repos_sorted_forks.csv', header, data)

def write_sort_repos_by_watchers(output_folder, repos):
    repos_with_watchers = [repo for repo in repos if repo.watchers > 20]
    sorted_repos = sorted(repos_with_watchers, key=lambda repo: int(repo.watchers), reverse=True)
    header = ['owner', 'repo', 'stars', 'forks', 'watchers', 'deleted', 'private', 'archived', 'disabled']
    data = [
        (owner, repo_name, repo.stars, repo.forks, repo.watchers, repo.deleted, repo.private, repo.archived, repo.disabled)
        for repo in sorted_repos
        for owner, repo_name in [repo.full_name.split('/')]
    ]
    write_csv(output_folder, 'repos_sorted_watchers.csv', header, data)

def write_private_repos(output_folder, repos):
    private_repos = [repo for repo in repos if repo.private]
    header = ['owner', 'repo', 'stars', 'forks', 'watchers', 'deleted', 'private', 'archived', 'disabled']
    data = [
        (owner, repo_name, repo.stars, repo.forks, repo.watchers, repo.deleted, repo.private, repo.archived, repo.disabled)
        for repo in private_repos
        for owner, repo_name in [repo.full_name.split('/')]
    ]
    write_csv(output_folder, 'repos_private.csv', header, data)

def write_deleted_repos(output_folder, repos):
    private_repos = [repo for repo in repos if repo.deleted]
    header = ['owner', 'repo', 'stars', 'forks', 'watchers', 'deleted', 'private', 'archived', 'disabled']
    data = [
        (owner, repo_name, repo.stars, repo.forks, repo.watchers, repo.deleted, repo.private, repo.archived, repo.disabled)
        for repo in private_repos
        for owner, repo_name in [repo.full_name.split('/')]
    ]
    write_csv(output_folder, 'repos_deleted.csv', header, data)

def write_archived_repos(output_folder, repos):
    private_repos = [repo for repo in repos if repo.deleted]
    header = ['owner', 'repo', 'stars', 'forks', 'watchers', 'deleted', 'private', 'archived', 'disabled']
    data = [
        (owner, repo_name, repo.stars, repo.forks, repo.watchers, repo.deleted, repo.private, repo.archived, repo.disabled)
        for repo in private_repos
        for owner, repo_name in [repo.full_name.split('/')]
    ]
    write_csv(output_folder, 'repos_archived.csv', header, data)

def write_disabled_repos(output_folder, repos):
    private_repos = [repo for repo in repos if repo.deleted]
    header = ['owner', 'repo', 'stars', 'forks', 'watchers', 'deleted', 'private', 'archived', 'disabled']
    data = [
        (owner, repo_name, repo.stars, repo.forks, repo.watchers, repo.deleted, repo.private, repo.archived, repo.disabled)
        for repo in private_repos
        for owner, repo_name in [repo.full_name.split('/')]
    ]
    write_csv(output_folder, 'repos_disabled.csv', header, data)

def write_site_admin_users(output_folder, users):
    site_admin_users = [user for user in users if user.site_admin]
    header = ['user', 'repos_collab', 'deleted', 'site_admin', 'hireable', 'email', 'company', 'github_star']
    data = [
        (user.username, ','.join(user.repos_collab), user.deleted, user.site_admin, user.hireable, user.email, user.company, user.github_star)
        for user in site_admin_users
    ]
    write_csv(output_folder, 'users_site_admin.csv', header, data)

def write_deleted_users(output_folder, users):
    site_admin_users = [user for user in users if user.deleted]
    header = ['user', 'repos_collab', 'deleted', 'site_admin', 'hireable', 'email', 'company', 'github_star']
    data = [
        (user.username, ','.join(user.repos_collab), user.deleted, user.site_admin, user.hireable, user.email, user.company, user.github_star)
        for user in site_admin_users
    ]
    write_csv(output_folder, 'users_deleted.csv', header, data)

def write_hireable_users(output_folder, users):
    site_admin_users = [user for user in users if user.hireable]
    header = ['user', 'repos_collab', 'deleted', 'site_admin', 'hireable', 'email', 'company', 'github_star']
    data = [
        (user.username, ','.join(user.repos_collab), user.deleted, user.site_admin, user.hireable, user.email, user.company, user.github_star)
        for user in site_admin_users
    ]
    write_csv(output_folder, 'users_hireable.csv', header, data)

def write_github_star_users(output_folder, users):
    site_admin_users = [user for user in users if user.github_star]
    header = ['user', 'repos_collab', 'deleted', 'site_admin', 'hireable', 'email', 'company', 'github_star']
    data = [
        (user.username, ','.join(user.repos_collab), user.deleted, user.site_admin, user.hireable, user.email, user.company, user.github_star)
        for user in site_admin_users
    ]
    write_csv(output_folder, 'users_github_star.csv', header, data)

def write_email_users(output_folder, users):
    site_admin_users = [user for user in users if user.email]
    header = ['user', 'repos_collab', 'deleted', 'site_admin', 'hireable', 'email', 'company', 'github_star']
    data = [
        (user.username, ','.join(user.repos_collab), user.deleted, user.site_admin, user.hireable, user.email, user.company, user.github_star)
        for user in site_admin_users
    ]
    write_csv(output_folder, 'users_email.csv', header, data)

def write_company_users(output_folder, users):
    site_admin_users = [user for user in users if user.company]
    header = ['user', 'repos_collab', 'deleted', 'site_admin', 'hireable', 'email', 'company', 'github_star']
    data = [
        (user.username, ','.join(user.repos_collab), user.deleted, user.site_admin, user.hireable, user.email, user.company, user.github_star)
        for user in site_admin_users
    ]
    write_csv(output_folder, 'users_company.csv', header, data)

def main(users_file, repos_file, logs_folder, output_folder):
    if logs_folder:
        temp_users_file = os.path.join(output_folder, "users.csv")
        if os.path.isfile(temp_users_file):
            users_file = temp_users_file
        
        temp_repos_file = os.path.join(output_folder, "repos.csv")
        if os.path.isfile(temp_repos_file):
            repos_file = temp_repos_file
    
    if not repos_file and not users_file:
        print("No input files found")
        return

    if repos_file:
        write_sort_repos_by_stars(output_folder, load_csv_repo_file_gen(repos_file))
        write_sort_repos_by_forks(output_folder, load_csv_repo_file_gen(repos_file))
        write_sort_repos_by_watchers(output_folder, load_csv_repo_file_gen(repos_file))
        write_private_repos(output_folder, load_csv_repo_file_gen(repos_file))
        write_deleted_repos(output_folder, load_csv_repo_file_gen(repos_file))
        write_archived_repos(output_folder, load_csv_repo_file_gen(repos_file))
        write_disabled_repos(output_folder, load_csv_repo_file_gen(repos_file))

    if users_file:
        write_site_admin_users(output_folder, load_csv_user_file_gen(users_file))
        write_deleted_users(output_folder, load_csv_user_file_gen(users_file))
        write_hireable_users(output_folder, load_csv_user_file_gen(users_file))
        write_github_star_users(output_folder, load_csv_user_file_gen(users_file))
        write_email_users(output_folder, load_csv_user_file_gen(users_file))
        write_company_users(output_folder, load_csv_user_file_gen(users_file))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Use the generated CSV files to get interesting information.")
    parser.add_argument('-u', '--users-file', type=str, help="The path of the file containing the users csv files.")
    parser.add_argument('-r', '--repos-file', type=str, help="The path of the file containing the repos csv files.")
    parser.add_argument('-i', '--logs-folder', type=str, help="The path of the folder containing the users and/or repos csvs.")
    parser.add_argument('-o', '--output-folder', type=str, help="The path of the folder where the CSV files will be generated.", required=True)

    args = parser.parse_args()
    if args.users_file is None and args.repos_file is None and args.logs_folder is None:
        parser.error("At least one of --users-file or --repos-file or --logs-folder is required.")
    
    # If users_file, check the file exists
    if args.users_file is not None and not os.path.isfile(args.users_file):
        parser.error("The file specified by --users-file does not exist.")
    
    # If repos_file, check the file exists
    if args.repos_file is not None and not os.path.isfile(args.repos_file):
        parser.error("The file specified by --repos-file does not exist.")
    
    # If logs_folder, check the folder exists
    if args.logs_folder is not None and not os.path.isdir(args.logs_folder):
        parser.error("The folder specified by --logs-folder does not exist.")

    main(args.users_file, args.repos_file, args.logs_folder, args.output_folder)
