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

def write_sort_repos_by_stars(output_folder, minimum_stars, repos):
    repos_with_stars = [repo for repo in repos if repo.stars > minimum_stars]
    sorted_repos = sorted(repos_with_stars, key=lambda repo: int(repo.stars), reverse=True)
    header = ['owner', 'repo', 'stars', 'forks', 'watchers', 'deleted', 'private', 'archived', 'disabled']
    data = [
        (owner, repo_name, repo.stars, repo.forks, repo.watchers, repo.deleted, repo.private, repo.archived, repo.disabled)
        for repo in sorted_repos
        for owner, repo_name in [repo.full_name.split('/')]
    ]
    write_csv(output_folder, 'repos_sorted_stars.csv', header, data)
    print("[+] Repos sorted by stars written to repos_sorted_stars.csv")


def write_sort_repos_by_forks(output_folder, minimum_forks, repos):
    repos_with_forks = [repo for repo in repos if repo.forks > minimum_forks]
    sorted_repos = sorted(repos_with_forks, key=lambda repo: int(repo.forks), reverse=True)
    header = ['owner', 'repo', 'stars', 'forks', 'watchers', 'deleted', 'private', 'archived', 'disabled']
    data = [
        (owner, repo_name, repo.stars, repo.forks, repo.watchers, repo.deleted, repo.private, repo.archived, repo.disabled)
        for repo in sorted_repos
        for owner, repo_name in [repo.full_name.split('/')]
    ]
    write_csv(output_folder, 'repos_sorted_forks.csv', header, data)
    print("[+] Repos sorted by forks written to repos_sorted_forks.csv")

def write_sort_repos_by_watchers(output_folder, minimum_watchers, repos):
    repos_with_watchers = [repo for repo in repos if repo.watchers > minimum_watchers]
    sorted_repos = sorted(repos_with_watchers, key=lambda repo: int(repo.watchers), reverse=True)
    header = ['owner', 'repo', 'stars', 'forks', 'watchers', 'deleted', 'private', 'archived', 'disabled']
    data = [
        (owner, repo_name, repo.stars, repo.forks, repo.watchers, repo.deleted, repo.private, repo.archived, repo.disabled)
        for repo in sorted_repos
        for owner, repo_name in [repo.full_name.split('/')]
    ]
    write_csv(output_folder, 'repos_sorted_watchers.csv', header, data)
    print("[+] Repos sorted by watchers written to repos_sorted_watchers.csv")

def write_private_repos(output_folder, repos):
    private_repos = [repo for repo in repos if repo.private]
    header = ['owner', 'repo', 'stars', 'forks', 'watchers', 'deleted', 'private', 'archived', 'disabled']
    data = [
        (owner, repo_name, repo.stars, repo.forks, repo.watchers, repo.deleted, repo.private, repo.archived, repo.disabled)
        for repo in private_repos
        for owner, repo_name in [repo.full_name.split('/')]
    ]
    write_csv(output_folder, 'repos_private.csv', header, data)
    print("[+] Private repos written to repos_private.csv")

def write_deleted_repos(output_folder, repos):
    private_repos = [repo for repo in repos if repo.deleted]
    header = ['owner', 'repo', 'stars', 'forks', 'watchers', 'deleted', 'private', 'archived', 'disabled']
    data = [
        (owner, repo_name, repo.stars, repo.forks, repo.watchers, repo.deleted, repo.private, repo.archived, repo.disabled)
        for repo in private_repos
        for owner, repo_name in [repo.full_name.split('/')]
    ]
    write_csv(output_folder, 'repos_deleted.csv', header, data)
    print("[+] Deleted repos written to repos_deleted.csv")

def write_archived_repos(output_folder, repos):
    private_repos = [repo for repo in repos if repo.archived]
    header = ['owner', 'repo', 'stars', 'forks', 'watchers', 'deleted', 'private', 'archived', 'disabled']
    data = [
        (owner, repo_name, repo.stars, repo.forks, repo.watchers, repo.deleted, repo.private, repo.archived, repo.disabled)
        for repo in private_repos
        for owner, repo_name in [repo.full_name.split('/')]
    ]
    write_csv(output_folder, 'repos_archived.csv', header, data)
    print("[+] Archived repos written to repos_archived.csv")

def write_disabled_repos(output_folder, repos):
    private_repos = [repo for repo in repos if repo.disabled]
    header = ['owner', 'repo', 'stars', 'forks', 'watchers', 'deleted', 'private', 'archived', 'disabled']
    data = [
        (owner, repo_name, repo.stars, repo.forks, repo.watchers, repo.deleted, repo.private, repo.archived, repo.disabled)
        for repo in private_repos
        for owner, repo_name in [repo.full_name.split('/')]
    ]
    write_csv(output_folder, 'repos_disabled.csv', header, data)
    print("[+] Disabled repos written to repos_disabled.csv")

def write_site_admin_users(output_folder, users):
    site_admin_users = [user for user in users if user.site_admin]
    header = ['user', 'repos_collab', 'deleted', 'site_admin', 'hireable', 'email', 'company', 'github_star']
    data = [
        (user.username, ','.join(user.repos_collab), user.deleted, user.site_admin, user.hireable, user.email, user.company, user.github_star)
        for user in site_admin_users
    ]
    write_csv(output_folder, 'users_site_admin.csv', header, data)
    print("[+] Site admin users written to users_site_admin.csv")

def write_deleted_users(output_folder, users):
    site_admin_users = [user for user in users if user.deleted]
    header = ['user', 'repos_collab', 'deleted', 'site_admin', 'hireable', 'email', 'company', 'github_star']
    data = [
        (user.username, ','.join(user.repos_collab), user.deleted, user.site_admin, user.hireable, user.email, user.company, user.github_star)
        for user in site_admin_users
    ]
    write_csv(output_folder, 'users_deleted.csv', header, data)
    print("[+] Deleted users written to users_deleted.csv")

def write_hireable_users(output_folder, users):
    site_admin_users = [user for user in users if user.hireable]
    header = ['user', 'repos_collab', 'deleted', 'site_admin', 'hireable', 'email', 'company', 'github_star']
    data = [
        (user.username, ','.join(user.repos_collab), user.deleted, user.site_admin, user.hireable, user.email, user.company, user.github_star)
        for user in site_admin_users
    ]
    write_csv(output_folder, 'users_hireable.csv', header, data)
    print("[+] Hireable users written to users_hireable.csv")

def write_github_star_users(output_folder, users):
    site_admin_users = [user for user in users if user.github_star]
    header = ['user', 'repos_collab', 'deleted', 'site_admin', 'hireable', 'email', 'company', 'github_star']
    data = [
        (user.username, ','.join(user.repos_collab), user.deleted, user.site_admin, user.hireable, user.email, user.company, user.github_star)
        for user in site_admin_users
    ]
    write_csv(output_folder, 'users_github_star.csv', header, data)
    print("[+] Github star users written to users_github_star.csv")

def write_email_users(output_folder, users):
    site_admin_users = [user for user in users if user.email]
    header = ['user', 'repos_collab', 'deleted', 'site_admin', 'hireable', 'email', 'company', 'github_star']
    data = [
        (user.username, ','.join(user.repos_collab), user.deleted, user.site_admin, user.hireable, user.email, user.company, user.github_star)
        for user in site_admin_users
    ]
    write_csv(output_folder, 'users_email.csv', header, data)
    print("[+] Email users written to users_email.csv")

def write_company_users(output_folder, users):
    site_admin_users = [user for user in users if user.company]
    header = ['user', 'repos_collab', 'deleted', 'site_admin', 'hireable', 'email', 'company', 'github_star']
    data = [
        (user.username, ','.join(user.repos_collab), user.deleted, user.site_admin, user.hireable, user.email, user.company, user.github_star)
        for user in site_admin_users
    ]
    write_csv(output_folder, 'users_company.csv', header, data)
    print("[+] Company users written to users_company.csv")

def main(users_file, repos_file, logs_folder, output_folder, minimum_stars, minimum_forks, minimum_watchers):
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
        write_sort_repos_by_stars(output_folder, minimum_stars, load_csv_repo_file_gen(repos_file))
        write_sort_repos_by_forks(output_folder, minimum_forks, load_csv_repo_file_gen(repos_file))
        write_sort_repos_by_watchers(output_folder, minimum_watchers, load_csv_repo_file_gen(repos_file))
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

    parser.add_argument('-s', '--minimum-stars', default=1, type=int, help="Min stars of repos.", required=True)
    parser.add_argument('-f', '--minimum-forks', default=1,type=int, help="Min forks of repos.", required=True)
    parser.add_argument('-w', '--minimum-watchers', default=1, type=int, help="Min watchers of repos.", required=True)

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

    main(args.users_file, args.repos_file, args.logs_folder, args.output_folder, int(args.minimum_stars), int(args.minimum_forks), int(args.minimum_watchers))
