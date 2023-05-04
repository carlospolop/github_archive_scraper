import argparse
import csv
import os

from lib.functions import load_csv_repo_file_gen, load_csv_user_file_gen

def write_csv(output_folder, file_name, header, data):
    file_path = os.path.join(output_folder, file_name)

    with open(file_path, 'w', newline='', encoding='utf-8') as csv_file:
        csv_writer = csv.writer(csv_file)
        csv_writer.writerow(header)

        for row in data:
            csv_writer.writerow(row)

def write_sort_repos_by_stars(output_folder, repos):
    repos_with_stars = [repo for repo in repos if repo.stars > 0]
    sorted_repos = sorted(repos_with_stars, key=lambda repo: int(repo.stars), reverse=True)
    header = ['owner', 'repo', 'stars', 'forks', 'watchers', 'deleted', 'private', 'archived', 'disabled']
    data = [
        (owner, repo_name, repo.stars, repo.forks, repo.watchers, repo.deleted, repo.private, repo.archived, repo.disabled)
        for repo in sorted_repos
        for owner, repo_name in [repo.full_name.split('/')]
    ]
    write_csv(output_folder, 'repos_sorted_stars.csv', header, data)


def write_sort_repos_by_forks(output_folder, repos):
    repos_with_forks = [repo for repo in repos if repo.stars > 0]
    sorted_repos = sorted(repos_with_forks, key=lambda repo: int(repo.forks), reverse=True)
    header = ['owner', 'repo', 'stars', 'forks', 'watchers', 'deleted', 'private', 'archived', 'disabled']
    data = [
        (owner, repo_name, repo.stars, repo.forks, repo.watchers, repo.deleted, repo.private, repo.archived, repo.disabled)
        for repo in sorted_repos
        for owner, repo_name in [repo.full_name.split('/')]
    ]
    write_csv(output_folder, 'repos_sorted_forks.csv', header, data)

def write_sort_repos_by_watchers(output_folder, repos):
    repos_with_watchers = [repo for repo in repos if repo.stars > 0]
    sorted_repos = sorted(repos_with_watchers, key=lambda repo: int(repo.forks), reverse=True)
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

def main(output_folder):
    write_sort_repos_by_stars(output_folder, load_csv_repo_file_gen(output_folder))
    write_sort_repos_by_forks(output_folder, load_csv_repo_file_gen(output_folder))
    write_sort_repos_by_watchers(output_folder, load_csv_repo_file_gen(output_folder))
    write_private_repos(output_folder, load_csv_repo_file_gen(output_folder))
    write_deleted_repos(output_folder, load_csv_repo_file_gen(output_folder))
    write_archived_repos(output_folder, load_csv_repo_file_gen(output_folder))
    write_disabled_repos(output_folder, load_csv_repo_file_gen(output_folder))

    write_site_admin_users(output_folder, load_csv_user_file_gen(output_folder))
    write_deleted_users(output_folder, load_csv_user_file_gen(output_folder))
    write_hireable_users(output_folder, load_csv_user_file_gen(output_folder))
    write_github_star_users(output_folder, load_csv_user_file_gen(output_folder))
    write_email_users(output_folder, load_csv_user_file_gen(output_folder))
    write_company_users(output_folder, load_csv_user_file_gen(output_folder))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Use the generated CSV files to get interesting information.")
    parser.add_argument('output_folder', type=str, help="The path of the folder where the CSV files are generated.")

    args = parser.parse_args()
    main(args.output_folder)
