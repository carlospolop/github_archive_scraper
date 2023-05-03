class Repository:
    def __init__(self, full_name, stars, forks, watchers, deleted, private, archived, disabled):
        self.full_name = full_name
        self.stars = stars
        self.forks = forks
        self.watchers = watchers
        self.deleted = deleted
        self.private = private
        self.archived = archived
        self.disabled = disabled

    def __eq__(self, other):
        if isinstance(other, Repository):
            return self.full_name == other.full_name
        return False

    def __hash__(self):
        return hash(self.full_name)


class User:
    def __init__(self, username, repos_collab, deleted, site_admin, hireable, email, company, github_star):
        self.username = username
        self.repos_collab = repos_collab
        self.deleted = deleted
        self.site_admin = site_admin
        self.hireable = hireable
        self.email = email
        self.company = company
        self.github_star = github_star

    def __eq__(self, other):
        if isinstance(other, User):
            return self.username == other.username
        return False

    def __hash__(self):
        return hash(self.username)
