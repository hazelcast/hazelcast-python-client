import json
import os
import subprocess


SERIALIZATION_VERSION = 1
CLIENT_TYPE = "PYH"
CLIENT_VERSION_INFO = (3, 12, 2)
CLIENT_VERSION = ".".join(map(str, CLIENT_VERSION_INFO))


class GitInfoProvider(object):
    def __init__(self):
        here = os.path.abspath(os.path.dirname(__file__))
        git_info = read_git_info(here)

        if git_info:
            self.commit_id = git_info.get("commit_id", "")
            self.commit_date = git_info.get("commit_date", "")
        else:
            self.commit_id = get_commit_id()
            self.commit_date = get_commit_date()
            write_git_info(here, self.commit_id, self.commit_date)


def read_git_info(here):
    try:
        file_path = os.path.abspath(os.path.join(here, "git_info.json"))
        with open(file_path, "r") as f:
            info = json.load(f)
            return info
    except:
        return None


def write_git_info(here, commit_id, commit_date):
    try:
        git_info = dict()
        git_info["commit_id"] = commit_id
        git_info["commit_date"] = commit_date

        file_path = os.path.abspath(os.path.join(here, "git_info.json"))
        with open(file_path, "w") as f:
            json.dump(git_info, f)
    except:
        pass


def get_commit_id():
    try:
        commit_id = subprocess.check_output(["git", "show", "-s", "--format=\"%h\""]).decode()
        commit_id = commit_id.strip().replace("\"", "").replace("'", "")
        return commit_id
    except:
        return ""


def get_commit_date():
    try:
        commit_date = subprocess.check_output(["git", "show", "-s", "--format=\"%cd\"", "--date=short"]).decode()
        commit_date = commit_date.strip().replace("\"", "").replace("'", "").replace("-", "")
        return commit_date
    except:
        return ""


_info_provider = GitInfoProvider()
GIT_COMMIT_ID = _info_provider.commit_id
GIT_COMMIT_DATE = _info_provider.commit_date
