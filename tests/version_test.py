import os

from hazelcast.version import get_commit_id, get_commit_date, read_git_info, write_git_info, GitInfoProvider
from unittest import TestCase


class VersionTest(TestCase):
    def test_git_utilities(self):
        commit_id = get_commit_id()
        commit_date = get_commit_date()

        here = os.path.abspath(os.path.dirname(__file__))

        write_git_info(here, commit_id, commit_date)

        info = read_git_info(here)

        self.assertEqual(commit_id, info.get("commit_id"))
        self.assertEqual(commit_date, info.get("commit_date"))

        try:
            os.remove(os.path.join(here, "git_info.json"))
        except:
            pass

    def test_git_info_provider(self):
        info_provider = GitInfoProvider()

        commit_id = get_commit_id()
        commit_date = get_commit_date()

        self.assertEqual(commit_id, info_provider.commit_id)
        self.assertEqual(commit_date, info_provider.commit_date)
