def test_revisions(repository):
    revisions = repository.revisions
    assert len(revisions) == 2

    assert revisions[0].commit_msg == "My second commit"
    assert revisions[0].revision_nr == 1
    assert revisions[1].commit_msg == "My first commit"
    assert revisions[1].revision_nr == 0


def test_checkout_newest(repository):
    repository.checkout(repository.revisions[0].revision_hash)
    with open(repository.path / "file.txt", "r") as f:
        assert f.read() == "bar"


def test_checkout_oldest(repository):
    repository.checkout(repository.revisions[1].revision_hash)
    with open(repository.path / "file.txt", "r") as f:
        assert f.read() == "foo"
