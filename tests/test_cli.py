from unittest.mock import Mock

from air2neo import cli


def test_cli_runs_migration_and_loads_dotenv(monkeypatch):
    air2neo = Mock()
    air2neo_cls = Mock(return_value=air2neo)
    load_dotenv = Mock()
    monkeypatch.setattr(cli, "Air2Neo", air2neo_cls)
    monkeypatch.setattr(cli, "load_dotenv", load_dotenv)

    result = cli.main(["--env-file", "custom.env"])

    assert result == 0
    load_dotenv.assert_called_once_with("custom.env")
    air2neo_cls.assert_called_once_with()
    air2neo.run.assert_called_once_with()


def test_cli_can_skip_dotenv(monkeypatch):
    air2neo = Mock()
    air2neo_cls = Mock(return_value=air2neo)
    load_dotenv = Mock()
    monkeypatch.setattr(cli, "Air2Neo", air2neo_cls)
    monkeypatch.setattr(cli, "load_dotenv", load_dotenv)

    result = cli.main(["--skip-dotenv"])

    assert result == 0
    load_dotenv.assert_not_called()
    air2neo_cls.assert_called_once_with()
    air2neo.run.assert_called_once_with()
