from unittest.mock import Mock

from air2neo import cli


def test_cli_runs_migration_and_loads_dotenv(monkeypatch):
    air2neo = Mock()
    air2neo_cls = Mock(return_value=air2neo)
    load_dotenv = Mock()
    monkeypatch.setattr(cli, "Air2Neo", air2neo_cls)
    monkeypatch.setattr(cli, "load_dotenv", load_dotenv)

    result = cli.main(
        [
            "--env-file",
            "custom.env",
            "--airtable-api-key",
            "cli-airtable-key",
            "--airtable-base-id",
            "cli-base-id",
            "--neo4j-uri",
            "neo4j://cli",
            "--neo4j-username",
            "cli-user",
            "--neo4j-password",
            "cli-password",
        ]
    )

    assert result == 0
    load_dotenv.assert_called_once_with("custom.env")
    air2neo_cls.assert_called_once_with(
        airtable_api_key="cli-airtable-key",
        airtable_base_id="cli-base-id",
        metatable_name="Metatable",
        neo4j_uri="neo4j://cli",
        neo4j_username="cli-user",
        neo4j_password="cli-password",
    )
    air2neo.run.assert_called_once_with()


def test_cli_can_skip_dotenv_and_read_environment(monkeypatch):
    air2neo = Mock()
    air2neo_cls = Mock(return_value=air2neo)
    load_dotenv = Mock()
    monkeypatch.setattr(cli, "Air2Neo", air2neo_cls)
    monkeypatch.setattr(cli, "load_dotenv", load_dotenv)
    monkeypatch.setenv("AIRTABLE_API_KEY", "env-airtable-key")
    monkeypatch.setenv("AIRTABLE_BASE_ID", "env-base-id")
    monkeypatch.setenv("AIRTABLE_METATABLE_NAME", "EnvMetatable")
    monkeypatch.setenv("NEO4J_URI", "neo4j://env")
    monkeypatch.setenv("NEO4J_USERNAME", "env-user")
    monkeypatch.setenv("NEO4J_PASSWORD", "env-password")

    result = cli.main(["--skip-dotenv"])

    assert result == 0
    load_dotenv.assert_not_called()
    air2neo_cls.assert_called_once_with(
        airtable_api_key="env-airtable-key",
        airtable_base_id="env-base-id",
        metatable_name="EnvMetatable",
        neo4j_uri="neo4j://env",
        neo4j_username="env-user",
        neo4j_password="env-password",
    )
    air2neo.run.assert_called_once_with()


def test_cli_arguments_override_environment(monkeypatch):
    air2neo = Mock()
    air2neo_cls = Mock(return_value=air2neo)
    monkeypatch.setattr(cli, "Air2Neo", air2neo_cls)
    monkeypatch.setattr(cli, "load_dotenv", Mock())
    monkeypatch.setenv("AIRTABLE_API_KEY", "env-airtable-key")
    monkeypatch.setenv("AIRTABLE_BASE_ID", "env-base-id")
    monkeypatch.setenv("AIRTABLE_METATABLE_NAME", "EnvMetatable")
    monkeypatch.setenv("NEO4J_URI", "neo4j://env")
    monkeypatch.setenv("NEO4J_USERNAME", "env-user")
    monkeypatch.setenv("NEO4J_PASSWORD", "env-password")

    result = cli.main(
        [
            "--airtable-api-key",
            "cli-airtable-key",
            "--airtable-base-id",
            "cli-base-id",
            "--metatable-name",
            "CliMetatable",
            "--neo4j-uri",
            "neo4j://cli",
            "--neo4j-username",
            "cli-user",
            "--neo4j-password",
            "cli-password",
        ]
    )

    assert result == 0
    air2neo_cls.assert_called_once_with(
        airtable_api_key="cli-airtable-key",
        airtable_base_id="cli-base-id",
        metatable_name="CliMetatable",
        neo4j_uri="neo4j://cli",
        neo4j_username="cli-user",
        neo4j_password="cli-password",
    )
