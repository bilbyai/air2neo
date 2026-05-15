import argparse
from collections.abc import Sequence
from os import environ

from dotenv import load_dotenv

from .main import Air2Neo

_CONFIG_ENV_VARS = {
    "airtable_api_key": "AIRTABLE_API_KEY",
    "airtable_base_id": "AIRTABLE_BASE_ID",
    "metatable_name": "AIRTABLE_METATABLE_NAME",
    "neo4j_uri": "NEO4J_URI",
    "neo4j_username": "NEO4J_USERNAME",
    "neo4j_password": "NEO4J_PASSWORD",
}

_REQUIRED_CONFIG = (
    "airtable_api_key",
    "airtable_base_id",
    "neo4j_uri",
    "neo4j_username",
    "neo4j_password",
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="air2neo",
        description="Run the Airtable to Neo4j migration.",
    )
    parser.add_argument(
        "--env-file",
        default=".env",
        help="Path to a dotenv file to load before running the migration.",
    )
    parser.add_argument(
        "--skip-dotenv",
        action="store_true",
        help="Do not load a dotenv file before running the migration.",
    )
    parser.add_argument(
        "--airtable-api-key",
        help="Airtable API key. Overrides AIRTABLE_API_KEY.",
    )
    parser.add_argument(
        "--airtable-base-id",
        help="Airtable base ID. Overrides AIRTABLE_BASE_ID.",
    )
    parser.add_argument(
        "--metatable-name",
        help="Airtable metatable name. Overrides AIRTABLE_METATABLE_NAME.",
    )
    parser.add_argument(
        "--neo4j-uri",
        help="Neo4j URI. Overrides NEO4J_URI.",
    )
    parser.add_argument(
        "--neo4j-username",
        help="Neo4j username. Overrides NEO4J_USERNAME.",
    )
    parser.add_argument(
        "--neo4j-password",
        help="Neo4j password. Overrides NEO4J_PASSWORD.",
    )
    return parser


def _config_from_args(args: argparse.Namespace) -> dict[str, str]:
    config = {
        name: getattr(args, name) or environ.get(env_var)
        for name, env_var in _CONFIG_ENV_VARS.items()
    }
    config["metatable_name"] = config["metatable_name"] or "Metatable"
    return config


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if not args.skip_dotenv:
        load_dotenv(args.env_file)

    config = _config_from_args(args)
    missing_config = [name for name in _REQUIRED_CONFIG if not config[name]]
    if missing_config:
        missing_env_vars = [_CONFIG_ENV_VARS[name] for name in missing_config]
        parser.error(
            "missing required config: "
            + ", ".join(missing_env_vars)
            + ". Provide them as CLI flags, environment variables, or a dotenv file."
        )

    Air2Neo(**config).run()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
