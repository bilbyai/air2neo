import argparse
from collections.abc import Sequence

from dotenv import load_dotenv

from .main import Air2Neo


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
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)

    if not args.skip_dotenv:
        load_dotenv(args.env_file)

    Air2Neo().run()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
