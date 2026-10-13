"""Finish QuickBooks report config and pull a report once OAuth tokens exist.

Examples:
  python -m quickbooks --env clinic status
  python -m quickbooks --env clinic auth-url --entity MDC
  python -m quickbooks --env clinic exchange --entity MDC --callback-url "<url>"
  python -m quickbooks --env clinic pull --entity MDC --report profit_and_loss --start 2026-09-01 --end 2026-09-30
"""

from __future__ import annotations

import argparse
import secrets
import sys
from pathlib import Path

from .client import QuickBooksReportsClient
from .credentials import QuickBooksConfigError, QuickBooksCredentials, project_root
from .reports import load_report_catalog


def _output_dir() -> Path:
    return project_root() / "logs"


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="QuickBooks Online reports extraction")
    parser.add_argument(
        "--env",
        choices=["local", "clinic", "test"],
        default=None,
        help="Stage file quickbooks/.env_<stage>. Defaults to QUICKBOOKS_ENVIRONMENT.",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("status", help="Show which credential fields are still empty")

    auth = sub.add_parser("auth-url", help="Print the OAuth URL for the reports user")
    auth.add_argument("--entity", required=True)

    exchange = sub.add_parser("exchange", help="Store tokens from the OAuth callback URL")
    exchange.add_argument("--entity", required=True)
    exchange.add_argument("--callback-url", required=True)

    pull = sub.add_parser("pull", help="Download one report, or the default set")
    pull.add_argument("--entity", required=True)
    pull.add_argument("--report", default=None, help="Catalog key. Omit to pull the default set.")
    pull.add_argument("--start", default=None)
    pull.add_argument("--end", default=None)
    pull.add_argument("--as-of", default=None)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    try:
        credentials = QuickBooksCredentials.load(environment=args.env)
        catalog = load_report_catalog()
        if args.command == "status":
            print("\n".join(credentials.status_lines()))
            print("default reports: " + ", ".join(catalog.defaults))
            return 0
        if args.command == "auth-url":
            state = secrets.token_urlsafe(24)
            credentials.set_value("QUICKBOOKS_OAUTH_STATE", state)
            print(credentials.authorization_url(args.entity, state))
            print(
                "Sign in as integration_user, then run exchange with the callback URL.",
                file=sys.stderr,
            )
            return 0
        client = QuickBooksReportsClient(credentials, catalog)
        if args.command == "exchange":
            client.exchange_code(args.entity, args.callback_url)
            print(f"Stored realm and tokens for {args.entity}")
            return 0
        keys = [args.report] if args.report else list(catalog.defaults)
        for key in keys:
            spec = catalog.get(key)
            as_of = args.as_of or (args.end if spec.date_param == "as_of" else None)
            path = client.pull_report(
                args.entity,
                spec,
                start_date=args.start,
                end_date=args.end,
                as_of=as_of,
                output_dir=_output_dir(),
            )
            print(path)
        return 0
    except (QuickBooksConfigError, KeyError, ValueError) as exc:
        print(str(exc), file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
