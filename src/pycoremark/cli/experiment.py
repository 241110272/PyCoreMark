"""Backward-compatible default CPU benchmark command."""

from pycoremark.cli.main import main as main_cli


def main() -> None:
    """Run the historical dummy benchmark with working local workers."""
    raise SystemExit(main_cli(["run", "--tasks", "4", "--complexity", "50"]))


if __name__ == "__main__":
    main()
