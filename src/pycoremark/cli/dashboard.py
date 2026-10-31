"""Legacy dashboard command redirected to the interactive CLI."""

from pycoremark.cli.main import dashboard_main


def main() -> None:
    dashboard_main()


if __name__ == "__main__":
    main()

