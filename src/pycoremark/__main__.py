"""Enable ``python -m pycoremark`` as the PATH-independent CLI entry point."""

from pycoremark.cli.main import main


if __name__ == "__main__":
    raise SystemExit(main())
