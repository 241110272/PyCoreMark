"""Launch the Streamlit/Plotly dashboard."""

from pathlib import Path
import sys


def main() -> None:
    from streamlit.web.cli import main as streamlit_main

    app = Path(__file__).parents[1] / "dashboard" / "web.py"
    sys.argv = ["streamlit", "run", str(app)]
    streamlit_main()
