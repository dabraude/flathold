"""Run the Streamlit app when using `python -m flathold` or the `flathold` console script."""

import subprocess
import sys
from pathlib import Path

APP_PATH = Path(__file__).resolve().parent / "ui" / "app.py"


def main() -> None:
    sys.exit(
        subprocess.run(
            [sys.executable, "-m", "streamlit", "run", str(APP_PATH), "--", *sys.argv[1:]],
            check=False,
        ).returncode
    )


if __name__ == "__main__":
    main()
