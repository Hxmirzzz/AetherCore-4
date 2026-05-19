import sys
from src.presentation.console.console_app import main
from pathlib import Path

Path('logs').mkdir(parents=True, exist_ok=True)

if __name__ == "__main__":
    sys.exit(main())