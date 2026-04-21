"""Initialize local guru tracking database."""

from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from tracker.db import DB_ENGINE, DB_PATH, init_db


def main() -> None:
    init_db()
    print(f'Initialized database ({DB_ENGINE}) at {DB_PATH}')


if __name__ == '__main__':
    main()
