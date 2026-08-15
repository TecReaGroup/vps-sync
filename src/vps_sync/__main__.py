"""Allow the package to run with ``python -m vps_sync``."""

from vps_sync.cli import main

if __name__ == "__main__":
    raise SystemExit(main())
