"""Allow `python -m scrapling_ops ...`."""

from .cli import main

if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
