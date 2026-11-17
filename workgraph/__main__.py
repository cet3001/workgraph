"""Enable python -m workgraph without running on import."""
from .cli import main

if __name__ == "__main__":
    raise SystemExit(main())
