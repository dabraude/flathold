# Development Rules

1. ALWAYS return frozen dataclasses from functions
2. ALWAYS run the following in order:
    1. `uv run ruff format`
    2. `uv run check --fix` fix any remaining errors
    3. `uv run pyright` fix any errors
    4. `uv run pre-commit run --all-files`
