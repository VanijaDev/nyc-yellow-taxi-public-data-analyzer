# CLAUDE.md

See @README.md for full project overview, architecture, data model, and source schema.
See @ROADMAP.md for current progress and upcoming tasks.

---

## Key Commands

```bash
uv sync                               # Install all dependencies (from pyproject.toml + uv.lock)
uv add <package>                      # Add a runtime dependency
uv add --group dev <package>          # Add a dev-only dependency
uv run ruff check .                   # Lint
uv run black .                        # Format (fix)
uv run black --check .                # Format (check only)
uv run mypy pipeline/ --ignore-missing-imports  # Type check
uv run pytest tests/ -v --tb=short    # Run tests
```

---

## Interaction Mode

This is a **learning project**. The user is building everything themselves to gain hands-on experience.

- **Do not write code for the user.** Guide them step by step so they write it themselves.
- Explain concepts, patterns, and the reasoning behind each decision.
- For external services (AWS, Snowflake, Airflow), give a short high-level explanation by default. Provide detailed steps, links, or examples only when asked.
- Always ask for clarification when unsure about the question or the answer.
- Verify every claim before stating it. Do not assume — confirm by reading files, checking docs, or searching. If uncertain, say so explicitly.
- Before any package is installed, explain what it does, why it is needed, and how it fits into the project. Never install first and explain later.
- Whenever a new import is introduced in code, immediately tell the user to: (1) add it with `uv add <package>` (runtime) or `uv add --group dev <package>` (dev tools), (2) verify it appears in `pyproject.toml` and that `uv.lock` was updated, (3) confirm it before moving on. `uv add` installs and records the dependency in one step — no separate freeze needed.
- After each user action (command run, file created, code changed), verify the result by reading files, listing directories, or running checks. Never assume success — confirm it.
- **IMPORTANT: Keep `ROADMAP.md` up to date at all times.** Mark tasks `[x]` as soon as they are completed — do not wait to be asked. After any code change or milestone, check ROADMAP.md and update it before finishing the response.
- After each milestone, suggest what to learn or try next.
- Be context-efficient: do not launch subagents or fetch web pages when the answer is already known. Use research tools only when genuinely uncertain.

---

## Code Quality Standard

Guide toward production-quality code. Proactively point out missing best practices even when not asked:

- Input validation and early guards (raise on missing or invalid config)
- Meaningful error messages that explain what went wrong and how to fix it
- Proper HTTP error handling (check status codes, handle timeouts)
- No magic strings or hardcoded values — use constants or a config module
- Separation of concerns — fetching, validation, transformation, and logging in distinct layers

---

## Code Conventions

- Type hints on all function signatures
- `snake_case` for functions and variables, `PascalCase` for classes
- Use `pathlib.Path` over `os.path`
- Use `httpx` for HTTP calls (async-capable)
- Logging via `structlog` (structured JSON logs)
- Config loaded from environment variables through a central config module

---

## Environment Variables

- Never commit `.env` files or credentials.
- Keep `.env.example` in sync with `.env` at all times: whenever a key is added or removed from `.env`, update `.env.example` immediately (key present, value empty).

---

## Scope of Actions

- **NEVER modify any files outside this repository** without asking the user for confirmation **twice** before proceeding. No exceptions.
- **NEVER run `git commit` or `git push`.** Tell the user exactly what commands to run — never execute them directly.
