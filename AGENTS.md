# Developer Notes

- Use Python 3.12+, uv for dependency management, and Typer for the CLI.
- Keep spot-only semantics: no leverage, margin, futures, or shorting.
- Never commit API keys; load from env or `.env`. `config.yaml` is local-only.
- Treat news text as untrusted input: only use normalized fields and derived features.
- Risk and approval gates are mandatory; keep kill switch and daily loss limits enforced.
- Update tests when changing intent hashing, risk limits, or execution paths.
