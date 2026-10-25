#!/bin/sh
# One-time setup of the repository's git hooks (run it once per clone).
# It points git at .githooks, where the pre-commit hook scans the staged changes for secrets with gitleaks.
set -eu
cd "$(dirname "$0")/.."
git config core.hooksPath .githooks
chmod +x .githooks/pre-commit
echo "git hooks enabled: core.hooksPath = $(git config --get core.hooksPath)"
if command -v gitleaks >/dev/null 2>&1; then
    echo "gitleaks found: $(gitleaks version)"
else
    echo "gitleaks is not installed: the hook will only warn. Install it, for example: brew install gitleaks"
fi
