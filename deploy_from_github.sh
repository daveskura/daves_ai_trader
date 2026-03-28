#!/bin/bash -i
# Dave Skura, Mar 28,2026

set -e   # stop immediately if any command fails

git stash
git pull
git stash pop

echo ""
git log --oneline -3

echo ""
python strategy_runner.py --dry-run


