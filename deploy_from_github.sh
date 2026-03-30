
#!/bin/bash
set -e

echo "=== Deploy started: $(date) ==="

# Only stash if there are local changes
git diff --quiet || git stash

git pull

# Restore stashed changes if we stashed anything
git stash list | grep -q stash && git stash pop || true

echo ""
git log --oneline -3

echo ""
pip3 install -r requirements.txt -q

echo ""
echo "=== Dry run ==="
python3 strategy_runner.py --dry-run

echo ""
echo "=== Deploy complete: $(date) ==="

 
