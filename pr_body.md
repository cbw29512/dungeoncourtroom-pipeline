## What changed
Removed a redundant call to `save_to_history([question])` from main.py that was happening after every successful episode.

## Why it matters
The history saving is now handled correctly in `scraper_node.py` after episode completion, not before. This fixes a bug where questions were added to the history even on failed runs.

## How to verify
Run the pipeline with one of these commands:
  python main.py --test
  python main.py --question "Can a Bard inspire themselves?"

Check that `docket_history.json` is populated correctly with only successful episodes.