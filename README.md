## Legalacts.ru Codes Parser

A Playwright-based parser that crawls `https://legalacts.ru/`, discovers all law codes from the codes index, traverses sections/chapters/articles, and saves each article with metadata into per-code files.

### Features
- Human-like browsing (mouse moves, scrolling, random delays, non-headless by default)
- Robust waits and retries
- Extracts metadata: section_number, section_name, chapter_number, chapter_name, article_number, article_name, updated_at
- Filters navigation artifacts from article text (e.g., stray "<", ">", and nav "Статья ..." lines)
- Writes UTF-8 text files under `output/` (one file per code)

### Requirements
- Python 3.10+

### Setup
```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python -m playwright install chromium
```

### Run
```bash
python codes_parser.py --output-dir output --headed --max-articles 5
```

Options:
- `--output-dir` (default: `output`) where result files are stored
- `--headed/--headless` (default: headed) run browser visibly for better anti-bot behavior
- `--codes` comma-separated code slugs to limit crawl (e.g. `APK-RF,GK-RF`)
- `--max-articles` limit number of articles per code (for testing)
- `--delay-min`/`--delay-max` seconds for random human-like delays

Example full crawl (visible browser):
```bash
python codes_parser.py --output-dir output --headed
```

### Discovery source
Codes are discovered from `https://legalacts.ru/kodeksy/` using the `main-center-block-linkslist-noleft ps-0` container.

### Output format
Each article entry is preceded by its metadata on separate lines, followed by a blank line and the article text.

Example block:
```
[section_number] I
[section_name] Общие положения
[chapter_number] 1
[chapter_name] Основные положения
[article_number] 1
[article_name] Осуществление правосудия арбитражными судами
[updated_at] 17.11.2005

<article text...>
```

### Notes
- The parser reuses the same tab while walking articles in a code.
- If blocked, reduce speed, increase delays, and run in `--headed` mode.
