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
 - Qdrant running locally (default: `http://127.0.0.1:6333`)

### Setup
```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python -m playwright install chromium
```

### Run (Codes)
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

### Discovery source (Codes)
Codes are discovered from `https://legalacts.ru/kodeksy/` using the `main-center-block-linkslist-noleft ps-0` container.

### Output format (Codes)
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

### Federal laws parser
Parses federal laws from `https://legalacts.ru/docs/5/` across all pages and writes all laws into a single file with metadata followed by law text.

Run (Laws):
```bash
python laws_parser.py --output-file output/federal_laws.txt --headed --start-page 1 --max-pages 1 --max-laws 3
```

Options:
- `--output-file` (default: `output/federal_laws.txt`) destination file for all laws
- `--headed/--headless` (default: headed)
- `--start-page` start page number to resume from (default: 1)
- `--max-pages` limit number of index pages to scan (testing)
- `--max-laws` limit number of laws to fetch (testing)
- `--delay-min`/`--delay-max` human-like delays

Law metadata fields:
- `law_number` (e.g., 297-ФЗ)
- `law_name`
- `updated_at` (DD.MM.YYYY)

### Notes
- The parser reuses the same tab while walking items.
- If blocked, reduce speed, increase delays, and run in `--headed` mode.

## Qdrant Uploader (HYBRID)

Upload parsed legal codes or laws into Qdrant using dense + sparse (HYBRID) embeddings.

### Additional requirements

Install embeddings and Qdrant clients if not already present:

```bash
pip install langchain-qdrant langchain-huggingface qdrant-client
```

Ensure Qdrant is running locally (Docker example):

```bash
docker run -p 6333:6333 -p 6334:6334 qdrant/qdrant:latest
```

### Run uploader

```bash
python qdrant_uploader.py --file output/APK-RF.txt
```

Options:

- `--file` path to input file or name under `output/` (required)
- `--collection` overrides collection name (default: input file stem)
- `--qdrant-url` Qdrant URL (default: `http://127.0.0.1:6333`)
- `--qdrant-api-key` Qdrant API key (optional)
- `--batch-size` upload batch size (default: 256)
- `--append` append to existing collection (default: drop & recreate)
- `--limit` limit number of articles for smoke testing

Examples:

```bash
python qdrant_uploader.py --file output/SK-RF.txt --collection family_code --batch-size 128
python qdrant_uploader.py --file output/federal_laws.txt --limit 500
python qdrant_uploader.py --file output/APK-RF.txt --qdrant-url http://127.0.0.1:6333 --qdrant-api-key $QDRANT_API_KEY
python qdrant_uploader.py --file output/APK-RF.txt --collection my_collection --qdrant-url http://134.122.45.44:6333 --qdrant-api-key qwerty
```

The uploader stores one point per article with the text and metadata fields 
(`section_number`, `section_name`, `chapter_number`, `chapter_name`, `article_number`, 
`article_name`, `updated_at` for codes; `law_number`, `law_name`, `updated_at` for laws), 
and adds `source_file` and a unique `article_uid`.
