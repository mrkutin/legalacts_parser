import asyncio
import os
import re
import random
from dataclasses import dataclass
from typing import List, Optional, Tuple, Dict

import click
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from playwright.async_api import async_playwright, Browser, BrowserContext, Page, Error as PlaywrightError


BASE_URL = "https://legalacts.ru"
LEFT_BLOCK_SELECTOR = "div.p-2.main-left-block.col-12.col-lg-2"
CENTER_BLOCK_SELECTOR = "div.main-center-block.col-12.col-lg-8"
CODES_LIST_SELECTOR = "div.main-center-block-linkslist-noleft.ps-0"
ARTICLE_TEXT_SELECTOR = "div.main-center-block-article-text"


@dataclass
class ArticleMeta:
    section_number: str
    section_name: str
    chapter_number: str
    chapter_name: str
    article_number: str
    article_name: str
    updated_at: str


def random_user_agent() -> str:
    user_agents = [
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    ]
    return random.choice(user_agents)


async def human_delay(min_seconds: float, max_seconds: float) -> None:
    await asyncio.sleep(random.uniform(min_seconds, max_seconds))


async def humanize_page(page: Page, min_delay: float, max_delay: float) -> None:
    width = random.randint(1200, 1600)
    height = random.randint(800, 1000)
    try:
        await page.set_viewport_size({"width": width, "height": height})
    except Exception:
        pass

    try:
        for _ in range(random.randint(2, 4)):
            x = random.randint(50, width - 50)
            y = random.randint(100, height - 100)
            await page.mouse.move(x, y, steps=random.randint(10, 30))
            await human_delay(min_delay, max_delay)
        for _ in range(random.randint(2, 4)):
            delta = random.randint(200, 600)
            await page.mouse.wheel(0, delta)
            await human_delay(min_delay, max_delay)
    except Exception:
        pass


def ensure_output_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def slug_from_href(href: str) -> str:
    parts = href.strip("/").split("/")
    return parts[1] if len(parts) > 1 else parts[-1]


async def get_text_or_empty(page: Page, selector: str) -> str:
    try:
        locator = page.locator(selector)
        if await locator.count() == 0:
            return ""
        return (await locator.first.inner_text()).strip()
    except Exception:
        return ""


def find_date_in_text(text: str) -> str:
    dates = re.findall(r"\b(\d{2}\.\d{2}\.\d{4})\b", text)
    return dates[-1] if dates else ""


def parse_title_number_and_name(title: str, keyword: str) -> Tuple[str, str]:
    t = title.strip()
    number = ""
    name = t
    try:
        if t.lower().startswith(keyword.lower()):
            rest = t[len(keyword):].strip()
            # Support Roman numerals or decimal numbers with optional dots/hyphens (e.g., 241.2, 12.1-1)
            m = re.match(r"^([IVXLCDM]+|\d+(?:[.\-]\d+)*)[\.:\)]\s*(.*)$", rest, re.IGNORECASE)
            if m:
                number, name = m.group(1).strip(), m.group(2).strip()
            else:
                parts = rest.split(maxsplit=1)
                if parts:
                    number = parts[0]
                    name = parts[1] if len(parts) > 1 else ""
    except Exception:
        pass
    return number, name


@retry(
    reraise=True,
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=1, max=6),
    retry=retry_if_exception_type(PlaywrightError),
)
async def goto_with_retry(page: Page, url: str) -> None:
    await page.goto(url, timeout=45000, wait_until="domcontentloaded")


async def extract_codes_from_home(page: Page, min_delay: float, max_delay: float) -> List[Tuple[str, str]]:
    await goto_with_retry(page, BASE_URL + "/kodeksy/")
    await page.wait_for_selector(CODES_LIST_SELECTOR, timeout=30000)
    await humanize_page(page, min_delay, max_delay)

    links = await page.eval_on_selector_all(
        f"{CODES_LIST_SELECTOR} a",
        "els => els.map(a => ({href: a.getAttribute('href'), text: a.textContent?.trim() || ''}))",
    )
    code_links: List[Tuple[str, str]] = []
    for item in links:
        href = item.get("href") or ""
        text = item.get("text") or ""
        if href.startswith("/kodeks/") and text:
            code_links.append((text, href))
    return code_links


async def extract_toc_items(page: Page, min_delay: float, max_delay: float) -> List[Dict[str, str]]:
    await page.wait_for_selector(CENTER_BLOCK_SELECTOR, timeout=30000)
    await humanize_page(page, min_delay, max_delay)

    items = await page.eval_on_selector_all(
        f"{CENTER_BLOCK_SELECTOR} p.text-start",
        "els => els.map(p => ({cls: p.className, href: p.querySelector('a')?.getAttribute('href') || '', text: p.querySelector('a')?.textContent?.trim() || ''}))",
    )
    return items


async def extract_article_text_and_date(page: Page, min_delay: float, max_delay: float) -> Tuple[str, str]:
    await page.wait_for_selector(ARTICLE_TEXT_SELECTOR, timeout=30000)
    await humanize_page(page, min_delay, max_delay)

    article_text = await get_text_or_empty(page, ARTICLE_TEXT_SELECTOR)
    article_text = clean_article_text(article_text)
    center_text = await get_text_or_empty(page, "div.main-center-block")
    updated_at = find_date_in_text(center_text)

    return article_text.strip(), updated_at


async def write_article(output_path: str, meta: ArticleMeta, article_text: str) -> None:
    with open(output_path, "a", encoding="utf-8") as f:
        lines = []
        if meta.section_number:
            lines.append(f"[section_number] {meta.section_number}\n")
        if meta.section_name:
            lines.append(f"[section_name] {meta.section_name}\n")
        if meta.chapter_number:
            lines.append(f"[chapter_number] {meta.chapter_number}\n")
        if meta.chapter_name:
            lines.append(f"[chapter_name] {meta.chapter_name}\n")
        if meta.article_number:
            lines.append(f"[article_number] {meta.article_number}\n")
        if meta.article_name:
            lines.append(f"[article_name] {meta.article_name}\n")
        if meta.updated_at:
            lines.append(f"[updated_at] {meta.updated_at}\n")

        for ln in lines:
            f.write(ln)
        if lines:
            f.write("\n")

        f.write(article_text)
        f.write("\n\n")


async def process_code(
    browser_context: BrowserContext,
    code_text: str,
    code_href: str,
    output_dir: str,
    min_delay: float,
    max_delay: float,
    max_articles: Optional[int] = None,
) -> None:
    page = await browser_context.new_page()
    await human_delay(min_delay, max_delay)

    code_url = BASE_URL + code_href
    await goto_with_retry(page, code_url)

    items = await extract_toc_items(page, min_delay, max_delay)

    current_section_number = ""
    current_section_name = ""
    current_chapter_number = ""
    current_chapter_name = ""

    code_slug = slug_from_href(code_href)
    ensure_output_dir(output_dir)
    output_path = os.path.join(output_dir, f"{code_slug}.txt")

    written = 0

    for item in items:
        href: str = item.get("href", "")
        text: str = (item.get("text", "") or "").strip()
        if not text and not href:
            await human_delay(min_delay, max_delay)
            continue

        lower = text.lower()
        if lower.startswith("раздел"):
            sn, snm = parse_title_number_and_name(text, "Раздел")
            current_section_number, current_section_name = sn, snm
        elif lower.startswith("глава"):
            cn, cnm = parse_title_number_and_name(text, "Глава")
            current_chapter_number, current_chapter_name = cn, cnm
        elif lower.startswith("статья") and href:
            an, anm = parse_title_number_and_name(text or "", "Статья")
            article_url = BASE_URL + href

            try:
                await goto_with_retry(page, article_url)
                article_text, updated_at = await extract_article_text_and_date(page, min_delay, max_delay)
                meta = ArticleMeta(
                    section_number=current_section_number or "",
                    section_name=current_section_name or "",
                    chapter_number=current_chapter_number or "",
                    chapter_name=current_chapter_name or "",
                    article_number=an or "",
                    article_name=anm or (text or ""),
                    updated_at=updated_at or "",
                )
                await write_article(output_path, meta, article_text)
                written += 1
                await goto_with_retry(page, code_url)
            except PlaywrightError:
                pass

            if max_articles is not None and written >= max_articles:
                break

        await human_delay(min_delay, max_delay)

    await page.close()


async def launch_context(headless: bool, min_delay: float, max_delay: float) -> BrowserContext:
    playwright = await async_playwright().start()
    browser: Browser = await playwright.chromium.launch(
        headless=headless,
        args=[
            "--disable-blink-features=AutomationControlled",
            "--no-default-browser-check",
            "--disable-dev-shm-usage",
        ],
        slow_mo=random.randint(int(min_delay * 300), int(max_delay * 500)),
    )

    context = await browser.new_context(
        locale="ru-RU",
        user_agent=random_user_agent(),
        viewport={"width": random.randint(1280, 1600), "height": random.randint(800, 1000)},
    )
    return context


async def run_async(output_dir: str, codes: str, headed: bool, max_articles: Optional[int], delay_min: float, delay_max: float) -> None:
    ensure_output_dir(output_dir)

    context = await launch_context(headless=not headed, min_delay=delay_min, max_delay=delay_max)

    try:
        home_page = await context.new_page()
        await human_delay(delay_min, delay_max)

        all_codes = await extract_codes_from_home(home_page, delay_min, delay_max)
        await home_page.close()

        code_slug_allowlist: Optional[set] = None
        if codes.strip():
            code_slug_allowlist = {c.strip() for c in codes.split(",") if c.strip()}

        selected = []
        for code_text, href in all_codes:
            slug = slug_from_href(href)
            if code_slug_allowlist is None or slug in code_slug_allowlist:
                selected.append((code_text, href))

        for code_text, href in selected:
            await process_code(
                browser_context=context,
                code_text=code_text,
                code_href=href,
                output_dir=output_dir,
                min_delay=delay_min,
                max_delay=delay_max,
                max_articles=max_articles,
            )
            await human_delay(max(0.2, delay_min * 0.5), max(0.8, delay_max * 1.2))

    finally:
        try:
            await context.close()
        except Exception:
            pass


def clean_article_text(raw: str) -> str:
    lines = [ln.strip() for ln in raw.splitlines()]
    cleaned: List[str] = []
    nav_re = re.compile(r"^(<|>|Статья\s+\d+[\.\:\s]|Статья\s+[IVXLCDM]+[\.\:\s])")
    for ln in lines:
        if not ln:
            cleaned.append(ln)
            continue
        if nav_re.match(ln):
            continue
        cleaned.append(ln)
    # Collapse excessive blank lines
    out: List[str] = []
    blank = False
    for ln in cleaned:
        if ln == "":
            if not blank:
                out.append("")
            blank = True
        else:
            out.append(ln)
            blank = False
    return "\n".join(out).strip()


@click.command()
@click.option("--output-dir", default="output", show_default=True, help="Directory to store result files")
@click.option("--codes", default="", help="Comma-separated code slugs to limit crawl (e.g. APK-RF,GK-RF)")
@click.option("--headed/--headless", default=True, show_default=True, help="Run with a visible browser window")
@click.option("--max-articles", default=None, type=int, help="Limit number of articles per code (testing)")
@click.option("--delay-min", default=0.3, type=float, show_default=True, help="Minimum human delay in seconds")
@click.option("--delay-max", default=1.0, type=float, show_default=True, help="Maximum human delay in seconds")
def main(output_dir: str, codes: str, headed: bool, max_articles: Optional[int], delay_min: float, delay_max: float) -> None:
    asyncio.run(run_async(output_dir, codes, headed, max_articles, delay_min, delay_max))


if __name__ == "__main__":
    main()
