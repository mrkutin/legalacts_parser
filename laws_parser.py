import asyncio
import os
import re
import random
from dataclasses import dataclass
from typing import List, Optional, Tuple, Dict, Set

import click
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from playwright.async_api import async_playwright, Browser, BrowserContext, Page, Error as PlaywrightError, Response


BASE_URL = "https://legalacts.ru"
LAWS_INDEX_PATH = "/docs/5/"
CENTER_BLOCK_SELECTOR = "div.main-center-block.col-12.col-lg-8"
LAW_ITEM_SELECTOR = f"{CENTER_BLOCK_SELECTOR} div.pb-4 a[href^='/doc/']"
PAGINATION_LINKS_SELECTOR = "li.page-item a.page-link"
LAW_HEADER_SELECTOR = "h1.main-center-block-title.pb-4"
LAW_TEXT_SELECTORS = "p.pCenter, p.pRight, p.pBoth"


@dataclass
class LawMeta:
    law_number: str
    law_name: str
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
        for _ in range(random.randint(1, 3)):
            x = random.randint(50, width - 50)
            y = random.randint(100, height - 100)
            await page.mouse.move(x, y, steps=random.randint(8, 20))
            await human_delay(min_delay, max_delay)
        for _ in range(random.randint(1, 3)):
            delta = random.randint(150, 500)
            await page.mouse.wheel(0, delta)
            await human_delay(min_delay, max_delay)
    except Exception:
        pass


@retry(
    reraise=True,
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=1, max=6),
    retry=retry_if_exception_type(PlaywrightError),
)
async def goto_with_retry(page: Page, url: str) -> Optional[Response]:
    resp: Optional[Response] = await page.goto(url, timeout=45000, wait_until="domcontentloaded")
    try:
        status = resp.status if resp is not None else 0
        if status != 200:
            await page.reload(wait_until="domcontentloaded")
    except Exception:
        pass
    return resp


def ensure_output_dir(path: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)


async def get_text_or_empty(page: Page, selector: str) -> str:
    try:
        locator = page.locator(selector)
        if await locator.count() == 0:
            return ""
        return (await locator.first.inner_text()).strip()
    except Exception:
        return ""


def parse_law_header(header_text: str) -> LawMeta:
    text = "\n".join([ln.strip() for ln in header_text.splitlines() if ln.strip()])
    # updated_at (date)
    m_date = re.search(r"\b(\d{2}\.\d{2}\.\d{4})\b", text)
    updated_at = m_date.group(1) if m_date else ""

    # law_number (e.g., 297-ФЗ), allow dotted/hyphenated before -ФЗ
    m_num = re.search(r"(?:N|№)\s*([0-9]+(?:[.\-][0-9]+)*-ФЗ)", text)
    if not m_num:
        m_num = re.search(r"([0-9]+(?:[.\-][0-9]+)*-ФЗ)", text)
    law_number = m_num.group(1) if m_num else ""

    # law_name: try quoted part first, else take last non-empty line without quotes
    m_name = re.search(r"“([^”]+)”", text)
    if not m_name:
        m_name = re.search(r'"([^\"]+)"', text)
    if m_name:
        law_name = m_name.group(1).strip()
    else:
        lines = [ln for ln in text.splitlines() if ln]
        law_name = lines[-1] if lines else ""
        law_name = law_name.strip('"“”')

    return LawMeta(law_number=law_number, law_name=law_name, updated_at=updated_at)


async def extract_law_text(page: Page, min_delay: float, max_delay: float) -> str:
    await page.wait_for_selector(CENTER_BLOCK_SELECTOR, timeout=30000)
    await humanize_page(page, min_delay, max_delay)

    paragraphs = await page.eval_on_selector_all(
        f"{CENTER_BLOCK_SELECTOR} {LAW_TEXT_SELECTORS}",
        "els => els.map(p => p.textContent?.trim() || '').filter(Boolean)",
    )
    return "\n".join(paragraphs).strip()


async def write_law(output_file: str, meta: LawMeta, law_text: str) -> None:
    ensure_output_dir(output_file)
    with open(output_file, "a", encoding="utf-8") as f:
        if meta.law_number:
            f.write(f"[law_number] {meta.law_number}\n")
        if meta.law_name:
            f.write(f"[law_name] {meta.law_name}\n")
        if meta.updated_at:
            f.write(f"[updated_at] {meta.updated_at}\n")
        if meta.law_number or meta.law_name or meta.updated_at:
            f.write("\n")
        f.write(law_text)
        f.write("\n\n")


async def get_max_pages(page: Page, min_delay: float, max_delay: float) -> int:
    await page.wait_for_selector(CENTER_BLOCK_SELECTOR, timeout=30000)
    await humanize_page(page, min_delay, max_delay)

    hrefs = await page.eval_on_selector_all(
        PAGINATION_LINKS_SELECTOR,
        "els => els.map(a => a.getAttribute('href') || '')",
    )
    max_page = 1
    for href in hrefs:
        m = re.search(r"[?&]page=(\d+)", href)
        if m:
            max_page = max(max_page, int(m.group(1)))
    return max_page


async def collect_law_links_on_page(page: Page, min_delay: float, max_delay: float) -> List[Tuple[str, str]]:
    await page.wait_for_selector(CENTER_BLOCK_SELECTOR, timeout=30000)
    await humanize_page(page, min_delay, max_delay)

    links = await page.eval_on_selector_all(
        LAW_ITEM_SELECTOR,
        "els => els.map(a => ({href: a.getAttribute('href') || '', text: a.textContent?.trim() || ''}))",
    )
    result: List[Tuple[str, str]] = []
    for item in links:
        href = item.get("href") or ""
        text = item.get("text") or ""
        if href.startswith("/doc/") and text:
            result.append((href, text))
    return result


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


@click.command()
@click.option("--output-file", default="output/federal_laws.txt", show_default=True, help="Single output file for all laws")
@click.option("--headed/--headless", default=True, show_default=True, help="Run with a visible browser window")
@click.option("--max-pages", default=None, type=int, help="Limit number of pages to scan (testing)")
@click.option("--max-laws", default=None, type=int, help="Limit number of laws to fetch (testing)")
@click.option("--start-page", default=1, type=int, show_default=True, help="Start page number to resume from")
@click.option("--delay-min", default=0.3, type=float, show_default=True, help="Minimum human delay in seconds")
@click.option("--delay-max", default=1.0, type=float, show_default=True, help="Maximum human delay in seconds")
def main(output_file: str, headed: bool, max_pages: Optional[int], max_laws: Optional[int], start_page: int, delay_min: float, delay_max: float) -> None:
    asyncio.run(run_async(output_file, headed, max_pages, max_laws, start_page, delay_min, delay_max))


async def run_async(output_file: str, headed: bool, max_pages: Optional[int], max_laws: Optional[int], start_page: int, delay_min: float, delay_max: float) -> None:
    context = await launch_context(headless=not headed, min_delay=delay_min, max_delay=delay_max)
    page = await context.new_page()
    try:
        index_url = BASE_URL + LAWS_INDEX_PATH

        fetched = 0
        pnum = start_page if start_page and start_page > 0 else 1
        while True:
            list_url = index_url if pnum == 1 else f"{index_url}?page={pnum}"
            print(f"[laws] processing page {pnum}: {list_url}")
            try:
                await goto_with_retry(page, list_url)
            except PlaywrightError:
                break

            links = await collect_law_links_on_page(page, delay_min, delay_max)
            if not links:
                break

            for href, _ in links:
                law_url = BASE_URL + href
                try:
                    await goto_with_retry(page, law_url)
                    header_text = await get_text_or_empty(page, LAW_HEADER_SELECTOR)
                    meta = parse_law_header(header_text)
                    law_text = await extract_law_text(page, delay_min, delay_max)
                    await write_law(output_file, meta, law_text)
                    fetched += 1
                except PlaywrightError:
                    pass
                finally:
                    # Return to list page after each law
                    try:
                        await goto_with_retry(page, list_url)
                    except PlaywrightError:
                        pass

                if max_laws is not None and fetched >= max_laws:
                    break

            if max_laws is not None and fetched >= max_laws:
                break

            pnum += 1
            if max_pages is not None and pnum > max_pages:
                break

            await human_delay(delay_min, delay_max)

    finally:
        try:
            await page.context.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()
