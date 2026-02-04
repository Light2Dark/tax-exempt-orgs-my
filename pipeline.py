import marimo

__generated_with = "0.19.7"
app = marimo.App(width="columns")

with app.setup:
    import marimo as mo
    from datetime import date, datetime
    from dataclasses import dataclass
    import re
    from typing import Literal
    from bs4 import BeautifulSoup
    from loguru import logger
    from playwright.async_api import async_playwright, Page
    from pathlib import Path
    import polars as pl
    import asyncio

    URL_446 = "https://www.hasil.gov.my/en/quick-links/services/donation-approval/subsection-44-6-of-the-income-tax-act-1967/"  # 124 pages
    URL_11D = "https://www.hasil.gov.my/en/quick-links/services/donation-approval/subsection-44-11d-of-the-income-tax-act-1967/"
    URL_PUA = "https://www.hasil.gov.my/en/quick-links/services/donation-approval/pu-a-1392020/"

    GENERATED_11D_BASE_PATH = "./public/generated/subsection_11D"
    GENERATED_446_BASE_PATH = "./public/generated/subsection_44_6"
    GENERATED_PUA_BASE_PATH = "./public/generated/subsection_PUA"
    DEFAULT_TIMEOUT = 60 * 60

    Section = Literal["446", "11D", "PUA"]


@app.cell
def _():
    # hack for loguru
    import sys

    _handlers: dict = logger._core.handlers  # type: ignore

    if not _handlers.get(1):
        logger.add(
            lambda msg: sys.stderr.write(msg + "\n") and sys.stderr.flush(),
            format="{time} | {level} | {function} | {message}",
            level=10,
        )

    if _handlers.get(0):
        logger.remove(0)
    return


@app.cell
def _(process_html, save_org_csv):
    async def scrape_subsection(
        *,
        section: Section,
        page_start: int,
        page_end: int,
        save_snapshot: bool = False,
    ) -> list[str]:
        """Scrape a specific subsection page from page_start to page_end (inclusive). Returns list of save paths"""

        if section == "446":
            goto_url = URL_446
            subsection_name = "44(6)"
        elif section == "11D":
            goto_url = URL_11D
            subsection_name = "44(11D)"
        else:
            goto_url = URL_PUA
            subsection_name = "P.U.(A)"

        logger.info(f"Scraping {goto_url} from page {page_start} to page {page_end}")

        try:
            async with async_playwright() as p:
                browser = await p.chromium.launch()
                page = await browser.new_page()

                logger.info(f"Starting subsection {subsection_name} scrape from page {page_start} to page {page_end}")
                await page.goto(goto_url)

                try:
                    await authenticate_page(page, section)
                except Exception as e:
                    logger.error(f"Error authenticating page: {e}")
                    return []

                save_paths: list[str] = []
                for page_number in range(page_start, page_end + 1):
                    url = f"{goto_url}?page={page_number}"
                    logger.info(f"Scraping page {page_number}")
                    try:
                        await page.goto(url)
                        await page.wait_for_load_state("networkidle")
                        content = await page.content()
                    except Exception as e:
                        logger.error(f"Error scraping page {page_number}: {e}")
                        continue

                    if save_snapshot:
                        save_page_html(content, page_number, section)

                    try:
                        orgs = process_html(content)
                    except Exception as e:
                        logger.error(f"Error processing html: {e}. Content: {content}")
                        continue

                    path = save_org_csv(section, orgs, page_number)
                    save_paths.append(path)

                logger.info("Succesfully scraped")
                return save_paths
        finally:
            await browser.close()

    return (scrape_subsection,)


@app.function
async def authenticate_page(page: Page, section: Section):
    # PUA page does not have this selector
    if section != "PUA":
        category_input = page.locator("#DermaKategori")
        await category_input.select_option("Semua")

    state_input = page.get_by_label("State", exact=False)
    await state_input.select_option("Semua")

    search_button = page.locator("input[type='submit']")
    await search_button.click()

    await page.wait_for_load_state("networkidle")


@app.function
def save_page_html(content: str, page_num: int, section: Section):
    with open(f"./snapshots/subsection_{section}_page{page_num}.html", "w") as f:
        f.write(content)


@app.cell
def _():
    def save_org_csv(section: Section, orgs: list[Organization], thread_id: int | None) -> str:
        base_path = (
            GENERATED_446_BASE_PATH
            if section == "446"
            else GENERATED_11D_BASE_PATH
            if section == "11D"
            else GENERATED_PUA_BASE_PATH
        )
        output_file = Path(f"{base_path}/thread_{thread_id}.csv")
        if output_file.exists() and mo.app_meta().mode != "edit":
            logger.warning("File already exists, overwriting")
        pl.DataFrame(orgs).write_csv(output_file)
        return output_file

    return (save_org_csv,)


@app.function
def merge_orgs(file_paths: list[str], save_path: str):
    if len(file_paths) == 0:
        raise ValueError("No file paths provided")
    df = pl.scan_csv(file_paths).collect()
    df.write_csv(save_path)

    logger.info(f"Saved file to {save_path}")


@app.cell
def _():
    def process_html(html: str) -> list[Organization]:
        soup = BeautifulSoup(html, "html.parser")

        th = soup.find("th", string="APPROVAL REFERENCE NO.")
        table = th.find_parent("table")
        columns = [col.text for col in table.find_all("th")]
        is_pua = len(columns) == 7
        rows = table.find_all("tr")

        organizations: list[Organization] = []

        for row in rows:
            cols = row.find_all("td")
            if cols and cols[0] and cols[0].text.strip():
                reference_num = cols[0].text.strip()
                organization = cols[1].find("strong").text.strip()
                address = cols[1].text.strip().replace(organization, "").strip()
                address = re.sub(r"\s+", " ", address)

                if is_pua:
                    category = "Worship"
                    start_date = cols[2].text.strip()
                    end_date = cols[3].text.strip()
                    status = cols[4].text.strip()
                    remarks = None
                else:
                    category = cols[2].text.strip()
                    start_date = cols[3].text.strip()
                    end_date = cols[4].text.strip()
                    status = cols[5].text.strip()
                    remarks = cols[6].text.strip()

                organizations.append(
                    Organization(
                        reference_num=reference_num,
                        organization=organization,
                        address=address,
                        category=category,
                        start_date=start_date,
                        end_date=end_date,
                        status=status,
                        remarks=remarks,
                    )
                )
        return organizations

    return (process_html,)


@app.class_definition
@dataclass
class Organization:
    reference_num: int
    organization: str
    address: str
    category: str
    start_date: date
    end_date: date
    status: Literal["approved", "expired", "revoked"]
    remarks: str | None

    def __init__(
        self,
        reference_num: str,
        organization: str,
        address: str,
        category: str,
        start_date: str,
        end_date: str,
        status: str,
        remarks: str | None,
    ):
        self.reference_num = reference_num
        self.organization = organization
        self.address = address
        self.category = category
        self.start_date = (
            datetime.strptime(start_date, "%d %b %Y").date() if isinstance(start_date, str) else start_date
        )
        self.end_date = datetime.strptime(end_date, "%d %b %Y").date() if isinstance(end_date, str) else end_date
        if status.startswith("DILULUSKAN"):
            self.status = "approved"
        elif status == "KELULUSAN DITARIK BALIK":
            self.status = "revoked"
        else:
            self.status = "rejected"
        self.remarks = remarks if remarks else None


@app.cell(column=1, hide_code=True)
def _():
    mo.md(r"""
    Page by page scraping would take:

    9s on average per scrape
    124 pages at the moment

    9 * 124 = 1116s => 18.6 minutes

    For our purposes, we will scrape 10 pages per thread
    """)
    return


@app.function
def merge_csv(section: Section) -> pl.DataFrame:
    logger.info(f"Merging CSVs from {section} section")
    if section == "446":
        path = GENERATED_446_BASE_PATH
        savepath = "subsection_44_6.csv"
    elif section == "11D":
        path = GENERATED_11D_BASE_PATH
        savepath = "subsection_11D.csv"
    else:
        path = GENERATED_PUA_BASE_PATH
        savepath = "subsection_pua.csv"

    csv_paths = list(Path(path).glob("thread_*.csv"))
    final_path = f"{path}/{savepath}"
    merge_orgs(csv_paths, final_path)
    return pl.read_csv(final_path)


@app.cell
def _(scrape_subsection):
    async def submit_jobs(pages: list[tuple[int, int]], concurrent: int) -> list[str]:
        """Submit jobs to the executor

        Args:
            pages: List of tuples of page start and page end
            concurrent: Max number of concurrent requests
        """
        logger.info(f"Beginning scrape with {concurrent} max requests")
        semaphore = asyncio.Semaphore(concurrent)

        async def scrape_with_limit(page_start: int, page_end: int):
            async with semaphore:
                return await scrape_subsection(
                    page_start=page_start,
                    page_end=page_end,
                    save_snapshot=True,
                )

        tasks = [scrape_with_limit(pg_start, pg_end) for pg_start, pg_end in pages]
        results = await asyncio.gather(*tasks)
        return results

    return


@app.function
def verify_subsection_446():
    files = Path("./public/generated/subsection_44_6")

    for _i in range(1, 125):
        file = f"./public/generated/subsection_44_6/thread_{_i}.csv"
        if not Path(file).exists():
            logger.warning(f"{file} does not exist")


if __name__ == "__main__":
    app.run()
