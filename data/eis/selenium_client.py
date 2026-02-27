from __future__ import annotations

import random
import time
from pathlib import Path

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait

from .config import CHROMEDRIVER_PATH, USER_AGENT


def build_driver(
    download_dir: Path, headless: bool = False, block_images: bool = False
) -> webdriver.Chrome:
    options = Options()
    if headless:
        options.add_argument("--headless=new")
    options.add_argument("--start-maximized")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument(f"--user-agent={USER_AGENT}")

    prefs = {
        "download.default_directory": str(download_dir),
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True,
        "plugins.always_open_pdf_externally": True,
    }
    if block_images:
        prefs["profile.managed_default_content_settings.images"] = 2
    options.add_experimental_option("prefs", prefs)

    service = Service(str(CHROMEDRIVER_PATH))
    return webdriver.Chrome(service=service, options=options)


def human_sleep(min_seconds: float, max_seconds: float) -> None:
    time.sleep(random.uniform(min_seconds, max_seconds))


def wait_for_ready(driver: webdriver.Chrome, timeout: int = 30) -> None:
    WebDriverWait(driver, timeout).until(
        lambda d: d.execute_script("return document.readyState") == "complete"
    )


def wait_for_any_selector(
    driver: webdriver.Chrome, selectors: list[str], timeout: int = 30
) -> None:
    def _any_present(drv):
        for selector in selectors:
            if drv.find_elements("css selector", selector):
                return True
        return False

    WebDriverWait(driver, timeout).until(_any_present)
