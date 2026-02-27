from __future__ import annotations

from pathlib import Path

DATA_DIR = Path(__file__).resolve().parents[1]

RAW_HTML_DIR = DATA_DIR / "raw" / "html"
RAW_ATTACHMENTS_DIR = DATA_DIR / "raw" / "attachments"
PROCESSED_DIR = DATA_DIR / "processed"
LOGS_DIR = DATA_DIR / "logs"
STATE_DIR = DATA_DIR / "state"

SAMPLES_DIR = DATA_DIR / "samples"

CHROMEDRIVER_PATH = DATA_DIR / "chromedriver-mac-arm64" / "chromedriver"

GENERAL_INFO_URL = (
    "https://zakupki.gov.ru/epz/bankguarantee/guaranteeCard/generalInformation.html"
    "?guaranteeInfoId={id}"
)
DOCUMENTS_URL = (
    "https://zakupki.gov.ru/epz/bankguarantee/guaranteeCard/document-info.html"
    "?guaranteeInfoId={id}"
)

MISSING_PAGE_PHRASE = "Запрашиваемая страница не существует"

ATTACHMENTS_LABEL = "Прикрепленные файлы"

TARGET_SECTIONS = [
    "Информация о банке-гаранте",
    "Информация о поставщике (подрядчике, исполнителе) – принципале",
    "Информация о заказчике-бенефициаре",
    "Информация о банковской гарантии",
]

MAIN_INFO_SECTION = "Сводная информация (верхний блок)"
GUARANTEE_DATES_SECTION = "Сроки и сумма (нижний блок)"
DOCUMENT_META_SECTION = "Документы: Информация о банковской гарантии"

DEFAULT_TEST_IDS = [1962721, 11, 1, 196221]

DEFAULT_SLEEP_MIN = 5.0
DEFAULT_SLEEP_MAX = 15.0
LONG_SLEEP_EVERY = 25
LONG_SLEEP_MIN = 30.0
LONG_SLEEP_MAX = 60.0

DEFAULT_HTML_SAVE_FIRST_N = 10

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)
