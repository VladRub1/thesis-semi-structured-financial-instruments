from __future__ import annotations

import re
from typing import Dict, List, Tuple

from bs4 import BeautifulSoup

from .config import (
    ATTACHMENTS_LABEL,
    GUARANTEE_DATES_SECTION,
    MAIN_INFO_SECTION,
    MISSING_PAGE_PHRASE,
    TARGET_SECTIONS,
)

_WHITESPACE_RE = re.compile(r"\s+")


def _normalize_whitespace(text: str) -> str:
    cleaned = (text or "").replace("\xa0", " ")
    return _WHITESPACE_RE.sub(" ", cleaned).strip()


def normalize_label(text: str) -> str:
    label = _normalize_whitespace(text)
    return label.rstrip(":").strip()


def normalize_value(text: str) -> str:
    return _normalize_whitespace(text)


def extract_text_value(element) -> str:
    if element is None:
        return ""
    # Preserve raw text as much as possible while trimming outer whitespace.
    return normalize_value("".join(element.strings))


def is_missing_page(html: str) -> bool:
    return MISSING_PAGE_PHRASE in html


def _parse_main_info(soup: BeautifulSoup) -> Tuple[Dict[str, str], List[str]]:
    warnings: List[str] = []
    fields: Dict[str, str] = {}

    card = soup.find("div", class_="cardMainInfo")
    if not card:
        warnings.append("Top summary block not found")
        return fields, warnings

    for status in card.find_all("div", class_="cardMainInfo__status"):
        title_el = status.find("span", class_="cardMainInfo__title")
        if title_el:
            status_text = _normalize_whitespace(title_el.get_text(" ", strip=True))
            if status_text:
                fields.setdefault("Статус", normalize_value(status_text))

    for section in card.find_all("div", class_="cardMainInfo__section"):
        title_el = section.find("span", class_="cardMainInfo__title")
        content_el = section.find("span", class_="cardMainInfo__content")
        if title_el and content_el:
            label = normalize_label(title_el.get_text(" ", strip=True))
            if label:
                fields[label] = extract_text_value(content_el)

        for value_div in section.find_all("div", class_="cardMainInfo__value"):
            label = _normalize_whitespace(" ".join(value_div.find_all(string=True, recursive=False)))
            label = normalize_label(label)
            value_el = value_div.find("div", class_="cardMainInfo__content")
            if label and value_el:
                fields[label] = extract_text_value(value_el)

    purchase_link = card.find("span", class_="cardMainInfo__purchaseLink")
    if purchase_link:
        text = _normalize_whitespace(purchase_link.get_text(" ", strip=True))
        if text:
            number = text.replace("№", "").strip()
            fields.setdefault("Номер банковской гарантии", normalize_value(number))

    price_block = card.find("div", class_="price")
    if price_block:
        title_el = price_block.find("span", class_="cardMainInfo__title")
        content_el = price_block.find("span", class_="cardMainInfo__content")
        if title_el and content_el:
            label = normalize_label(title_el.get_text(" ", strip=True))
            if label:
                fields[label] = extract_text_value(content_el)

    return fields, warnings


def _parse_guarantee_dates_table(container) -> Tuple[Dict[str, str], List[str]]:
    warnings: List[str] = []
    fields: Dict[str, str] = {}
    if container is None:
        warnings.append("Guarantee dates table container missing")
        return fields, warnings

    table = container.find("table", class_="blockInfo__table")
    if not table:
        warnings.append("Guarantee dates table not found")
        return fields, warnings

    headers = [normalize_label(th.get_text(" ", strip=True)) for th in table.find_all("th")]
    rows = table.find_all("tr", class_="tableBlock__row")
    data_rows = [row for row in rows if row.find("td")]

    for row_index, row in enumerate(data_rows, start=1):
        cells = [extract_text_value(td) for td in row.find_all("td")]
        for header, cell in zip(headers, cells):
            if not header:
                continue
            label = header if row_index == 1 else f"{header} (строка {row_index})"
            fields[label] = cell

    return fields, warnings


def parse_general_info(html: str) -> Tuple[Dict[str, Dict[str, str]], List[str]]:
    soup = BeautifulSoup(html, "html.parser")
    warnings: List[str] = []
    sections: Dict[str, Dict[str, str]] = {}

    main_fields, main_warnings = _parse_main_info(soup)
    warnings.extend(main_warnings)
    if main_fields:
        sections[MAIN_INFO_SECTION] = main_fields

    for header in soup.find_all("h2", class_="blockInfo__title"):
        section_name = _normalize_whitespace(header.get_text(" ", strip=True))
        if section_name not in TARGET_SECTIONS:
            continue

        container = header.parent
        fields: Dict[str, str] = {}
        for section in container.find_all("section", class_="blockInfo__section"):
            title_el = section.find("span", class_="section__title")
            info_el = section.find("span", class_="section__info")
            if title_el and info_el:
                label = normalize_label(title_el.get_text(" ", strip=True))
                if label:
                    fields[label] = extract_text_value(info_el)
                continue

            sub_el = section.find("span", class_="section__sub")
            if sub_el:
                for title_span in sub_el.find_all("span", class_="title"):
                    label = normalize_label(title_span.get_text(" ", strip=True))
                    if not label:
                        continue
                    info_span = title_span.find_next_sibling("span", class_="info")
                    fields[label] = extract_text_value(info_span)

        if not fields:
            warnings.append(f"Section '{section_name}' found but no fields parsed")
        sections[section_name] = fields

        if section_name == "Информация о банковской гарантии":
            table_fields, table_warnings = _parse_guarantee_dates_table(container)
            warnings.extend(table_warnings)
            if table_fields:
                sections[GUARANTEE_DATES_SECTION] = table_fields

    for required in TARGET_SECTIONS:
        if required not in sections:
            warnings.append(f"Section '{required}' not found")

    return sections, warnings


def _is_download_link(href: str) -> bool:
    href_lower = href.lower()
    return ("download" in href_lower) or ("/filestore/" in href_lower) or ("file.html?uid" in href_lower)


def _extract_tooltip_text(link) -> str:
    tooltip = link.get("data-tooltip")
    if not tooltip:
        return ""
    try:
        tooltip_soup = BeautifulSoup(tooltip, "html.parser")
        return _normalize_whitespace(tooltip_soup.get_text(" ", strip=True))
    except Exception:
        return ""


def _extract_document_number(attachment) -> str:
    number_el = attachment.find(
        "div",
        class_="attachment__value",
        string=lambda s: s and "Информация о банковской гарантии" in s,
    )
    if not number_el:
        return ""
    text = _normalize_whitespace(number_el.get_text(" ", strip=True))
    if "№" in text:
        after = text.split("№", 1)[1].strip()
        if after:
            return after.split(" ")[0]
    match = re.search(r"№\s*([\\w\\-\\/]+)", text)
    if match:
        return match.group(1)
    return ""


def _parse_document_metadata_rows(soup: BeautifulSoup) -> List[Dict[str, str]]:
    rows: List[Dict[str, str]] = []
    blocks = soup.find_all("div", class_="card-attachments__block")
    for block in blocks:
        title = block.find("div", class_="title")
        if not title:
            continue
        title_text = _normalize_whitespace(title.get_text(" ", strip=True))
        if title_text != "Информация о банковской гарантии":
            continue

        attachments = block.find_all("div", class_="attachment")
        for index, attachment in enumerate(attachments, start=1):
            document_number = _extract_document_number(attachment)
            if document_number:
                rows.append(
                    {
                        "field_name": "Номер банковской гарантии",
                        "field_value": normalize_value(document_number),
                        "document_index": index,
                        "document_number": normalize_value(document_number),
                    }
                )

            for label_div in attachment.find_all("div", class_="attachment__text"):
                label = normalize_label(label_div.get_text(" ", strip=True))
                if not label or label == ATTACHMENTS_LABEL:
                    continue
                value_div = label_div.find_next_sibling("div", class_="attachment__value")
                rows.append(
                    {
                        "field_name": label,
                        "field_value": extract_text_value(value_div),
                        "document_index": index,
                        "document_number": normalize_value(document_number) if document_number else "",
                    }
                )

    return rows


def parse_document_info(html: str) -> Tuple[List[Dict[str, str]], List[Dict[str, str]], List[str]]:
    soup = BeautifulSoup(html, "html.parser")
    warnings: List[str] = []
    metadata_rows = _parse_document_metadata_rows(soup)
    attachments: List[Dict[str, str]] = []
    seen: set[tuple[int, str]] = set()

    blocks = soup.find_all("div", class_="card-attachments__block")
    for block in blocks:
        title = block.find("div", class_="title")
        if not title:
            continue
        title_text = _normalize_whitespace(title.get_text(" ", strip=True))
        if title_text != "Информация о банковской гарантии":
            continue

        for doc_index, attachment in enumerate(block.find_all("div", class_="attachment"), start=1):
            document_number = _extract_document_number(attachment)
            for link in attachment.find_all("a", href=True):
                href = link["href"]
                if "signview" in href:
                    continue
                if not _is_download_link(href):
                    continue
                key = (doc_index, href)
                if key in seen:
                    continue
                seen.add(key)

                original_name = _extract_tooltip_text(link)
                if not original_name:
                    original_name = _normalize_whitespace(link.get_text(" ", strip=True))

                attachments.append(
                    {
                        "download_url": href,
                        "original_filename": original_name,
                        "document_index": doc_index,
                        "document_number": normalize_value(document_number) if document_number else "",
                    }
                )

    if not attachments:
        warnings.append("Attachments not found in document blocks")

    return attachments, metadata_rows, warnings
