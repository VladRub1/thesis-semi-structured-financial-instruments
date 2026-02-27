from __future__ import annotations

from pathlib import Path
import re

import pandas as pd


def resolve_processed_base() -> Path:
    cwd = Path.cwd().resolve()
    candidates = [
        cwd / "data" / "processed",
        cwd / "processed",
    ]
    for parent in [cwd, *cwd.parents]:
        candidates.append(parent / "data" / "processed")
    for candidate in candidates:
        if candidate.exists():
            return candidate
    raise FileNotFoundError("Could not locate data/processed directory from cwd")


def extract_run_id(path: Path) -> int | None:
    match = re.search(r"run_(\d+)_", path.name)
    if not match:
        return None
    return int(match.group(1))


def latest_run_id(path: Path) -> int | None:
    run_ids = [extract_run_id(p) for p in path.glob("*.parquet")]
    run_ids = [rid for rid in run_ids if rid is not None]
    return max(run_ids) if run_ids else None


def read_run_parquets(path: Path, run_id: int | None) -> pd.DataFrame:
    if run_id is None:
        files = list(path.glob("*.parquet"))
    else:
        files = list(path.glob(f"*run_{run_id}_*.parquet"))
    if not files:
        return pd.DataFrame()
    frames = [pd.read_parquet(file) for file in sorted(files)]
    return pd.concat(frames, ignore_index=True)


def read_latest_csvs(final_dir: Path) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame] | None:
    guarantees_path = final_dir / "guarantees_latest.csv"
    attributes_path = final_dir / "attributes_latest.csv"
    files_path = final_dir / "files_latest.csv"
    if not (guarantees_path.exists() and attributes_path.exists() and files_path.exists()):
        return None
    guarantees = pd.read_csv(guarantees_path)
    attributes = pd.read_csv(attributes_path)
    files = pd.read_csv(files_path)
    return guarantees, attributes, files


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [str(c).strip() for c in df.columns]
    if "id" not in df.columns:
        for col in df.columns:
            if str(col).strip().lower() == "id":
                df = df.rename(columns={col: "id"})
                break
    if "id" not in df.columns:
        unnamed_cols = [
            col
            for col in df.columns
            if str(col).strip().lower().startswith("unnamed")
            or str(col).strip().lower() == "index"
        ]
        for col in unnamed_cols:
            series = pd.to_numeric(df[col], errors="coerce")
            if series.notna().any():
                df = df.rename(columns={col: "id"})
                break
    if "run_id" not in df.columns:
        for col in df.columns:
            if str(col).strip().lower() == "run_id":
                df = df.rename(columns={col: "run_id"})
                break
    if "id" not in df.columns and df.index.name and df.index.name.lower() == "id":
        df = df.reset_index()
    return df


def main() -> None:
    base = resolve_processed_base()
    final_dir = base / "final"
    csvs = read_latest_csvs(final_dir)
    if csvs is not None:
        guarantees_latest, attributes_latest, files_latest = csvs
        run_id = None
    else:
        run_id = latest_run_id(base / "guarantees")
        if run_id is None:
            raise RuntimeError("No run_id found in processed/guarantees")
        guarantees_latest = read_run_parquets(base / "guarantees", run_id)
        attributes_latest = read_run_parquets(base / "attributes", run_id)
        files_latest = read_run_parquets(base / "files", run_id)

    guarantees_latest = normalize_columns(guarantees_latest)
    attributes_latest = normalize_columns(attributes_latest)
    files_latest = normalize_columns(files_latest)

    if "id" not in files_latest.columns:
        raise RuntimeError("files_latest missing 'id' column; check source tables.")
    if "run_id" not in files_latest.columns:
        raise RuntimeError("files_latest missing 'run_id' column; check source tables.")

    # Filter to guarantees with existing files and select one file per guarantee
    files_latest = files_latest.copy()
    if "file_exists" not in files_latest.columns:
        files_latest["file_exists"] = files_latest["stored_path"].apply(
            lambda p: isinstance(p, str) and p and Path(p).exists()
        )

    files_existing = files_latest[files_latest["file_exists"]].copy()

    def file_size(path_value: str) -> int:
        try:
            return Path(path_value).stat().st_size
        except Exception:
            return -1

    files_existing["size_bytes"] = files_existing["stored_path"].apply(file_size)

    # Document metadata for current versions
    meta = attributes_latest[
        attributes_latest["section"].eq("Документы: Информация о банковской гарантии")
    ].copy()

    meta_pivot = meta.pivot_table(
        index=["id", "run_id", "document_index"],
        columns="field_name",
        values="field_value",
        aggfunc="first",
    ).reset_index()

    current_doc_idx = (
        meta_pivot[
            meta_pivot.get("Редакция", "")
            .astype(str)
            .str.contains("Действующая", na=False)
        ]
        .groupby("id")["document_index"]
        .apply(lambda s: set(s.dropna().tolist()))
        .to_dict()
    )

    def choose_file(group: pd.DataFrame) -> pd.DataFrame:
        gid = group.name
        candidates = group.copy()
        if "id" not in candidates.columns:
            candidates = candidates.assign(id=gid)

        if gid in current_doc_idx and current_doc_idx[gid]:
            filtered = candidates[candidates["document_index"].isin(current_doc_idx[gid])]
            if not filtered.empty:
                candidates = filtered

        # If all hashes are identical, keep one.
        hashes = candidates["sha256"].fillna("")
        unique_hashes = [h for h in hashes.unique().tolist() if h]
        if len(unique_hashes) == 1:
            candidates = candidates[hashes.eq(unique_hashes[0])]

        # Otherwise keep the smallest file, tie-break by file_index.
        candidates = candidates.assign(
            size_bytes=candidates["size_bytes"].where(
                candidates["size_bytes"] >= 0, 10**18
            )
        ).sort_values(["size_bytes", "file_index"])
        return candidates.head(1)

    if files_existing.empty:
        selected_files = files_existing.copy()
    else:
        selected_files = (
            files_existing.groupby("id", group_keys=False)
            .apply(choose_file)
            .reset_index(drop=True)
        )
    selected_files = normalize_columns(selected_files)
    if "id" not in selected_files.columns:
        if selected_files.index.name and selected_files.index.name.lower() == "id":
            selected_files = selected_files.reset_index()
        else:
            raise RuntimeError(
                "selected_files missing 'id' column; columns="
                f"{list(selected_files.columns)}"
            )

    # Keep only guarantees with an existing selected file
    selected_ids = selected_files["id"].unique()
    guarantees_latest = guarantees_latest[guarantees_latest["id"].isin(selected_ids)].copy()
    attributes_latest = attributes_latest[attributes_latest["id"].isin(selected_ids)].copy()

    # Build wide analytical table
    base_df = guarantees_latest[["id", "run_id"]].drop_duplicates()
    attr = attributes_latest.copy()

    def get_attr_value(section: str, field_names, column_name: str) -> pd.DataFrame:
        if isinstance(field_names, str):
            field_names = [field_names]
        sub = attr[(attr["section"] == section) & (attr["field_name"].isin(field_names))].copy()
        if sub.empty:
            return base_df.assign(**{column_name: pd.NA})[["id", "run_id", column_name]]
        sub["field_name"] = pd.Categorical(sub["field_name"], categories=field_names, ordered=True)
        sub = sub.sort_values(["id", "run_id", "field_name"])
        sub = sub.drop_duplicates(["id", "run_id"])
        return sub[["id", "run_id", "field_value"]].rename(columns={"field_value": column_name})

    def get_attr_label(section: str, field_names, column_name: str) -> pd.DataFrame:
        if isinstance(field_names, str):
            field_names = [field_names]
        sub = attr[(attr["section"] == section) & (attr["field_name"].isin(field_names))].copy()
        if sub.empty:
            return base_df.assign(**{column_name: pd.NA})[["id", "run_id", column_name]]
        sub["field_name"] = pd.Categorical(sub["field_name"], categories=field_names, ordered=True)
        sub = sub.sort_values(["id", "run_id", "field_name"])
        sub = sub.drop_duplicates(["id", "run_id"])
        return sub[["id", "run_id", "field_name"]].rename(columns={"field_name": column_name})

    # Bank info
    bank_inn = get_attr_value("Информация о банке-гаранте", "ИНН", "bank_inn")
    bank_name = get_attr_value(
        "Информация о банке-гаранте",
        "Сокращенное наименование банка",
        "bank_name",
    )

    # Principal info
    pcpl_inn = get_attr_value(
        "Информация о поставщике (подрядчике, исполнителе) – принципале",
        "ИНН",
        "pcpl_inn",
    )
    pcpl_name = get_attr_value(
        "Информация о поставщике (подрядчике, исполнителе) – принципале",
        [
            "Сокращенное наименование поставщика (подрядчика, исполнителя)",
            "Полное наименование поставщика (подрядчика, исполнителя)",
        ],
        "pcpl_name",
    )
    pcpl_region = get_attr_value(
        "Информация о поставщике (подрядчике, исполнителе) – принципале",
        "Наименование субъекта РФ (код)",
        "pcpl_region",
    )
    pcpl_city = get_attr_value(
        "Информация о поставщике (подрядчике, исполнителе) – принципале",
        "Наименование населенного пункта местонахождения (код по ОКТМО)",
        "pcpl_city",
    )
    pcpl_type = get_attr_value(
        "Информация о поставщике (подрядчике, исполнителе) – принципале",
        "Вид",
        "pcpl_type",
    )

    # Beneficiary info
    bene_inn = get_attr_value("Информация о заказчике-бенефициаре", "ИНН", "bene_inn")
    bene_name = get_attr_value(
        "Информация о заказчике-бенефициаре",
        [
            "Сокращенное наименование заказчика",
            "Полное наименование заказчика",
            "Сокращенное наименование Заказчика",
            "Полное наименование Заказчика",
            "Сокращенное наименование заказчика-бенефициара",
            "Полное наименование заказчика-бенефициара",
        ],
        "bene_name",
    )
    bene_region = get_attr_value(
        "Информация о заказчике-бенефициаре",
        "Наименование субъекта РФ (код)",
        "bene_region",
    )
    bene_city = get_attr_value(
        "Информация о заказчике-бенефициаре",
        "Наименование населенного пункта местонахождения (код по ОКТМО)",
        "bene_city",
    )
    bene_type = get_attr_value(
        "Информация о заказчике-бенефициаре",
        "Организационно-правовая форма (код по ОКОПФ)",
        "bene_type",
    )

    # Guarantee info
    issue_date = get_attr_value(
        "Сводная информация (верхний блок)", "Выдача банковской гарантии", "issue_date"
    )
    start_date = get_attr_value(
        "Сроки и сумма (нижний блок)", "Дата вступления в силу", "start_date"
    )
    end_date = get_attr_value(
        "Сроки и сумма (нижний блок)", "Дата окончания срока действия", "end_date"
    )
    end_date_fallback = get_attr_value(
        "Сводная информация (верхний блок)", "Окончание срока действия", "end_date_fallback"
    )

    sum_summary = get_attr_value(
        "Сводная информация (верхний блок)", "Размер банковской гарантии", "sum_summary"
    )

    sum_field_names = [
        name for name in attr["field_name"].unique() if str(name).startswith("Денежная сумма,")
    ]
    sum_lower = get_attr_value("Сроки и сумма (нижний блок)", sum_field_names, "sum_lower")
    currency_label = get_attr_label(
        "Сроки и сумма (нижний блок)", sum_field_names, "currency_from_label"
    )

    ikz = get_attr_value(
        "Информация о банковской гарантии",
        ["Идентификационный код закупки (ИКЗ)", "Идентификационный код закупки"],
        "ikz",
    )

    coverage_type = get_attr_value(
        "Информация о банковской гарантии", "Вид обеспечения", "coverage_type"
    )

    guarantee_number = get_attr_value(
        "Сводная информация (верхний блок)", "Номер банковской гарантии", "guarantee_number"
    )

    # Document metadata for selected files
    selected_meta = meta_pivot.merge(
        selected_files[["id", "document_index"]], on=["id", "document_index"], how="inner"
    )

    published_time = selected_meta[["id", "run_id", "Размещено"]].rename(
        columns={"Размещено": "published_time"}
    )
    redaction_type = selected_meta[["id", "run_id", "Редакция"]].rename(
        columns={"Редакция": "redaction_type"}
    )
    guarantee_number_doc = selected_meta[["id", "run_id", "Номер банковской гарантии"]].rename(
        columns={"Номер банковской гарантии": "guarantee_number_doc"}
    )

    # Assemble table
    wide = base_df.copy()
    for part in [
        bank_inn,
        bank_name,
        pcpl_inn,
        pcpl_name,
        pcpl_region,
        pcpl_city,
        pcpl_type,
        bene_inn,
        bene_name,
        bene_region,
        bene_city,
        bene_type,
        issue_date,
        start_date,
        end_date,
        end_date_fallback,
        sum_summary,
        sum_lower,
        currency_label,
        ikz,
        coverage_type,
        guarantee_number,
        published_time,
        redaction_type,
        guarantee_number_doc,
    ]:
        wide = wide.merge(part, on=["id", "run_id"], how="left")

    wide = wide.merge(
        selected_files[["id", "run_id", "stored_filename", "stored_path", "sha256"]],
        on=["id", "run_id"],
        how="left",
    )

    # Coalesce guarantee number from summary and document metadata
    wide["guarantee_number"] = wide["guarantee_number"].fillna(wide["guarantee_number_doc"])
    wide.drop(columns=["guarantee_number_doc"], inplace=True)

    # Normalize fields: dates, timestamps, numeric values, and region/city codes
    def normalize_text(series: pd.Series) -> pd.Series:
        return (
            series.astype(str)
            .str.replace(r"\s+", " ", regex=True)
            .str.strip()
            .replace("nan", pd.NA)
        )

    def normalize_okopf(series: pd.Series) -> pd.Series:
        s = normalize_text(series)
        extracted = s.str.extract(r"\(([^()]*)\)\s*$", expand=False)
        base = s.str.replace(r"\s*\([^)]*\)\s*$", "", regex=True).str.strip()
        out = base.where(extracted.isna(), base + " (" + extracted + ")")
        return out.replace("", pd.NA)

    def strip_parens(series: pd.Series) -> pd.Series:
        return (
            normalize_text(series)
            .str.replace(r"\s*\([^)]*\)\s*$", "", regex=True)
            .replace("", pd.NA)
        )

    def parse_number(series: pd.Series) -> pd.Series:
        cleaned = (
            normalize_text(series)
            .str.replace(r"[^0-9,.-]", "", regex=True)
            .str.replace(",", ".", regex=False)
        )
        return pd.to_numeric(cleaned, errors="coerce")

    def parse_date(series: pd.Series) -> pd.Series:
        s = normalize_text(series).str.replace(r"\(.*?\)", "", regex=True).str.strip()
        return pd.to_datetime(s, dayfirst=True, errors="coerce").dt.date

    def parse_datetime_msk(series: pd.Series) -> pd.Series:
        s = normalize_text(series).str.replace(r"\(.*?\)", "", regex=True).str.strip()
        dt = pd.to_datetime(s, dayfirst=True, errors="coerce")
        if dt.dt.tz is None:
            dt = dt.dt.tz_localize("Europe/Moscow")
        return dt

    # Currency extraction
    wide["currency_symbol"] = normalize_text(wide["sum_summary"]).str.extract(
        r"([A-Za-zА-Яа-я₽$€]+)$", expand=False
    )
    wide["currency_from_label"] = normalize_text(wide["currency_from_label"])
    wide["currency_from_value"] = normalize_text(wide["sum_lower"]).str.extract(
        r"ОКВ\s*(\d+)", expand=False
    )

    # Numeric amounts
    wide["sum"] = parse_number(wide["sum_summary"]).fillna(parse_number(wide["sum_lower"]))

    # Dates
    wide["issue_date"] = parse_date(wide["issue_date"])
    wide["start_date"] = parse_date(wide["start_date"])
    wide["end_date"] = parse_date(wide["end_date"]).fillna(parse_date(wide["end_date_fallback"]))
    wide.drop(columns=["end_date_fallback"], inplace=True)

    # Published time
    wide["published_time"] = parse_datetime_msk(wide["published_time"])

    # Regions and cities
    wide["pcpl_region"] = strip_parens(wide["pcpl_region"])
    wide["pcpl_city"] = strip_parens(wide["pcpl_city"])
    wide["bene_region"] = strip_parens(wide["bene_region"])
    wide["bene_city"] = strip_parens(wide["bene_city"])

    # Names
    wide["bank_name"] = normalize_text(wide["bank_name"])
    wide["pcpl_name"] = normalize_text(wide["pcpl_name"])
    wide["bene_name"] = normalize_text(wide["bene_name"])
    wide["bene_type"] = normalize_okopf(wide["bene_type"])

    output_dir = base / "final"
    output_dir.mkdir(parents=True, exist_ok=True)
    out_csv = output_dir / "wide_analytical_latest.csv"
    out_parquet = output_dir / "wide_analytical_latest.parquet"
    wide.to_csv(out_csv, index=False)
    wide.to_parquet(out_parquet, index=False)

    print("Latest run_id:", run_id)
    print("Selected files rows", len(selected_files))
    print("Guarantees with files", wide["id"].nunique())
    print("Saved:", out_csv)
    print("Saved:", out_parquet)


if __name__ == "__main__":
    main()
