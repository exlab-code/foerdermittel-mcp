#!/usr/bin/env python3
"""Pipeline: download CorrelAid parquet → enrich via LLM → build SQLite.

Usage:
    python enrich.py                    # incremental run
    python enrich.py --force            # re-enrich everything
    python enrich.py --limit 10         # enrich only 10 (for testing)
    python enrich.py --dry-run          # download + filter only, no API calls
    python enrich.py --stats            # print DB statistics
    python enrich.py --dsee              # include DSEE programs (from GitHub Pages)
    python enrich.py --dsee data.json   # include DSEE programs (from local file)
"""

import argparse
import hashlib
import html
import io
import json
import logging
import os
import re
import sqlite3
import sys
import tempfile
import time
import zipfile
from dataclasses import dataclass

import numpy as np
import pandas as pd
import requests
import yaml
from anthropic import Anthropic
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

PARQUET_URL = (
    "https://foerderdatenbankdump.fra1.cdn.digitaloceanspaces.com"
    "/data/parquet_data.zip"
)
TAXONOMY_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "taxonomy.yaml")
DB_PATH = os.environ.get("DB_PATH", os.path.join(os.path.dirname(os.path.abspath(__file__)), "foerdermittel.db"))

# Mapping: parquet column name → our DB column name
# Array-typed parquet columns are joined with ", " before storage.
PARQUET_TO_DB = {
    "id_hash": "id",
    "title": "title",
    "description": "description",
    "more_info": "more_info",
    "legal_basis": "legal_basis",
    "eligible_applicants": "eligible_applicants",  # array → str
    "funding_type": "funding_type_raw",             # array → str
    "funding_area": "funding_area",                 # array → str
    "funding_location": "funding_location",         # array → str
    "contact_info_institution": "institution_name",
    "funding_body": "funding_body",
    "url": "source_url",
    "contact_info_email": "contact_email",
    "contact_info_website": "contact_website",
    "further_links": "further_links",               # array → JSON
    "checksum": "checksum",
    "last_updated": "last_updated",
}

# Enrichment columns produced by Claude
ENRICHMENT_COLUMNS = [
    "clean_title",
    "super_kategorien",
    "themen",
    "zielgruppen",
    "foerderart",
    "short_summary",
    "eligibility_summary",
    "funding_summary",
    "bundesland",
    "funding_type_normalized",
    "application_deadline",
    "deadline_type",
]

# Fields indexed in FTS5 -- only LLM-generated clean text, not raw HTML
FTS_FIELDS = [
    "clean_title",
    "title",
    "short_summary",
    "eligibility_summary",
    "funding_summary",
    "institution_name",
    "eligible_applicants",
]


# ---------------------------------------------------------------------------
# TaxonomyLoader
# ---------------------------------------------------------------------------


class TaxonomyLoader:
    """Reads taxonomy.yaml and generates tool schemas with enum constraints."""

    def __init__(self, path: str = TAXONOMY_PATH):
        with open(path, "r", encoding="utf-8") as f:
            self.data = yaml.safe_load(f)

    @property
    def super_kategorien(self) -> list[str]:
        return [e["label"] for e in self.data["super_kategorien"]]

    @property
    def themen(self) -> list[str]:
        return [e["label"] for e in self.data["themen"]]

    @property
    def zielgruppen(self) -> list[str]:
        return [e["label"] for e in self.data["zielgruppen"]]

    @property
    def foerderart(self) -> list[str]:
        return [e["label"] for e in self.data["foerderart"]]

    @property
    def bundeslaender(self) -> list[str]:
        return self.data["bundeslaender"]

    @property
    def funding_types(self) -> list[str]:
        return self.data["funding_types"]

    @property
    def deadline_types(self) -> list[str]:
        return self.data["deadline_types"]

    def _base_properties(self) -> dict:
        """Shared property definitions used by both providers."""
        return {
            "clean_title": {
                "type": "string",
                "description": (
                    "Keep the original program name but add context if the title alone "
                    "is unclear. Drop legal references (RL, VV-, §). For short/cryptic "
                    "titles use format 'OriginalName – kurze Erklärung'. "
                    "For already clear titles, keep as-is or shorten slightly."
                ),
            },
            "super_kategorien": {
                "type": "array",
                "items": {"type": "string", "enum": self.super_kategorien},
                "description": "1-3 super-categories for this program.",
            },
            "themen": {
                "type": "array",
                "items": {"type": "string", "enum": self.themen},
                "description": "1-5 specific topics/themes.",
            },
            "zielgruppen": {
                "type": "array",
                "items": {"type": "string", "enum": self.zielgruppen},
                "description": "1-5 target groups who can apply.",
            },
            "foerderart": {
                "type": "array",
                "items": {"type": "string", "enum": self.foerderart},
                "description": "1-3 types of funding mechanism.",
            },
            "short_summary": {
                "type": "string",
                "description": (
                    "2-3 sentence plain German summary. "
                    "Describe WHAT is funded, for WHOM, and WHY/purpose. "
                    "Be specific and informative."
                ),
            },
            "eligibility_summary": {
                "type": "string",
                "description": (
                    "1-2 sentences: Who can apply? List all eligible groups "
                    "and important prerequisites."
                ),
            },
            "funding_summary": {
                "type": "string",
                "description": (
                    "1-2 sentences: Funding amount/type with concrete numbers "
                    "if available (amounts, percentages, duration, co-funding). "
                    "E.g. 'Zuschuss bis 50.000 EUR, max. 80% der förderfähigen Kosten, Laufzeit bis 3 Jahre.'"
                ),
            },
            "bundesland": {
                "type": "string",
                "enum": self.bundeslaender,
                "description": "Which Bundesland, or 'bundesweit' if federal.",
            },
            "funding_type_normalized": {
                "type": "string",
                "enum": self.funding_types,
                "description": "Primary funding type.",
            },
            "application_deadline": {
                "type": ["string", "null"],
                "description": (
                    "Application deadline as ISO date (YYYY-MM-DD) or null "
                    "if ongoing/unknown."
                ),
            },
            "deadline_type": {
                "type": "string",
                "enum": self.deadline_types,
                "description": (
                    "einmalig = one-time deadline, laufend = ongoing, "
                    "jährlich = annual/recurring, geschlossen = closed."
                ),
            },
        }

    _REQUIRED_FIELDS = [
        "clean_title",
        "super_kategorien", "themen", "zielgruppen", "foerderart",
        "short_summary",
        "eligibility_summary", "funding_summary", "bundesland",
        "funding_type_normalized", "application_deadline", "deadline_type",
    ]

    def anthropic_tool(self) -> dict:
        """Tool schema for Anthropic API."""
        props = self._base_properties()
        # Anthropic supports minItems/maxItems
        props["super_kategorien"]["minItems"] = 1
        props["super_kategorien"]["maxItems"] = 3
        props["themen"]["minItems"] = 1
        props["themen"]["maxItems"] = 5
        props["zielgruppen"]["minItems"] = 1
        props["zielgruppen"]["maxItems"] = 5
        props["foerderart"]["minItems"] = 1
        props["foerderart"]["maxItems"] = 3
        return {
            "name": "classify_foerderprogramm",
            "description": (
                "Classify and summarize a German funding program (Förderprogramm). "
                "All tag values MUST come from the provided enums."
            ),
            "input_schema": {
                "type": "object",
                "required": self._REQUIRED_FIELDS,
                "properties": props,
            },
        }

    def openai_tool(self) -> dict:
        """Tool schema for OpenAI API with strict mode."""
        props = self._base_properties()
        # OpenAI strict mode requires additionalProperties: false
        # and does NOT support minItems/maxItems, but guarantees enum compliance
        return {
            "type": "function",
            "function": {
                "name": "classify_foerderprogramm",
                "description": (
                    "Classify and summarize a German funding program (Förderprogramm). "
                    "All tag values MUST come from the provided enums."
                ),
                "strict": True,
                "parameters": {
                    "type": "object",
                    "required": self._REQUIRED_FIELDS,
                    "additionalProperties": False,
                    "properties": props,
                },
            },
        }


# ---------------------------------------------------------------------------
# ParquetDownloader
# ---------------------------------------------------------------------------


def _array_to_str(val) -> str | None:
    """Convert a numpy array or list to a comma-separated string."""
    if val is None:
        return None
    if isinstance(val, np.ndarray):
        items = [str(v) for v in val if v is not None]
        return ", ".join(items) if items else None
    if isinstance(val, list):
        items = [str(v) for v in val if v is not None]
        return ", ".join(items) if items else None
    if isinstance(val, float) and pd.isna(val):
        return None
    return str(val)


class ParquetDownloader:
    """Downloads the CorrelAid parquet dump."""

    def __init__(self, url: str = PARQUET_URL):
        self.url = url

    def download(self) -> pd.DataFrame:
        """Download, extract, and transform the parquet file."""
        logger.info("Downloading parquet from %s ...", self.url)
        resp = requests.get(self.url, timeout=120)
        resp.raise_for_status()
        logger.info("Downloaded %.1f MB", len(resp.content) / 1024 / 1024)

        with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
            parquet_files = [n for n in zf.namelist() if n.endswith(".parquet")]
            if not parquet_files:
                raise RuntimeError("No .parquet file found in zip")
            logger.info("Extracting %s", parquet_files[0])
            with zf.open(parquet_files[0]) as pf:
                df = pd.read_parquet(io.BytesIO(pf.read()))

        logger.info("Loaded %d programs from parquet", len(df))

        # Filter out deleted programs
        if "deleted" in df.columns:
            before = len(df)
            df = df[df["deleted"] == False].copy()  # noqa: E712
            logger.info("Removed %d deleted programs (%d remaining)", before - len(df), len(df))

        return df

    @staticmethod
    def transform_row(row: pd.Series) -> dict:
        """Transform a parquet row into our DB schema, handling arrays and HTML."""
        d = {}
        for parquet_col, db_col in PARQUET_TO_DB.items():
            val = row.get(parquet_col)

            # Handle numpy arrays → comma-separated string
            if isinstance(val, np.ndarray):
                if db_col == "further_links":
                    d[db_col] = json.dumps(list(val), ensure_ascii=False)
                else:
                    d[db_col] = _array_to_str(val)
            # Handle NaN
            elif isinstance(val, float) and pd.isna(val):
                d[db_col] = None
            # Handle timestamps
            elif hasattr(val, "isoformat"):
                d[db_col] = val.isoformat()
            else:
                d[db_col] = val

        return d

    @staticmethod
    def compute_checksum(row: pd.Series) -> str:
        """Compute a checksum for change detection. Uses the parquet's own checksum if available."""
        parquet_checksum = row.get("checksum")
        if parquet_checksum and isinstance(parquet_checksum, str):
            return parquet_checksum
        text = "|".join(str(row.get(c, "")) for c in [
            "title", "description", "eligible_applicants",
            "funding_type", "funding_location", "contact_info_institution",
        ])
        return hashlib.md5(text.encode("utf-8")).hexdigest()


# ---------------------------------------------------------------------------
# DSEELoader
# ---------------------------------------------------------------------------


DSEE_DEFAULT_URL = (
    "https://exlab-code.github.io/digikal/dsee_foerderprogramme.json"
)


class DSEELoader:
    """Loads DSEE JSON (from file path or URL) and transforms entries to the common DB schema."""

    def __init__(self, source: str):
        self.source = source

    def load(self) -> list[dict]:
        """Read DSEE JSON and return list of transformed program dicts."""
        if self.source.startswith(("http://", "https://")):
            logger.info("Downloading DSEE JSON from %s ...", self.source)
            resp = requests.get(self.source, timeout=60)
            resp.raise_for_status()
            raw = resp.json()
            logger.info("Downloaded %.1f KB", len(resp.content) / 1024)
        else:
            with open(self.source, "r", encoding="utf-8") as f:
                raw = json.load(f)

        programs = [self._transform(entry) for entry in raw]
        logger.info("Loaded %d programs from DSEE JSON", len(programs))
        return programs

    @staticmethod
    def _transform(entry: dict) -> dict:
        """Transform a DSEE JSON entry to the common DB schema."""
        source_url = entry.get("source_url", "")
        prog_id = hashlib.md5(source_url.encode("utf-8")).hexdigest()

        # Build more_info from subtitle + application_hints + deadline
        more_info_parts = []
        if entry.get("subtitle"):
            more_info_parts.append(entry["subtitle"])
        if entry.get("application_hints"):
            more_info_parts.append(entry["application_hints"])
        if entry.get("application_deadline_text"):
            more_info_parts.append(f"Antragsfrist: {entry['application_deadline_text']}")
        more_info = "\n\n".join(more_info_parts) if more_info_parts else None

        # Join array fields
        regions = entry.get("regions") or []
        engagement_areas = entry.get("engagement_areas") or []
        additional_links = entry.get("additional_links") or []

        # Checksum for change detection
        checksum_text = "|".join(str(entry.get(c, "")) for c in [
            "title", "description", "target_group",
        ])
        checksum = hashlib.md5(checksum_text.encode("utf-8")).hexdigest()

        return {
            "id": prog_id,
            "title": entry.get("title"),
            "description": entry.get("description"),
            "more_info": more_info,
            "legal_basis": None,
            "eligible_applicants": entry.get("target_group"),
            "funding_type_raw": None,
            "funding_area": ", ".join(engagement_areas) if engagement_areas else None,
            "funding_location": ", ".join(regions) if regions else None,
            "institution_name": entry.get("funding_organization"),
            "funding_body": entry.get("funding_amount_text"),
            "source_url": source_url,
            "contact_email": entry.get("contact_email"),
            "contact_website": entry.get("website"),
            "further_links": json.dumps(additional_links, ensure_ascii=False) if additional_links else None,
            "checksum": checksum,
            "last_updated": None,
        }


# ---------------------------------------------------------------------------
# Enricher
# ---------------------------------------------------------------------------


class Enricher:
    """Classifies and summarizes funding programs via LLM tool calling."""

    PROVIDERS = ("anthropic", "openai")

    def __init__(
        self,
        taxonomy: TaxonomyLoader,
        provider: str = "openai",
        model: str | None = None,
    ):
        self.provider = provider
        if provider == "anthropic":
            self.client = Anthropic()
            self.model = model or "claude-sonnet-4-20250514"
            self.tool = taxonomy.anthropic_tool()
        elif provider == "openai":
            from openai import OpenAI
            self.client = OpenAI()
            self.model = model or "gpt-4o-mini"
            self.tool = taxonomy.openai_tool()
        else:
            raise ValueError(f"Unknown provider: {provider}. Use: {self.PROVIDERS}")

    SYSTEM_PROMPT = """\
Du klassifizierst deutsche Förderprogramme.

Klassifikation:
- Sei PRÄZISE. Wähle nur Kategorien, die wirklich passen. Weniger ist besser als zu viel.
- super_kategorien: Meist 1, maximal 2. Nur 3 wenn das Programm wirklich mehrere Bereiche abdeckt.
- themen: Nur Themen wählen, die im Programm explizit genannt werden. Nicht interpretieren oder ableiten.
- zielgruppen: Nur Gruppen, die laut Programm tatsächlich antragsberechtigt sind.
- foerderart: Nur die tatsächlich angebotene Förderart.

Titel:
- clean_title: Behalte den Originalnamen, aber ergänze Kontext wenn der Titel allein unverständlich ist.
  Kurze/kryptische Titel erweitern, lange Titel kürzen. Aktenzeichen (VV-, RL, §) weglassen.
  Beispiel: 'KLIMOPASS' → 'KLIMOPASS – Klimawandel-Anpassung für Kommunen in Baden-Württemberg'
  Beispiel: 'Bürgschaft Express' → 'Bürgschaft Express – Kreditsicherung für KMU in Rheinland-Pfalz'
  Beispiel: 'NBeteiligung' → 'NBeteiligung – Stille Beteiligungen für Unternehmen in Niedersachsen'
  Beispiel: Langer Titel mit (RL-XYZ) → Titel ohne Aktenzeichen, gekürzt auf das Wesentliche

Zusammenfassungen:
- Auf Deutsch, sachlich, keine Floskeln oder Marketingsprache.
- short_summary: 2-3 Sätze. Beschreibe WAS gefördert wird, FÜR WEN, und WARUM/WOZU. Sei konkret und informativ.
- eligibility_summary: 1-2 Sätze. Nenne alle antragsberechtigten Gruppen und wichtige Voraussetzungen.
- funding_summary: 1-2 Sätze. Konkrete Zahlen nennen wenn vorhanden (Beträge, Prozentsätze, Laufzeiten, Eigenanteil)."""

    def enrich(self, row: dict) -> dict | None:
        """Enrich a single program. Returns the structured dict or None on failure."""
        prompt = self._build_prompt(row)

        for attempt in range(5):
            try:
                if self.provider == "anthropic":
                    return self._call_anthropic(prompt, row)
                else:
                    return self._call_openai(prompt, row)
            except Exception as e:
                err_str = str(e)
                if "429" in err_str or "rate_limit" in err_str:
                    wait = 2 ** attempt
                    logger.warning("Rate limited for %s, retrying in %ds...", row.get("id", "?"), wait)
                    time.sleep(wait)
                    continue
                logger.error("Enrichment failed for %s: %s", row.get("id", "?"), e)
                return None

        logger.error("Enrichment failed for %s after 5 retries (rate limited)", row.get("id", "?"))
        return None

    def _call_anthropic(self, prompt: str, row: dict) -> dict | None:
        response = self.client.messages.create(
            model=self.model,
            max_tokens=1024,
            system=self.SYSTEM_PROMPT,
            tools=[self.tool],
            tool_choice={"type": "tool", "name": "classify_foerderprogramm"},
            messages=[{"role": "user", "content": prompt}],
        )
        for block in response.content:
            if block.type == "tool_use" and block.name == "classify_foerderprogramm":
                return block.input
        logger.warning("No tool_use block in response for %s", row.get("id", "?"))
        return None

    def _call_openai(self, prompt: str, row: dict) -> dict | None:
        response = self.client.chat.completions.create(
            model=self.model,
            max_tokens=1024,
            tools=[self.tool],
            tool_choice={"type": "function", "function": {"name": "classify_foerderprogramm"}},
            messages=[
                {"role": "system", "content": self.SYSTEM_PROMPT},
                {"role": "user", "content": prompt},
            ],
        )
        msg = response.choices[0].message
        if msg.tool_calls:
            return json.loads(msg.tool_calls[0].function.arguments)
        logger.warning("No tool call in response for %s", row.get("id", "?"))
        return None

    @staticmethod
    def _strip_html(text: str) -> str:
        """Strip HTML tags and decode entities for prompt building."""
        text = re.sub(r"<br\s*/?>", "\n", text)
        text = re.sub(r"</(p|div|li|h[1-6])>", "\n", text)
        text = re.sub(r"<[^>]+>", "", text)
        text = html.unescape(text)
        text = re.sub(r"\n{3,}", "\n\n", text)
        return text.strip()

    @staticmethod
    def _safe_str(val, max_len: int = 0) -> str | None:
        """Return a string or None if the value is NaN/None/empty."""
        if val is None:
            return None
        if isinstance(val, float) and pd.isna(val):
            return None
        s = str(val).strip()
        if not s:
            return None
        if max_len > 0:
            s = s[:max_len]
        return s

    @staticmethod
    def _build_prompt(row: dict) -> str:
        _s = Enricher._safe_str
        _h = Enricher._strip_html

        def _clean(val, max_len: int = 0) -> str | None:
            """Get string value, strip HTML, then truncate."""
            s = _s(val)
            if s is None:
                return None
            s = _h(s)
            if not s:
                return None
            if max_len > 0:
                s = s[:max_len]
            return s

        parts = [
            f"**Titel:** {_clean(row.get('title'), 500) or 'N/A'}",
        ]
        if _clean(row.get("description")):
            parts.append(f"**Beschreibung:** {_clean(row['description'], 3000)}")
        if _clean(row.get("more_info")):
            parts.append(f"**Weitere Infos:** {_clean(row['more_info'], 2000)}")
        if _clean(row.get("legal_basis")):
            parts.append(f"**Rechtsgrundlage:** {_clean(row['legal_basis'], 500)}")
        if _s(row.get("eligible_applicants")):
            parts.append(f"**Antragsberechtigte:** {_s(row['eligible_applicants'], 1000)}")
        if _s(row.get("funding_type_raw")):
            parts.append(f"**Förderart:** {_s(row['funding_type_raw'], 500)}")
        if _s(row.get("funding_area")):
            parts.append(f"**Förderbereich:** {_s(row['funding_area'], 500)}")
        if _s(row.get("funding_location")):
            parts.append(f"**Fördergebiet:** {_s(row['funding_location'], 500)}")
        if _s(row.get("institution_name")):
            parts.append(f"**Kontakt-Institution:** {_s(row['institution_name'])}")
        if _s(row.get("funding_body")):
            parts.append(f"**Fördergeber:** {_s(row['funding_body'], 500)}")

        return "\n".join(parts)


# ---------------------------------------------------------------------------
# DatabaseBuilder
# ---------------------------------------------------------------------------


class DatabaseBuilder:
    """Creates the SQLite database with FTS5."""

    def __init__(self, db_path: str = DB_PATH):
        self.db_path = db_path

    def build(self, programs: list[dict]) -> str:
        """Build the database atomically. Returns the final path."""
        tmp_path = self.db_path + ".tmp"
        if os.path.exists(tmp_path):
            os.remove(tmp_path)

        conn = sqlite3.connect(tmp_path)
        try:
            conn.execute("PRAGMA journal_mode=WAL")
            self._create_schema(conn)
            inserted = self._insert_programs(conn, programs)
            self._verify(conn, inserted)
            conn.commit()
        finally:
            conn.close()

        # Atomic swap
        os.replace(tmp_path, self.db_path)
        logger.info("Database written to %s (%d programs)", self.db_path, inserted)
        return self.db_path

    @staticmethod
    def _create_schema(conn: sqlite3.Connection):
        conn.execute("""
            CREATE TABLE foerdermittel (
                rowid INTEGER PRIMARY KEY AUTOINCREMENT,
                id TEXT NOT NULL UNIQUE,
                title TEXT NOT NULL,
                description TEXT,
                more_info TEXT,
                legal_basis TEXT,
                eligible_applicants TEXT,
                funding_type_raw TEXT,
                funding_area TEXT,
                funding_location TEXT,
                institution_name TEXT,
                funding_body TEXT,
                source_url TEXT,
                contact_email TEXT,
                contact_website TEXT,
                further_links TEXT,
                checksum TEXT,
                last_updated TEXT,

                -- Enrichment fields
                clean_title TEXT,
                super_kategorien TEXT,
                themen TEXT,
                zielgruppen TEXT,
                foerderart TEXT,
                short_summary TEXT,
                eligibility_summary TEXT,
                funding_summary TEXT,
                bundesland TEXT,
                funding_type_normalized TEXT,
                application_deadline TEXT,
                deadline_type TEXT,

                -- Metadata
                enrichment_json TEXT,
                enriched_at TEXT
            )
        """)

        fts_cols = ", ".join(FTS_FIELDS)
        conn.execute(f"""
            CREATE VIRTUAL TABLE foerdermittel_fts USING fts5(
                {fts_cols},
                content=foerdermittel,
                content_rowid=rowid,
                tokenize='unicode61 remove_diacritics 2'
            )
        """)

        # Triggers for FTS sync
        new_vals = ", ".join(f"new.{f}" for f in FTS_FIELDS)
        old_vals = ", ".join(f"old.{f}" for f in FTS_FIELDS)

        conn.execute(f"""
            CREATE TRIGGER foerdermittel_ai AFTER INSERT ON foerdermittel BEGIN
                INSERT INTO foerdermittel_fts(rowid, {fts_cols})
                VALUES (new.rowid, {new_vals});
            END
        """)
        conn.execute(f"""
            CREATE TRIGGER foerdermittel_ad AFTER DELETE ON foerdermittel BEGIN
                INSERT INTO foerdermittel_fts(foerdermittel_fts, rowid, {fts_cols})
                VALUES ('delete', old.rowid, {old_vals});
            END
        """)
        conn.execute(f"""
            CREATE TRIGGER foerdermittel_au AFTER UPDATE ON foerdermittel BEGIN
                INSERT INTO foerdermittel_fts(foerdermittel_fts, rowid, {fts_cols})
                VALUES ('delete', old.rowid, {old_vals});
                INSERT INTO foerdermittel_fts(rowid, {fts_cols})
                VALUES (new.rowid, {new_vals});
            END
        """)

        # Indexes
        conn.execute("CREATE INDEX idx_bundesland ON foerdermittel(bundesland)")
        conn.execute("CREATE INDEX idx_funding_type ON foerdermittel(funding_type_normalized)")
        conn.execute("CREATE INDEX idx_deadline ON foerdermittel(application_deadline)")
        conn.execute("CREATE INDEX idx_deadline_type ON foerdermittel(deadline_type)")


        logger.info("Created schema with FTS5, triggers, and indexes")

    @staticmethod
    def _insert_programs(conn: sqlite3.Connection, programs: list[dict]) -> int:
        inserted = 0
        for prog in programs:
            # Serialize list/dict fields to JSON
            row = {}
            for key, val in prog.items():
                if isinstance(val, (list, dict)):
                    row[key] = json.dumps(val, ensure_ascii=False)
                elif isinstance(val, bool):
                    row[key] = int(val)
                elif pd.isna(val) if isinstance(val, float) else False:
                    row[key] = None
                else:
                    row[key] = val

            cols = list(row.keys())
            placeholders = ", ".join(["?"] * len(cols))
            col_names = ", ".join(cols)
            values = [row[c] for c in cols]

            try:
                conn.execute(
                    f"INSERT INTO foerdermittel ({col_names}) VALUES ({placeholders})",
                    values,
                )
                inserted += 1
            except sqlite3.IntegrityError as e:
                logger.warning("Skipping duplicate %s: %s", prog.get("id"), e)

        return inserted

    @staticmethod
    def _verify(conn: sqlite3.Connection, expected: int):
        actual = conn.execute("SELECT COUNT(*) FROM foerdermittel").fetchone()[0]
        fts_count = conn.execute("SELECT COUNT(*) FROM foerdermittel_fts").fetchone()[0]
        logger.info("Verification: %d rows in main table, %d in FTS (expected %d)", actual, fts_count, expected)

        if actual != expected:
            logger.warning("Row count mismatch: %d vs %d", actual, expected)


# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------


class Pipeline:
    """Orchestrates the full enrichment pipeline with incremental support."""

    def __init__(
        self,
        taxonomy: TaxonomyLoader,
        downloader: ParquetDownloader,
        enricher: Enricher,
        builder: DatabaseBuilder,
    ):
        self.taxonomy = taxonomy
        self.downloader = downloader
        self.enricher = enricher
        self.builder = builder

    def run(
        self,
        force: bool = False,
        limit: int | None = None,
        dry_run: bool = False,
        workers: int = 5,
        extra_programs: list[dict] | None = None,
    ):
        """Run the pipeline."""
        # 1. Download all programs from all sources
        df = self.downloader.download()

        # 2. Load existing enrichments (for incremental mode)
        existing_enrichments = {}
        if not force and os.path.exists(self.builder.db_path):
            existing_enrichments = self._load_existing_enrichments()
            logger.info("Loaded %d existing enrichments", len(existing_enrichments))

        # 3. Transform and split into cached vs. needs-enrichment
        programs = []
        to_enrich = []  # (index, transformed_dict)
        enriched_count = 0
        cached_count = 0
        failed_count = 0

        all_transformed = [self.downloader.transform_row(row) for _, row in df.iterrows()]
        if extra_programs:
            all_transformed.extend(extra_programs)
            logger.info("Added %d extra programs (e.g. DSEE)", len(extra_programs))

        for transformed in all_transformed:
            prog_id = transformed.get("id", "")
            checksum = transformed.get("checksum", "")

            cached = existing_enrichments.get(prog_id)
            if cached and cached.get("checksum") == checksum and not force:
                programs.append(cached["program"])
                cached_count += 1
            elif dry_run:
                programs.append(transformed)
            else:
                idx = len(programs)
                programs.append(transformed)  # placeholder
                to_enrich.append((idx, transformed))

        # --limit caps the number of new API calls, not the total programs
        if limit and len(to_enrich) > limit:
            logger.info("Limiting enrichment to %d of %d programs", limit, len(to_enrich))
            to_enrich = to_enrich[:limit]

        logger.info(
            "%d cached, %d to enrich, %d dry-run, %d total",
            cached_count, len(to_enrich), len(programs) - cached_count - len(to_enrich),
            len(programs),
        )

        # 4. Enrich in parallel
        if to_enrich:
            from concurrent.futures import ThreadPoolExecutor, as_completed

            def _enrich_one(item):
                idx, transformed = item
                enrichment = self.enricher.enrich(transformed)
                return idx, transformed, enrichment

            with ThreadPoolExecutor(max_workers=workers) as pool:
                futures = [pool.submit(_enrich_one, item) for item in to_enrich]
                for i, future in enumerate(as_completed(futures), 1):
                    idx, transformed, enrichment = future.result()
                    if enrichment:
                        programs[idx] = self._merge_program(transformed, enrichment)
                        enriched_count += 1
                    else:
                        failed_count += 1

                    if i % 10 == 0 or i == len(futures):
                        logger.info(
                            "Progress: %d/%d enriched, %d failed",
                            enriched_count, i, failed_count,
                        )

        logger.info(
            "Done: %d enriched, %d cached, %d failed, %d total",
            enriched_count, cached_count, failed_count, len(programs),
        )

        # 4. Build database
        self.builder.build(programs)

    def _load_existing_enrichments(self) -> dict:
        """Load enrichments from existing database for incremental mode."""
        enrichments = {}
        try:
            conn = sqlite3.connect(f"file:{self.builder.db_path}?mode=ro", uri=True)
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                "SELECT id, checksum, enrichment_json FROM foerdermittel"
            ).fetchall()

            for row in rows:
                if row["enrichment_json"]:
                    try:
                        enrichment_data = json.loads(row["enrichment_json"])
                        # Reconstruct the full program dict from DB
                        full_row = conn.execute(
                            "SELECT * FROM foerdermittel WHERE id = ?",
                            [row["id"]],
                        ).fetchone()
                        if full_row:
                            prog = dict(full_row)
                            prog.pop("rowid", None)
                            enrichments[row["id"]] = {
                                "checksum": row["checksum"],
                                "program": prog,
                            }
                    except (json.JSONDecodeError, TypeError):
                        continue

            conn.close()
        except Exception as e:
            logger.warning("Could not load existing enrichments: %s", e)

        return enrichments

    @staticmethod
    def _dedupe(items: list) -> list:
        """Remove duplicates while preserving order."""
        seen = set()
        result = []
        for item in items:
            if item not in seen:
                seen.add(item)
                result.append(item)
        return result

    @staticmethod
    def _merge_program(transformed: dict, enrichment: dict) -> dict:
        """Merge transformed parquet data with Claude enrichment."""
        _d = Pipeline._dedupe
        prog = dict(transformed)
        prog.update({
            "clean_title": enrichment.get("clean_title"),
            "super_kategorien": _d(enrichment.get("super_kategorien", [])),
            "themen": _d(enrichment.get("themen", [])),
            "zielgruppen": _d(enrichment.get("zielgruppen", []))[:10],
            "foerderart": _d(enrichment.get("foerderart", [])),
            "short_summary": enrichment.get("short_summary"),
            "eligibility_summary": enrichment.get("eligibility_summary"),
            "funding_summary": enrichment.get("funding_summary"),
            "bundesland": enrichment.get("bundesland"),
            "funding_type_normalized": enrichment.get("funding_type_normalized"),
            "application_deadline": enrichment.get("application_deadline"),
            "deadline_type": enrichment.get("deadline_type"),
            "enrichment_json": json.dumps(enrichment, ensure_ascii=False),
            "enriched_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        })
        return prog


# ---------------------------------------------------------------------------
# Stats
# ---------------------------------------------------------------------------


def print_stats(db_path: str = DB_PATH):
    """Print database statistics."""
    if not os.path.exists(db_path):
        logger.error("Database not found: %s", db_path)
        sys.exit(1)

    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)

    total = conn.execute("SELECT COUNT(*) FROM foerdermittel").fetchone()[0]
    enriched = conn.execute(
        "SELECT COUNT(*) FROM foerdermittel WHERE enrichment_json IS NOT NULL"
    ).fetchone()[0]
    print(f"\n{'='*60}")
    print(f"Fördermittel Database Statistics")
    print(f"{'='*60}")
    print(f"Total programs:     {total}")
    print(f"Enriched:           {enriched}")

    # Bundesland distribution
    print(f"\nBundesland distribution:")
    rows = conn.execute(
        "SELECT bundesland, COUNT(*) as cnt FROM foerdermittel "
        "WHERE bundesland IS NOT NULL GROUP BY bundesland ORDER BY cnt DESC"
    ).fetchall()
    for r in rows:
        print(f"  {r[0]:30s} {r[1]:5d}")

    # Funding type distribution
    print(f"\nFunding type distribution:")
    rows = conn.execute(
        "SELECT funding_type_normalized, COUNT(*) as cnt FROM foerdermittel "
        "WHERE funding_type_normalized IS NOT NULL GROUP BY funding_type_normalized ORDER BY cnt DESC"
    ).fetchall()
    for r in rows:
        print(f"  {r[0]:30s} {r[1]:5d}")

    # Top super-categories
    print(f"\nSuper-Kategorien (top tags):")
    rows = conn.execute(
        "SELECT super_kategorien FROM foerdermittel WHERE super_kategorien IS NOT NULL"
    ).fetchall()
    from collections import Counter
    cat_counter: Counter = Counter()
    for (raw,) in rows:
        try:
            cats = json.loads(raw) if isinstance(raw, str) else raw
            if isinstance(cats, list):
                cat_counter.update(cats)
        except (json.JSONDecodeError, TypeError):
            pass
    for cat, count in cat_counter.most_common():
        print(f"  {cat:30s} {count:5d}")

    # Top themen
    print(f"\nThemen (top 20):")
    rows = conn.execute(
        "SELECT themen FROM foerdermittel WHERE themen IS NOT NULL"
    ).fetchall()
    thema_counter: Counter = Counter()
    for (raw,) in rows:
        try:
            tags = json.loads(raw) if isinstance(raw, str) else raw
            if isinstance(tags, list):
                thema_counter.update(tags)
        except (json.JSONDecodeError, TypeError):
            pass
    for tag, count in thema_counter.most_common(20):
        print(f"  {tag:30s} {count:5d}")

    # Deadline types
    print(f"\nDeadline types:")
    rows = conn.execute(
        "SELECT deadline_type, COUNT(*) as cnt FROM foerdermittel "
        "WHERE deadline_type IS NOT NULL GROUP BY deadline_type ORDER BY cnt DESC"
    ).fetchall()
    for r in rows:
        print(f"  {r[0]:30s} {r[1]:5d}")

    print(f"\n{'='*60}")
    conn.close()


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main():
    parser = argparse.ArgumentParser(
        description="Fördermittel enrichment pipeline",
    )
    parser.add_argument(
        "--force", action="store_true",
        help="Re-enrich all programs (ignore cache)",
    )
    parser.add_argument(
        "--limit", type=int, default=None,
        help="Only process N programs (for testing)",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Download and filter only, no API calls",
    )
    parser.add_argument(
        "--stats", action="store_true",
        help="Print database statistics and exit",
    )
    parser.add_argument(
        "--db-path", type=str, default=DB_PATH,
        help="Path to SQLite database",
    )
    parser.add_argument(
        "--provider", type=str, default="openai",
        choices=Enricher.PROVIDERS,
        help="LLM provider (default: openai)",
    )
    parser.add_argument(
        "--model", type=str, default=None,
        help="Model name (default: provider-specific)",
    )
    parser.add_argument(
        "--workers", type=int, default=5,
        help="Parallel API workers (default: 10)",
    )
    parser.add_argument(
        "--dsee", type=str, nargs="?", const=DSEE_DEFAULT_URL, default=None,
        help="Include DSEE programs. Accepts a file path or URL (default: GitHub Pages URL)",
    )
    args = parser.parse_args()

    if args.stats:
        print_stats(args.db_path)
        return

    # Load DSEE data if provided
    extra_programs = None
    if args.dsee:
        dsee_loader = DSEELoader(args.dsee)
        extra_programs = dsee_loader.load()

    taxonomy = TaxonomyLoader()
    downloader = ParquetDownloader()
    enricher = Enricher(taxonomy, provider=args.provider, model=args.model)
    builder = DatabaseBuilder(args.db_path)

    pipeline = Pipeline(taxonomy, downloader, enricher, builder)
    pipeline.run(
        force=args.force,
        limit=args.limit,
        dry_run=args.dry_run,
        workers=args.workers,
        extra_programs=extra_programs,
    )


if __name__ == "__main__":
    main()
