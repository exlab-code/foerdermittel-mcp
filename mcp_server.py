#!/usr/bin/env python3
"""MCP server for searching Förderprogramme (funding programs for NGOs).

Serves via stdio (local) or SSE (remote). Reads from a local SQLite database
with FTS5 full-text search. Run enrich.py first to build the database.

Usage:
    python mcp_server.py                         # stdio (for local Claude Code)
    python mcp_server.py --transport sse          # SSE on port 8080
    python mcp_server.py --transport sse --port 3000
"""

import argparse
import json
import os
import sqlite3
import sys
from datetime import date, timedelta

from mcp.server.fastmcp import FastMCP

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "foerdermittel.db")

mcp = FastMCP(
    "Fördermittel",
    instructions=(
        "Dieser Server bietet Zugang zu über 1200 Förderprogrammen für gemeinnützige "
        "Organisationen in Deutschland. Die Daten stammen von foerderdatenbank.de "
        "(CorrelAid) und werden mit LLM-generierten Zusammenfassungen und Taxonomie "
        "angereichert.\n\n"
        "Workflow:\n"
        "1. get_filter_options() → verfügbare Filter sehen\n"
        "2. search_foerderprogramme() → Programme suchen\n"
        "3. get_foerderprogramm_details() → Details abrufen\n"
        "4. list_upcoming_deadlines() → Fristen prüfen\n\n"
        "Für systematische Beratung nutze den find_funding Prompt."
    ),
)


def _get_db() -> sqlite3.Connection:
    """Open a read-only connection to the SQLite database."""
    if not os.path.exists(DB_PATH):
        raise FileNotFoundError(
            f"Database not found at {DB_PATH}. "
            "Run 'python enrich.py' first to build the database."
        )
    conn = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    return conn


def _row_to_dict(row: sqlite3.Row) -> dict:
    """Convert a sqlite3.Row to a dict, parsing JSON fields."""
    d = dict(row)
    d.pop("rowid", None)
    for field in ("super_kategorien", "themen", "zielgruppen", "foerderart",
                  "enrichment_json", "further_links"):
        if d.get(field):
            try:
                d[field] = json.loads(d[field])
            except (json.JSONDecodeError, TypeError):
                pass
    return d


def _format_program_summary(program: dict) -> dict:
    """Return a compact summary of a program for search results."""
    return {
        "id": program["id"],
        "title": program.get("clean_title") or program["title"],
        "original_title": program["title"],
        "short_summary": program.get("short_summary"),
        "institution_name": program.get("institution_name"),
        "funding_body": program.get("funding_body"),
        "bundesland": program.get("bundesland"),
        "funding_type_normalized": program.get("funding_type_normalized"),
        "funding_summary": program.get("funding_summary"),
        "application_deadline": program.get("application_deadline"),
        "deadline_type": program.get("deadline_type"),
        "super_kategorien": program.get("super_kategorien", []),
        "themen": program.get("themen", []),
        "source_url": program.get("source_url"),
    }


# ---------------------------------------------------------------------------
# Tools
# ---------------------------------------------------------------------------


@mcp.tool()
def search_foerderprogramme(
    query: str,
    bundesland: str | None = None,
    funding_type: str | None = None,
    tags: str | None = None,
    max_results: int = 10,
) -> list[dict]:
    """Search funding programs by keyword with optional filters.

    Args:
        query: Search terms (German). Searches title, description,
               short_summary, eligibility_summary, funding_summary,
               and funding organization.
        bundesland: Filter by state, e.g. "Bayern", "bundesweit".
        funding_type: Filter by type: Zuschuss, Kredit, Bürgschaft, Preis, Sonstige.
        tags: Comma-separated tags to filter by (matched against themen/super_kategorien JSON).
        max_results: Maximum number of results to return (default 10, max 50).
    """
    max_results = min(max_results, 50)
    conn = _get_db()
    try:
        words = [w for w in query.strip().split() if w]

        # BM25 column weights: clean_title, title, short_summary,
        #   eligibility_summary, funding_summary, institution_name, eligible_applicants
        bm25_weights = "bm25(foerdermittel_fts, 10.0, 8.0, 5.0, 3.0, 2.0, 3.0, 2.0)"

        # Try AND first (all terms must appear), fall back to OR
        fts_query_and = " AND ".join(f'"{w}"' for w in words)
        fts_query_or = " OR ".join(f'"{w}"' for w in words)

        def _build_sql(fts_query: str) -> tuple[str, list]:
            sql = f"""
                SELECT f.*, {bm25_weights} AS bm25_score
                FROM foerdermittel_fts fts
                JOIN foerdermittel f ON f.rowid = fts.rowid
                WHERE foerdermittel_fts MATCH ?
            """
            params: list = [fts_query]

            if bundesland:
                sql += " AND (f.bundesland = ? OR f.bundesland = 'bundesweit')"
                params.append(bundesland)

            if funding_type:
                sql += " AND f.funding_type_normalized = ?"
                params.append(funding_type)

            if tags:
                for tag in (t.strip() for t in tags.split(",") if t.strip()):
                    sql += (
                        " AND (f.super_kategorien LIKE ? "
                        "OR f.themen LIKE ? "
                        "OR f.zielgruppen LIKE ?)"
                    )
                    pattern = f"%{tag}%"
                    params.extend([pattern, pattern, pattern])

            sql += " ORDER BY bm25_score LIMIT ?"
            params.append(max_results)
            return sql, params

        # AND query first
        sql, params = _build_sql(fts_query_and)
        rows = conn.execute(sql, params).fetchall()

        # Fall back to OR if no results
        if not rows:
            sql, params = _build_sql(fts_query_or)
            rows = conn.execute(sql, params).fetchall()

        results = [_format_program_summary(_row_to_dict(r)) for r in rows]
        return results if results else [
            {"message": "Keine Ergebnisse gefunden. Versuche andere Suchbegriffe oder weniger Filter."}
        ]
    finally:
        conn.close()


@mcp.tool()
def get_foerderprogramm_details(id: str) -> dict:
    """Get full details for a specific funding program by its ID.

    Args:
        id: The program ID (id_hash from search results).
    """
    conn = _get_db()
    try:
        row = conn.execute(
            "SELECT * FROM foerdermittel WHERE id = ?", [id]
        ).fetchone()
        if not row:
            return {"error": f"Kein Förderprogramm mit ID {id} gefunden."}
        d = _row_to_dict(row)
        # Remove the cached enrichment JSON from the response (redundant)
        d.pop("enrichment_json", None)
        return d
    finally:
        conn.close()


@mcp.tool()
def list_upcoming_deadlines(days: int = 90) -> list[dict]:
    """List funding programs with deadlines in the next N days.

    Args:
        days: Number of days to look ahead (default 90, max 365).
    """
    days = min(days, 365)
    today = date.today().isoformat()
    cutoff = (date.today() + timedelta(days=days)).isoformat()

    conn = _get_db()
    try:
        rows = conn.execute(
            """
            SELECT * FROM foerdermittel
            WHERE application_deadline >= ?
              AND application_deadline <= ?
              AND deadline_type != 'geschlossen'
            ORDER BY application_deadline ASC
            LIMIT 50
            """,
            [today, cutoff],
        ).fetchall()

        results = [_format_program_summary(_row_to_dict(r)) for r in rows]

        # Include top ongoing programs
        ongoing = conn.execute(
            """
            SELECT * FROM foerdermittel
            WHERE deadline_type = 'laufend'
            ORDER BY title
            LIMIT 10
            """,
        ).fetchall()

        if ongoing:
            results.append({"section": "Laufende Programme (keine feste Frist)"})
            results.extend(_format_program_summary(_row_to_dict(r)) for r in ongoing)

        return results if results else [
            {"message": f"Keine Fristen in den nächsten {days} Tagen gefunden."}
        ]
    finally:
        conn.close()


@mcp.tool()
def get_filter_options() -> dict:
    """Get available filter values for Bundesland, funding type, tags, etc.

    Call this first to understand what filter values are available
    before using search_foerderprogramme.
    """
    conn = _get_db()
    try:
        def distinct_values(col: str) -> list[str]:
            rows = conn.execute(
                f"SELECT DISTINCT {col} FROM foerdermittel "
                f"WHERE {col} IS NOT NULL ORDER BY {col}"
            ).fetchall()
            return [r[0] for r in rows]

        # Extract unique tags from JSON columns
        super_kategorien: set[str] = set()
        themen: set[str] = set()
        zielgruppen: set[str] = set()

        for col, target in [
            ("super_kategorien", super_kategorien),
            ("themen", themen),
            ("zielgruppen", zielgruppen),
        ]:
            rows = conn.execute(
                f"SELECT {col} FROM foerdermittel WHERE {col} IS NOT NULL"
            ).fetchall()
            for (raw,) in rows:
                try:
                    tags = json.loads(raw) if isinstance(raw, str) else raw
                    if isinstance(tags, list):
                        target.update(tags)
                except (json.JSONDecodeError, TypeError):
                    continue

        total = conn.execute("SELECT COUNT(*) FROM foerdermittel").fetchone()[0]
        enriched = conn.execute(
            "SELECT COUNT(*) FROM foerdermittel WHERE enrichment_json IS NOT NULL"
        ).fetchone()[0]

        return {
            "total_programs": total,
            "enriched_programs": enriched,
            "bundeslaender": distinct_values("bundesland"),
            "funding_types": distinct_values("funding_type_normalized"),
            "deadline_types": distinct_values("deadline_type"),
            "super_kategorien": sorted(super_kategorien),
            "themen": sorted(themen),
            "zielgruppen": sorted(zielgruppen),
        }
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Prompt
# ---------------------------------------------------------------------------


@mcp.prompt()
def find_funding(
    organization_type: str,
    location: str,
    project_description: str,
) -> str:
    """Guide a systematic search for relevant funding programs.

    Args:
        organization_type: Type of organization (e.g. "eingetragener Verein", "gGmbH").
        location: Where the organization is based (e.g. "München, Bayern").
        project_description: What the project/activity is about.
    """
    return f"""Du hilfst einer gemeinnützigen Organisation, passende Förderprogramme zu finden.

**Organisation:** {organization_type}
**Standort:** {location}
**Projektbeschreibung:** {project_description}

Gehe systematisch vor:

1. **Rufe zuerst get_filter_options() auf**, um die verfügbaren Filter zu sehen.

2. **Führe mehrere Suchen durch** mit verschiedenen Suchbegriffen:
   - Suche nach Schlüsselwörtern aus der Projektbeschreibung
   - Suche nach der Zielgruppe / Organisationstyp
   - Suche nach dem Themenbereich
   - Filtere nach dem passenden Bundesland (aus dem Standort ableiten)
   - Probiere auch verwandte Begriffe und Synonyme

3. **Prüfe die Details** der vielversprechendsten Treffer mit get_foerderprogramm_details().

4. **Prüfe aktuelle Fristen** mit list_upcoming_deadlines().

5. **Erstelle eine Zusammenfassung** mit:
   - Den 5-10 relevantesten Förderprogrammen
   - Für jedes: Titel, Fördergeber, Fördersumme, Frist, und warum es passt
   - Hinweise auf bald ablaufende Fristen
   - Empfehlung, welche Programme am besten passen und warum
   - Nächste Schritte (was muss die Organisation tun)

Antworte auf Deutsch. Sei konkret und praxisorientiert."""


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main():
    parser = argparse.ArgumentParser(description="Fördermittel MCP Server")
    parser.add_argument(
        "--transport",
        choices=["stdio", "sse"],
        default="stdio",
        help="Transport mode (default: stdio)",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=8080,
        help="Port for SSE transport (default: 8080)",
    )
    args = parser.parse_args()

    if args.transport == "sse":
        import uvicorn
        from starlette.applications import Starlette
        from starlette.responses import FileResponse, JSONResponse
        from starlette.routing import Route, Mount

        async def download_db(request):
            if os.path.exists(DB_PATH):
                return FileResponse(DB_PATH, filename="foerdermittel.db")
            return JSONResponse({"error": "Database not found"}, status_code=404)

        app = Starlette(routes=[
            Route("/foerdermittel.db", download_db),
            Mount("/", app=mcp.sse_app()),
        ])

        uvicorn.run(app, host="0.0.0.0", port=args.port)
    else:
        mcp.run()


if __name__ == "__main__":
    main()
