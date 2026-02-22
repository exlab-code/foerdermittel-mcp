# Fördermittel MCP Server

MCP server providing structured access to 2000+ German funding programs for NGOs.

Data sourced from [foerderdatenbank.de](https://www.foerderdatenbank.de/) via [CorrelAid's open parquet dump](https://github.com/CorrelAid/foerdermittel-scraper), enriched with LLM-generated summaries and a controlled taxonomy.

## Architecture

```
CorrelAid parquet (updated every ~2 days)
       ↓
enrich.py              ← downloads parquet, enriches via LLM (OpenAI/Anthropic), builds SQLite
       ↓
foerdermittel.db       ← SQLite + FTS5 full-text search
       ↓
mcp_server.py          ← MCP server (stdio/SSE) + REST API
```

`taxonomy.yaml` defines the controlled vocabulary (super-categories, topics, target groups, funding types, states) used as enum constraints during LLM enrichment.

## Setup

```bash
git clone https://github.com/exlab-code/foerdermittel-mcp.git
cd foerdermittel-mcp
pip install -r requirements.txt

# Configure API key
cp .env.example .env
# Edit .env with your OpenAI API key (default provider)
```

### Build the database

```bash
# Full run (~2000 programs, ~$1-2 with gpt-4o-mini, ~15 min)
python enrich.py

# Test with a few programs first
python enrich.py --limit 5

# Subsequent runs are incremental (only re-enriches changed programs)
python enrich.py
```

### Enrichment options

```bash
python enrich.py                        # incremental run
python enrich.py --force                # re-enrich everything
python enrich.py --limit 10             # only N programs (for testing)
python enrich.py --dry-run              # download + filter only, no API calls
python enrich.py --stats                # print DB statistics
python enrich.py --provider anthropic   # use Anthropic instead of OpenAI
python enrich.py --model gpt-4o         # use a specific model
```

## MCP Server

### Local (stdio) — for Claude Code / Claude Desktop

```bash
python mcp_server.py
```

### Remote (SSE) — for shared/public access

```bash
python mcp_server.py --transport sse               # default port 8080
python mcp_server.py --transport sse --port 3000    # custom port
```

### Client configuration

Add to your Claude Code `.mcp.json` or Claude Desktop `claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "foerdermittel": {
      "command": "python3",
      "args": ["/path/to/foerdermittel-mcp/mcp_server.py"]
    }
  }
}
```

For a remote SSE server, use the URL-based config instead.

## REST API

In SSE mode, the server also exposes a REST API for non-MCP clients (ChatGPT Custom GPTs, web apps, scripts, etc.). All endpoints return JSON and support CORS.

### Endpoints

| Endpoint | Description |
|----------|-------------|
| `GET /api/search?q=Bildung` | Search programs by keyword |
| `GET /api/program/{id}` | Full details for a program |
| `GET /api/deadlines?days=90` | Programs with upcoming deadlines |
| `GET /api/filters` | Available filter values |

### Search parameters

| Parameter | Description |
|-----------|-------------|
| `q` | Search query (required) |
| `bundesland` | Filter by state, e.g. `Bayern`, `bundesweit` |
| `funding_type` | Filter by type: `Zuschuss`, `Kredit`, `Bürgschaft`, `Preis`, `Sonstige` |
| `tags` | Comma-separated tags |
| `limit` | Max results (default 10, max 50) |

### Examples

```bash
# Search for education funding in Berlin
curl "https://your-server/api/search?q=Bildung&bundesland=Berlin"

# Get program details
curl "https://your-server/api/program/abc123"

# Upcoming deadlines (next 60 days)
curl "https://your-server/api/deadlines?days=60"

# Available filters
curl "https://your-server/api/filters"
```

### MCP Tools

| Tool | Description |
|------|-------------|
| `search_foerderprogramme` | Full-text search (BM25) with filters for Bundesland, funding type, and tags |
| `get_foerderprogramm_details` | Full program details by ID |
| `list_upcoming_deadlines` | Programs with deadlines in the next N days |
| `get_filter_options` | Available filter values and program counts |

### Prompt

| Prompt | Description |
|--------|-------------|
| `find_funding` | Guided workflow: searches by organization type, location, and project description |

## Taxonomy

The taxonomy (`taxonomy.yaml`) defines the controlled vocabulary enforced via LLM tool calling:

- **14 Super-Kategorien** — Soziales, Umwelt, Kultur, Bildung, Sport, Integration, Forschung, Digitalisierung, Gesundheit, Demokratie, Jugend, Wirtschaft, Tierschutz, Entwicklungszusammenarbeit
- **~55 Themen** — Specific topics
- **~35 Zielgruppen** — Target groups (organizational types)
- **8 Förderarten** — Funding mechanisms
- **17 Bundesländer** — 16 states + bundesweit
- **5 Funding Types** — Zuschuss, Kredit, Bürgschaft, Preis, Sonstige
- **4 Deadline Types** — einmalig, laufend, jährlich, geschlossen

## Deployment

Deploy with any container platform (Coolify, Docker, etc.). The server runs in SSE mode for remote access.

```bash
python mcp_server.py --transport sse --port 8080
```

In SSE mode, the server serves both the REST API (`/api/*`) and the raw DB file at `/foerdermittel.db` for downstream consumers.

### Keeping data up to date

Set up a cron job on the server to re-run the enrichment pipeline (incremental — only processes new/changed programs):

```bash
# Every 2 days at 5:00 UTC
0 5 */2 * * cd /path/to/foerdermittel-mcp && python enrich.py
```

### Environment variables

| Variable | Purpose |
|----------|---------|
| `OPENAI_API_KEY` | LLM enrichment (gpt-4o-mini) |

## Cost

| Operation | Estimated cost | Time |
|-----------|---------------|------|
| Full enrichment (~2000 programs) | ~$1-2 (gpt-4o-mini) | ~15 min |
| Incremental run (50-100 changed) | ~$0.02-0.05 | ~1-2 min |
| MCP server | Free | Instant |

## License

MIT
