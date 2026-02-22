# Fördermittel MCP Server

MCP server providing structured access to 1200+ German funding programs for NGOs.

Data sourced from [foerderdatenbank.de](https://www.foerderdatenbank.de/) via [CorrelAid's open parquet dump](https://github.com/CorrelAid/fördermittel), enriched with LLM-generated summaries and a clean taxonomy.

## Architecture

```
taxonomy.yaml          ← controlled vocabulary (enums)
       ↓
enrich.py              ← downloads parquet, enriches via Claude Haiku, builds SQLite
       ↓
foerdermittel.db       ← SQLite + FTS5
       ↓
mcp_server.py          ← serves via stdio (local) or SSE (remote)
```

## Setup

```bash
# Clone
git clone https://github.com/yourusername/foerdermittel-mcp.git
cd foerdermittel-mcp

# Install dependencies
pip install -r requirements.txt

# Configure API key
cp .env.example .env
# Edit .env with your Anthropic API key

# Build database (first run enriches ~1200 programs, ~$0.54, ~10 min)
python enrich.py

# Or test with just 5 programs first
python enrich.py --limit 5
```

## Usage

### Enrichment Pipeline

```bash
python enrich.py                    # incremental run (only re-enriches changed programs)
python enrich.py --force            # re-enrich everything
python enrich.py --limit 10         # enrich only 10 (for testing)
python enrich.py --dry-run          # download + filter only, no API calls
python enrich.py --stats            # print DB statistics
```

### MCP Server

**Local (stdio) -- for Claude Code / Claude Desktop:**
```bash
python mcp_server.py
```

**Remote (SSE) -- for remote clients:**
```bash
python mcp_server.py --transport sse              # port 8080
python mcp_server.py --transport sse --port 3000   # custom port
```

### Client Configuration

**Claude Code** (`.mcp.json` or Claude Code settings):
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

**Claude Desktop** (`claude_desktop_config.json`):
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

## MCP Tools

| Tool | Description |
|------|-------------|
| `search_foerderprogramme` | FTS5 + BM25 search with Bundesland/funding type/tag filters |
| `get_foerderprogramm_details` | Full program details by ID |
| `list_upcoming_deadlines` | Programs with deadlines in next N days |
| `get_filter_options` | Available enum values and counts |

### Prompt

| Prompt | Description |
|--------|-------------|
| `find_funding` | Guides systematic search workflow (organization type, location, project) |

## Taxonomy

The taxonomy (`taxonomy.yaml`) defines the controlled vocabulary:

- **14 Super-Kategorien**: Soziales, Umwelt, Kultur, Bildung, Sport, Integration, Forschung, Digitalisierung, Gesundheit, Demokratie, Jugend, Wirtschaft, Tierschutz, Entwicklungszusammenarbeit
- **~55 Themen**: Specific topics grouped under super-categories
- **~35 Zielgruppen**: Target groups (organizational types)
- **8 Förderarten**: Funding mechanisms
- **17 Bundesländer**: 16 states + bundesweit
- **5 Funding Types**: Zuschuss, Kredit, Bürgschaft, Preis, Sonstige
- **4 Deadline Types**: einmalig, laufend, jährlich, geschlossen

The enrichment pipeline uses Claude Haiku with `tool_use` + `tool_choice` to force structured output matching these enums exactly.

## Cost

| Operation | Cost | Time |
|-----------|------|------|
| Full enrichment (1200 programs) | ~$0.54 | ~10 min |
| Incremental run (50-100 changed) | ~$0.02-0.05 | ~1-2 min |
| MCP server | Free | Instant |

## License

MIT
