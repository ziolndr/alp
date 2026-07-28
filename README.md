# A Living Purpose — ARBITER Purpose Field

This is the full field architecture, not a one-person demonstration and not a browser-side array of hard-coded candidates.

A person, goal, plan, or agency question is embedded through:

```text
https://api.arbiter.traut.ai/public/embed
```

The query vector is measured twice:

1. Against A Living Purpose's five owned service pathways.
2. Against the wider local field built from real official-source content.

This keeps the ecosystem broad while making every relevant search capable of becoming an A Living Purpose intake, coordinated plan, or referral relationship.

## What the field contains

The default source registry contains 64 high-value sources across:

- A Living Purpose's own service model
- San Diego Regional Center services, provider operations, resources, and Self-Determination
- California DDS supported living, independent living, Self-Determination, advocacy, and provider guidance
- Employment and supported employment
- Housing and rental assistance
- IHSS, SSI, work incentives, CalABLE, and benefits navigation
- MTS Access, reduced fares, and NCTD LIFT
- Therapeutic recreation, adaptive access, volunteering, and community programs
- Disability rights, self-advocacy, family support, and emergency preparedness
- Local provider programs from Options For All, TMI, Community Interface Services, PWI, The Arc of San Diego, Access to Independence, and EFRC

The source registry is not the search candidate set. During a full build, each registered source is fetched, relevant same-domain pages are discovered, text is extracted and chunked, every chunk is embedded, and those embedded records become the search field.

## No package installation

The application uses Python 3's standard library only.

No:

- pip
- virtual environment
- FastAPI
- Pydantic
- NumPy
- wheel compilation

## Run the complete product

```bash
cd ~/Downloads/A_LIVING_PURPOSE_FIELD_FULL
chmod +x RUN_A_LIVING_PURPOSE_FIELD.command scripts/*.command
./RUN_A_LIVING_PURPOSE_FIELD.command
```

That command:

1. Starts the local field server on `127.0.0.1:8844`.
2. Crawls the registered official sources.
3. Chunks and embeds the retrieved content with public ARBITER.
4. Stores the field in local SQLite.
5. Verifies a live semantic query.
6. Opens the interface.

## Faster first look

This creates a source-level field for all 67 registries plus the five canonical A Living Purpose service pathways without crawling all source pages:

```bash
./scripts/START_BACKGROUND.command
./scripts/BOOTSTRAP_FIELD.command
./scripts/VERIFY.command
open http://127.0.0.1:8844
```

Then replace it with the full page-level field:

```bash
./scripts/BUILD_FIELD.command
```

## Interface

The front end keeps the useful interaction model from LISTEN:

- A large free-text description
- Person, Goal, Plan, and Agency modes
- Person, Family, Case Manager, and Director perspectives
- Ranked ARBITER resonance
- Category filters
- Expandable official-source evidence
- A live ecosystem index
- Real corpus and source counts
- A real human-centered supported-living hero image
- A dedicated A Living Purpose service-pathway section
- A separate "How A Living Purpose can help" measurement above broader results
- Intake, call, and official-service actions on every ALP pathway

There is no patient selector. Any person or situation can be described directly.

## A Living Purpose service spine

The application treats these as first-class owned services rather than ordinary directory entries:

- Supported Living
- Independent Living
- Community Day Services
- Tailored Day Services
- Self-Determination Services

Each full build guarantees five canonical `alp-service-pathway` records even when the public website is temporarily unavailable or its page structure changes. Search responses return `alp_pathways` separately from the wider ecosystem results, so paid ALP services are never lost inside the directory while external resources remain ranked by semantic fit.

### List the owned service pathways

```bash
curl -s http://127.0.0.1:8844/api/alp-services | python3 -m json.tool
```

### Search response structure

`POST /api/search` now returns:

- `alp_pathways` — all five ALP services ranked against the person or goal
- `results` — the broader program, benefit, housing, transportation, employment, recreation, rights, and provider field
- `facets` — category counts for the broader field


## Commands

```bash
./scripts/START_BACKGROUND.command
./scripts/BUILD_FIELD.command
./scripts/BOOTSTRAP_FIELD.command
./scripts/VERIFY.command
./scripts/STATUS.command
./scripts/PROBE_ARBITER.command
./scripts/STOP.command
```

## Add another source

From the running application:

```bash
curl -sS -X POST http://127.0.0.1:8844/api/sources/add \
  -H 'Content-Type: application/json' \
  -d '{
    "title": "Program or resource name",
    "organization": "Organization name",
    "category": "Community Programs",
    "url": "https://official-source.example/program",
    "description": "What the program does, who it serves, and why it belongs in the field.",
    "tags": ["employment", "community", "San Diego"],
    "max_pages": 6,
    "include": ["program", "services", "employment"]
  }'
```

The source is persisted in `data/custom_sources.json`, registered, crawled, chunked, and embedded.

When `PURPOSE_FIELD_API_KEY` is set in `.env`, include:

```bash
-H 'X-Purpose-Field-Key: YOUR_KEY'
```

## API

### Stats

```bash
curl -s http://127.0.0.1:8844/api/stats | python3 -m json.tool
```

### Search the full field

```bash
curl -sS -X POST http://127.0.0.1:8844/api/search \
  -H 'Content-Type: application/json' \
  -d '{
    "query": "An adult wants their own apartment, budgeting support, public transportation, community relationships, and a path to paid work.",
    "mode": "person",
    "perspective": "case_manager",
    "limit": 20
  }' | python3 -m json.tool
```

### Search one support domain

```bash
curl -sS -X POST http://127.0.0.1:8844/api/search \
  -H 'Content-Type: application/json' \
  -d '{
    "query": "transportation for a person who cannot reliably use a fixed bus route",
    "mode": "goal",
    "perspective": "person",
    "categories": ["Transportation"],
    "limit": 20
  }' | python3 -m json.tool
```

### Trigger a background rebuild

```bash
curl -sS -X POST http://127.0.0.1:8844/api/build \
  -H 'Content-Type: application/json' \
  -d '{"reset": false, "metadata_only": false}'
```

## Storage

```text
data/purpose_field.sqlite3
```

Tables:

- `sources` — registered organizations, programs, agencies, and official resource roots
- `records` — crawled source sections and source profiles with 72D vectors
- `embedding_cache` — content-hash vector cache
- `state` — live build progress

## Privacy boundary

The default field contains public program and policy information. No client record is required to use the search surface.

Before any private participant data is stored or persisted:

- Enable a local API key.
- Run behind authenticated HTTPS or a private network.
- Encrypt the device and backups.
- Add role-based permissions and access logs.
- Establish retention and incident-response rules.
- Keep final service, clinical, eligibility, billing, and staffing decisions with authorized people.

The initial product deliberately searches public support infrastructure from a free-text description instead of creating a database around one demonstration participant.

## Fixed: stale service on port 8844

The previous launcher accepted any `/health` response on port 8844. If the earlier 17-object prototype was still running, the full-field launcher mistakenly reported success and `/api/search` returned 404.

The current launcher now:

- reclaims port 8844 before startup;
- verifies the running service reports `architecture: ecosystem-field`;
- verifies the complete 67-source registry;
- fails immediately if the new process exits; and
- runs a direct SQLite/ARBITER verification before the HTTP search test.

## Push to GitHub

```bash
chmod +x PUSH_TO_GITHUB.command
./PUSH_TO_GITHUB.command
```

The configured remote is `https://github.com/ziolndr/alp.git`. Runtime databases, logs, `.env`, caches, and local custom-source state are excluded from Git.


## Crisis and emergency safety layer

Purpose Field includes a non-ranked safety layer for queries that may indicate suicide or self-harm:

- Call or text 988
- 988 online chat
- San Diego Access & Crisis Line: 1-888-724-7240
- San Diego Mobile Crisis Response Teams, requested through 988 or the Access & Crisis Line
- 911 for immediate danger, threats of violence, or a medical emergency

The emergency panel is available even when the semantic field has not been built or ARBITER is unavailable. Crisis resources are pinned and never scored, diagnosed, filtered, or delayed by ARBITER.
