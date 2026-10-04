# Declarative CDC Data Migration Engine (DB Migrator)

> A conceptual prototype for an event-driven relational database migration and transformation engine based on Change Data Capture (CDC) and declarative JSON mapping configurations.

---

## Project Status

**Note:** This repository is an experimental prototype developed as part of an undergraduate thesis project. It is **not** a production-ready database migration engine. Several core capabilities—such as full transactional rollbacks across distributed split operations, complete handling of live `UPDATE`/`DELETE` CDC events, arbitrary nested dependency topologies, and robust error recovery—are exploratory and require further architectural refinement.

---

## Overview

Traditional database migrations between differing schemas typically rely on writing procedural SQL scripts or executing offline batch ETL jobs. These approaches are often tightly coupled to specific schemas, challenging to maintain, and require taking systems offline to prevent inconsistency.

This project investigates a declarative, decoupled alternative:

- **Change Data Capture (CDC)**: Captures row-level database changes from the source PostgreSQL Write-Ahead Log (WAL) in real-time via Debezium and Apache Kafka.
- **Declarative JSON Mapping**: Defines source-to-target entity transformations, column mappings, and key generation strategies in structured JSON configurations rather than manual SQL scripts.
- **Decoupled Worker Processing**: A staging database (**Pivot Database**) buffers incoming CDC fragments, coordinates multi-table joins, resolves surrogate/foreign keys, and queues assembled SQL statements for target execution.

---

> This README focuses on the repository structure and implementation details. For a broader project background, architecture walkthrough, and case study diagrams, the [presentation slides](https://drive.google.com/file/d/1N0RoOYR53Mbn-hrKf2I7h46jqxknPHNF/view?usp=sharing) may be a more accessible starting point.

---

## Repository Structure

```
.
├── api/                               # Go REST API orchestration layer
│   ├── connector.go                   # Debezium connector registration endpoint
│   ├── database.go                    # Schema discovery and database credentials API
│   ├── mapping.go                     # Mapping JSON file CRUD endpoints
│   ├── queue.go                       # Migration queue and progress monitoring endpoints
│   ├── server.go                      # HTTP server initialization and Gin route registration
│   ├── util.go                        # API response formatting helpers
│   └── worker.go                      # Worker process lifecycle management (start/stop/status)
├── client/                            # Frontend dashboard (React + Vite + TypeScript)
│   ├── src/
│   │   ├── components/                # Shared UI and entity mapping form sections
│   │   ├── feature/
│   │   │   ├── connection/            # Database and Debezium connection configuration pages
│   │   │   ├── entity-json/           # Visual mapping editor and schema mapping forms
│   │   │   └── execution/             # Worker control panel, queue monitor, and log viewer
│   │   └── main.tsx                   # Client entry point
│   ├── package.json
│   └── vite.config.ts
├── cmd/                               # Executable entry points
│   ├── checker/main.go                # Key resolution and identity worker
│   ├── executor/main.go               # Target database write execution worker
│   ├── joiner/main.go                 # Multi-source aggregation worker
│   ├── parser/main.go                 # Kafka CDC event consumer and parser worker
│   └── seeder/                        # Test data generation and seeding CLI
├── internal/                          # Core backend logic and shared packages
│   ├── app/                           # Worker bootstrap and initialization helpers
│   ├── checker/                       # Checker worker processing loop and keymap logic
│   ├── config/                        # Environment config loader and SQL DDL schemas
│   │   ├── config.go                  # Global application configuration struct
│   │   ├── json/publication/          # Active declarative JSON mapping configurations
│   │   └── pivot_db.sql               # Pivot Database table definitions
│   ├── debezium/                      # Debezium connector JSON configuration templates
│   ├── executor/                      # Target execution loop and batch query runner
│   ├── joiner/                        # Join worker aggregation and fragment assembly
│   ├── kafka/                         # Kafka consumer implementation and Debezium payload types
│   ├── mapping/                       # JSON mapping parser, schema definitions, and planner
│   ├── pipeline/                      # Processor pipeline for routing events to Pivot DB
│   ├── pivot/                         # Pivot DB repository (CRUD queries for queue, keymap, state)
│   └── sqlbuilder/                    # Dynamic SQL generation utility
├── migration/                         # SQL DDL schemas for practice and staging databases
│   ├── pivot/                         # Pivot DB teardown and cleanup scripts
│   └── publication/                   # Publication case study schemas (old and new schemas)
├── main.go                            # Entry point for the API server
├── Makefile                           # Development and orchestration tasks
├── docker-compose.base.yml            # Core infrastructure (Kafka, Debezium, AKHQ, Pivot DB)
└── docker-compose.publication.yml     # Test case source and target PostgreSQL databases
```

---

## Architecture

The system separates configuration management, orchestration, message streaming, and background transformation processing into decoupled layers.

```
                    ┌────────────────────────┐
                    │    Client Dashboard    │
                    │ (React / Vite / TS)    │
                    └───────────┬────────────┘
                                │ HTTP / REST
                                ▼
                    ┌────────────────────────┐
                    │       API Server       │
                    │      (Go / Gin)        │
                    └───────────┬────────────┘
                                │ Process Management / Status
              ┌─────────────────┼─────────────────┬─────────────────┐
              ▼                 ▼                 ▼                 ▼
       ┌─────────────┐   ┌─────────────┐   ┌─────────────┐   ┌─────────────┐
       │   Parser    │   │   Joiner    │   │   Checker   │   │  Executor   │
       │   Worker    │   │   Worker    │   │   Worker    │   │   Worker    │
       └──────┬──────┘   └──────┬──────┘   └──────┬──────┘   └──────┬──────┘
              │                 │                 │                 │
              │ Reads Kafka     │ Staging Read/Write               │ Writes Target
              ▼                 ▼                 ▼                 ▼
       ┌─────────────┐   ┌───────────────────────────────┐   ┌─────────────┐
       │ Debezium /  │   │        Pivot Database         │   │   Target    │
       │ Apache Kafka│   │  (_exec_queue, _keymap_*, ..) │   │  Database   │
       └─────────────┘   └───────────────────────────────┘   └─────────────┘
```

---

## Worker Backend

The data transformation engine is implemented in Go under `internal/`. Instead of running as a single monolithic daemon, it is split into four distinct worker processes located under `cmd/`. Each worker handles a specific processing phase and communicates asynchronously via the Pivot Database.

```
cmd/
├── parser/main.go     # Step 1: Ingest & Parse
├── joiner/main.go     # Step 2: Aggregate Multi-source Fragments
├── checker/main.go    # Step 3: Resolve Keys & Dependencies
└── executor/main.go   # Step 4: Write to Target Database
```

### 1. Parser (`cmd/parser/main.go` & `internal/pipeline/`)

- **Input**: CDC event messages from Kafka topics created by Debezium.
- **Mechanism**:
  - Loads mapping definitions using `internal/mapping/planner.go` to determine which Kafka topics map to which entities.
  - For simple, single-source mappings, it extracts fields, applies type casting (via `internal/cast/`), constructs the base SQL query text, and inserts a pending entry directly into `_exec_queue`.
  - For multi-source join mappings (N:1), it stores the incoming topic fragment into `_join_map` / `_join_map_topic` and enqueues a placeholder in `_need_join`.

### 2. Joiner (`cmd/joiner/main.go` & `internal/joiner/`)

- **Input**: Pending join records in `_need_join` and topic fragments in `_join_map_topic`.
- **Mechanism**:
  - Periodically polls `_need_join` in configurable batch sizes (`BatchMaxRows`).
  - Checks whether all required dimension/fact table fragments for the join key have arrived in `_join_map_topic`.
  - If complete, it combines the fragmented payloads, compiles the final SQL `INSERT` statement and arguments, writes the task into `_exec_queue`, and marks the join record as completed in `_need_join`.
  - If fragments are still missing, it increments the attempt count and applies backoff retry logic (`MaximumJoinAttempts`).

### 3. Checker (`cmd/checker/main.go` & `internal/checker/`)

- **Input**: Items in `_exec_queue` marked with `need_keymap = true` and `status = 'pending'`.
- **Mechanism**:
  - Scans for queue items that require primary/foreign key translation between source and target systems.
  - Resolves or generates keymap requests against `_keymap_generic` in the Pivot DB.
  - Once required key dependencies are resolved, it updates the `_exec_queue` item status to `ready` so that the Executor can process it. Items not requiring keymaps are marked `ready` immediately.

### 4. Executor (`cmd/executor/main.go` & `internal/executor/`)

- **Input**: Queue entries in `_exec_queue` with status `ready`.
- **Mechanism**:
  - Fetches batches of ready items from the Pivot Database and acquires execution locks (`MarkExecuting`).
  - Opens a transaction on the target database (`TargetDSN`).
  - Executes the prepared SQL statements against the target database.
  - If the primary insert uses SQL `RETURNING` (e.g. to generate an auto-increment ID or UUID), it fulfills the corresponding entry in `_keymap_generic`.
  - If the mapping specifies split child tables (`_exec_split`), it replaces `__KEYMAP_PLACEHOLDER__` tokens with the fulfilled parent key and executes child inserts sequentially.
  - Marks processed items in `_exec_queue` and `_exec_split` as `done` or `error`.

---

## Mapping Configuration

Mapping files are stored as JSON files under:

```
internal/config/json/publication/
```

These files serve as the configuration consumed by the workers at startup.

### Example Mapping Structure (`conference.json`)

```json
{
  "entity": "conference",
  "sources": [
    { "alias": "pcn", "from": "publication", "topic": "" },
    {
      "alias": "cny",
      "from": "country",
      "join": { "fact_column": "pub_cntry", "dim_column": "cntry_id" },
      "topic": ""
    },
    {
      "alias": "cre",
      "from": "conference",
      "join": { "fact_column": "pub_id", "dim_column": "conf_id" },
      "topic": ""
    }
  ],
  "target_table": "publication",
  "key": {
    "strategy": "shared_key",
    "source": "pcn.pub_id",
    "resolver": {
      "type": "mapping_table",
      "table": "_keymap_conference",
      "source_key_col": "",
      "target_key_col": ""
    }
  },
  "columns": {
    "code": { "from": "$key" },
    "title": { "from": "cre.conf_name" },
    "place": { "from": "pcn.pub_loc" },
    "country": { "from": "cny.cntry_name_en" },
    "year": { "from": "pcn.pub_year" },
    "publisher_name": { "from": "pcn.pub_edit" },
    "publicationtype_name": { "default": "CONFERENCE" }
  },
  "split_table": [
    {
      "table_name": "paper",
      "columns": {
        "doi": { "from": "pcn.pub_id", "cast": "string" },
        "title": { "from": "pcn.pub_title" },
        "startpage": { "from": "cre.conf_start_page" },
        "endpage": { "from": "cre.conf_end_page" },
        "publication_code": { "from": "$key" }
      }
    }
  ],
  "routing": {
    "on_create": { "mode": "insert" },
    "on_update": { "mode": "update", "matchKey": ["code"] },
    "on_snapshot": { "mode": "insert" }
  }
}
```

### Key Mapping Fields:

- **`sources`**: Specifies primary (fact) and secondary (dimension) source tables, their aliases, and join foreign key constraints.
- **`target_table`**: The destination table in the target database.
- **`key`**: Defines how the primary key is resolved (`natural` or `shared_key`) and the keymap lookup table name.
- **`columns`**: Defines column-level mapping rules, including direct extraction (`from`), default literals (`default`), scalar casting (`cast`), and dynamic expressions (`expr`).
- **`split_table`**: Defines secondary/child tables to insert into (e.g. splitting a flat record into `publication` and `paper`), where `$key` references the parent table's generated key.
- **`routing`**: Specifies operational behavior for CDC insert, update, and snapshot modes.

---

## API Layer

The API is implemented using Go and the Gin framework under `api/` (with entry point `main.go`).

### Purpose

The worker executables are standalone background processes. The API acts as an orchestration, configuration management, and inspection layer that enables the Client to control and monitor the system.

### Key API Endpoints:

- **Worker Process Management (`api/worker.go`)**:
  - `POST /worker/start`: Spawns the four background worker processes (`go run ./cmd/{parser,joiner,checker,executor}`), redirects stdout/stderr to `logs/<name>.log`, and records process IDs (PIDs).
  - `POST /worker/stop`: Sends termination signals (`SIGTERM` / `SIGKILL`) to running worker processes.
  - `GET /worker/status`: Verifies process vitality by inspecting active PIDs.
- **Mapping Configuration CRUD (`api/mapping.go`)**:
  - `GET /mapping/list`: Scans the mapping directory and returns active entity mapping files.
  - `POST /mapping`, `PUT /mapping`, `DELETE /mapping`: Creates, updates, or deletes JSON mapping files on disk.
- **Database & Schema Introspection (`api/database.go`)**:
  - `GET /database/schema`: Connects to source and target databases to introspect table names, columns, and data types from PostgreSQL's `information_schema`.
  - `POST /database/source`, `POST /database/target`: Validates credentials and stores connection settings in the Pivot DB `configuration` table.
- **Queue & Progress Monitoring (`api/queue.go`)**:
  - `GET /progress/queue/list`: Retrieves paginated rows from `_exec_queue` with search and status filtering.
  - `GET /progress/summary`: Returns aggregate queue counts by status (`pending`, `ready`, `executing`, `done`, `error`).

---

## Client Dashboard

The Client is located under `client/` and is built as a single-page application using **React (v19)**, **Vite**, **TypeScript**, **Tailwind CSS**, and **React Router**.

### Purpose

The Client provides a graphical user interface over the REST API:

1. **Connection Setup**: Manages database connection strings and registers Debezium connectors.
2. **Visual Mapping Editor**: Allows developers to construct entity, source join, key resolution, and column mapping rules visually, serializing the output into backend JSON files via the API.
3. **Execution Control & Monitoring**: Provides UI controls to start/stop the worker processes, inspect real-time progress counters, view execution queues, and read log outputs.

---

## Database and Infrastructure

The system distinguishes between supporting infrastructure, staging state, and actual migration test databases.

```
                              INFRASTRUCTURE
 ┌──────────────────────────────────────────────────────────────────────┐
 │ docker-compose.base.yml                                              │
 │ ├── zookeeper         (Port 2181)                                    │
 │ ├── kafka             (Port 9092, 29092)                             │
 │ ├── debezium          (Port 8083)                                    │
 │ ├── akhq              (Port 8080)                                    │
 │ └── pivot_db          (PostgreSQL - Port configurable via .base.env) │
 └──────────────────────────────────┬───────────────────────────────────┘
                                    │
                             DATA MIGRATION
 ┌──────────────────────────────────┴───────────────────────────────────┐
 │ docker-compose.publication.yml                                       │
 │ ├── old_publication_db (Source Database - Legacy schema)             │
 │ └── new_publication_db (Target Database - Normalized schema)         │
 └──────────────────────────────────────────────────────────────────────┘
```

### 1. Pivot Database (`pivot_db`)

Defined in `docker-compose.base.yml` with schema defined in `internal/config/pivot_db.sql`. It is an intermediate staging and state repository used exclusively by the migration engine:

- `configuration`: Key-value store for application runtime settings and database DSNs.
- `_exec_queue`: Central staging queue containing prepared SQL statements, arguments, returning column requirements, and lifecycle statuses.
- `_exec_split`: Staged child table operations tied to primary queue records.
- `_keymap_generic`: Cross-reference index linking source keys (`src_key`) to target keys (`tgt_key`).
- `_need_join`, `_join_map`, `_join_map_topic`: Intermediate buffers for storing partial CDC payloads until all required join sources arrive.

### 2. Practice Case Databases

Defined in `docker-compose.publication.yml` to simulate real-world schema transformations:

- **`old_publication_db` (Source)**: Contains legacy un-normalized publication data (e.g. denormalized publication, country, and conference tables).
- **`new_publication_db` (Target)**: Contains a modernized, normalized relational schema (e.g. separated `publication`, `paper`, and `authorship` tables).

---

## Conceptual Data Flow

```
+------------------+
| Source Database  |  User executes INSERT
+--------+---------+
         |
         | 1. Reads PostgreSQL WAL Log
         v
+------------------+
| Debezium Connect |
+--------+---------+
         |
         | 2. Streams CDC JSON Event
         v
+------------------+
|   Apache Kafka   |
+--------+---------+
         |
         | 3. Consumes Topic
         v
+------------------+     Writes Raw Staging     +------------------+
|  Parser Worker   +--------------------------->+                  |
+------------------+                            |                  |
                                                |                  |
+------------------+     Assembles Multi-Source |  Pivot Database  |
|  Joiner Worker   +<-------------------------->+                  |
+------------------+     Writes to _exec_queue  |  (_exec_queue,   |
                                                |   _keymap_*,     |
+------------------+     Resolves Key Mappings  |   _join_map)     |
|  Checker Worker  +<-------------------------->+                  |
+------------------+     Marks status='ready'   |                  |
                                                |                  |
+------------------+     Reads 'ready' Items    |                  |
| Executor Worker  +<---------------------------+                  |
+--------+---------+
         |
         | 4. Executes SQL INSERT (Transaction)
         v
+------------------+
| Target Database  |
+------------------+
```

---

## Publication Case Study

The repository includes a concrete case study based on scientific publication metadata to validate migration patterns across different schema structures:

1. **1:1 Direct Mapping**: Moving author and researcher profile records with column renaming and data type casting.
2. **N:1 Entity Aggregation**: Consolidating separate `publication`, `conference`, and `country` tables into a unified `publication` target record.
3. **1:N Entity Decomposition**: Splitting flat legacy publication rows into normalized `publication` and `paper` tables, dynamically assigning generated keys to child records.

SQL schemas and test seed data for this case study are located under `migration/publication/`.

---

## Known Limitations

- **CDC Operation Scope**: The worker engine currently focuses on processing row-level `INSERT` CDC events. Real-time handling of streaming `UPDATE` and `DELETE` operations is partially specified in routing schemas but not fully implemented across all workers.
- **Distributed Transaction Rollbacks**: When executing 1:N table splits, if an insert into a child table fails after the parent record has committed, there is no automatic Saga compensation or rollback mechanism.
- **Join Dependency Depth**: The current join buffer handles single-level joins (star schemas) effectively, but complex, deep cascading multi-level dependency chains may experience synchronization bottlenecks.
- **Pivot Database I/O Bottleneck**: Staging all CDC fragments and intermediate states in a relational PostgreSQL Pivot DB guarantees durability, but creates noticeable database I/O contention under high write throughput.

---

## Running the Project

### Prerequisites

- **Go**: Version 1.24+ (as specified in `go.mod`)
- **Node.js**: Version 18+ and npm
- **Docker & Docker Compose**

### 1. Start Supporting Infrastructure

Start Kafka, Zookeeper, Debezium Connect, AKHQ, and the Pivot Database:

```bash
make run-base
```

Initialize the Pivot Database schema:

```bash
make up-pivot
```

### 2. Start Migration Test Databases

Start the source (`old_publication`) and target (`new_publication`) PostgreSQL instances:

```bash
make run-pub
```

Apply test schemas and seed initial data:

```bash
make add-old-publication
make add-new-publication
```

### 3. Register Debezium CDC Connector

Register the PostgreSQL CDC connector with Debezium:

```bash
make conn-publication
```

### 4. Start API Server and Client Dashboard

In one terminal, start the Go API server (default port `8088` or as configured):

```bash
make api
```

In another terminal, start the React frontend dashboard:

```bash
make client
```

- The Client dashboard is available at `http://localhost:5173`.
- The AKHQ Kafka management UI is available at `http://localhost:8080`.
- Debezium Connect REST API is available at `http://localhost:8083`.

### 5. Run the Worker Processes

Worker processes can be triggered automatically via the Client UI / API, or started manually in individual terminals:

```bash
make parser     # Kafka consumer & staging worker
make joiner     # N:1 aggregation worker
make checker    # Key resolution worker
make executor   # Target DB execution worker
```

---

## References

- **Full Thesis Document**: [Download PDF](https://drive.google.com/file/d/1JexHKJLOF9bDmpZEsbosvIXhTp_PMlMF/view?usp=sharing)
- **Presentation Slides**: [View PDF](https://drive.google.com/file/d/1N0RoOYR53Mbn-hrKf2I7h46jqxknPHNF/view?usp=sharing)
- **Publication Schema Reference**: [DAMI Framework Paper (arXiv:2504.17662)](https://doi.org/10.48550/arXiv.2504.17662)
