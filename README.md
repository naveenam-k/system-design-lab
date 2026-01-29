# system-design-lab

A hands-on lab for mastering large-scale system design through real implementations, load testing, and iterative refinement.

## Directory Structure Reference

```
system-design-lab/
├── README.md
├── principles/
│   ├── design-invariants.md
│   ├── scalability-checklist.md
│   └── failure-models.md
│
├── systems/
│   ├── oauth-adapter/                    # Standalone OAuth learning module
│   │   ├── main.py
│   │   ├── docker-compose.yml
│   │   ├── Dockerfile
│   │   └── README.md
│   │
│   ├── top-k-user-aggregation/           # Top-K songs aggregation system
│   │   ├── design/
│   │   │   ├── 00_design_doc_.md
│   │   │   ├── 01_desgin_doc.md
│   │   │   ├── 02_technology_choice_doc.md
│   │   │   ├── 03_solution_doc.md
│   │   │   └── hld.png
│   │   │
│   │   └── implementation/
│   │       ├── docker-compose.yml
│   │       ├── README.md
│   │       ├── create-topics.sh
│   │       ├── init-runtime-dirs.sh
│   │       ├── clean-runtime.sh
│   │       │
│   │       ├── schemas/
│   │       │   └── cassandra/
│   │       │       ├── init.cql
│   │       │       ├── init-schema.sh
│   │       │       └── README.md
│   │       │
│   │       └── services/
│   │           ├── crawl-worker/         # Asynq job: fetch listen history → Kafka
│   │           │   ├── main.go
│   │           │   ├── tasks/crawl.go
│   │           │   ├── cmd/enqueue-test/
│   │           │   ├── Dockerfile
│   │           │   └── go.mod
│   │           │
│   │           ├── raw-event-processor/  # Kafka → Cassandra (raw events)
│   │           │   ├── main.go
│   │           │   ├── Dockerfile
│   │           │   └── go.mod
│   │           │
│   │           ├── aggregator/           # Kafka → Cassandra (counter increments)
│   │           │   ├── main.go
│   │           │   ├── Dockerfile
│   │           │   └── go.mod
│   │           │
│   │           └── api-server/           # Top-K API with Redis cache
│   │               ├── main.go
│   │               ├── Dockerfile
│   │               └── go.mod
│   │
│   ├── news-feed/                        # (planned)
│   ├── rate-limiter/                     # (planned)
│   ├── messaging-system/                 # (planned)
│   └── search-indexing/                  # (planned)
│
├── tooling/
│   ├── load-generators/
│   ├── data-generators/
│   └── observability/
│
├── templates/
│   ├── system-design-doc-template.md
│   ├── system-readme-template.md
│   └── load-test-template.md
│
└── docs/
    ├── interview-notes.md
    ├── common-tradeoffs.md
    └── design-review-checklist.md
```

## Systems Implemented

### Top-K User Aggregation
Aggregates music listening history across providers and serves Top-K songs per user.

**Architecture:**
```
enqueue-test → crawl-worker → Kafka → raw-event-processor → Cassandra (raw)
                                    → aggregator → Cassandra (counters)
                                                        ↓
                               api-server ← Redis (cache) ← GET /users/{id}/topk
```

**Stack:** Go, Kafka, Cassandra, Redis, Asynq, Docker Compose
