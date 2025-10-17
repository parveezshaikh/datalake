# Data Lake Application – High-Level Design

## 1. Purpose and Scope
Build a cloud-ready Data Lake that ingests, transforms, and distributes data across internal applications at scale. The Lake must orchestrate thousands of daily file exchanges, support diverse transformation patterns, and surface operational insights through dashboards.

## 2. Technology Stack
- **Compute & Processing**: Python, PySpark (primary), Pandas (lightweight jobs), NumPy (vectorized utilities)
- **Service Interfaces**: FastAPI (REST APIs), Django/Flask (dashboard server or embedded UI, preferably Django)
- **Storage**: Hive-backed data lake (metastore + distributed object storage), optional companion object storage for raw files
- **Containerization**: Docker images per service (ingestion workers, orchestration service, dashboard, utility jobs)
- **Messaging / Scheduling**: Airflow or managed workflow service for job orchestration; Kafka (optional) for streaming extensions
- **Observability**: Prometheus for metrics, OpenSearch/ELK for logs, Grafana for dashboards
- **Secrets & Config**: Vault or cloud secrets manager; Git-backed configuration versioning

## 3. Logical Architecture
Product processors (Mortgage, CIS, Branded Cards, Deposits, Personal Loan, Other LOBs, FFS, Adjustment, Accounting Engine) send domain extracts to the Data Lake. The Lake centralises ingestion connectors, a PySpark processing framework, shared configuration services, and Hive/object storage data layers before publishing curated data to the consumer platform, Finance and Risk reporting domains.

- **Product Processor Sources**
  - Each product processor pushes batched files or database extracts through a hardened interface (SFTP landing zones, managed file transfer, API, or CDC feed).
  - Landing policies enforce schema compatibility, data contracts, and naming conventions so mortgage, cards, deposits, and other lines of business coexist safely in the same Lake.
- **API & Orchestration Layer**
  - Pipeline Registry Service: CRUD APIs over XML pipeline definitions, validation, and versioning.
  - Configuration Service: Publishes the approved XML configuration set to connectors and Spark jobs; backs the self-service portal and Git synchronisation.
  - Job Orchestrator: Executes job pipelines, resolves dependencies, schedules runs (integrated with Airflow DAGs).
  - Self-Service Portal: Guided wizard (FastAPI backend with UI) that validates inputs, scaffolds XML, and commits changes through approval workflows.
- **Ingestion Layer**
  - **CSV Connector**: Streams files from SFTP/object storage to landing zones; schema validation using PySpark `DataFrameReader`.
  - **Database Connector**: Incremental extraction through JDBC readers or CDC integration, tuned per product processor feed.
- **Processing Layer (PySpark Cluster)**
  - **Transformation Engine**: Executes pipeline steps (deduplication, sorting, join, lookup, merge, rollup, masking) as reusable PySpark operators.
  - **Template Method Pattern**: Each pipeline inherits the base `SparkPipeline` template that manages session lifecycle, logging, and commits.
  - **Strategy Pattern**: Each transformation step is a strategy, allowing dynamic selection based on XML configuration.
- **Storage & Data Management**
  - **Data Store**: Hive-backed data lake and companion object storage hold raw and curated assets with ACID guarantees via table formats (Iceberg/Delta/Hudi).
  - **Staging Layer**: Raw/table-aligned Hive tables mirroring source schema.
  - **Standardization Layer**: Curated tables with cleansed, standardized formats.
  - **Service Layer**: Presentation tables powering downstream extracts or APIs.
  - **Metadata Store**: Hive metastore augmented by MongoDB (or Hive) tables for pipeline/job metadata & run history.
- **Delivery Layer**
  - **Consumer Zone**: Batch interfaces and APIs that feed GFTS Genesis with near-real-time snapshots sourced from the Service layer.
  - **Reporting Zone**: Finance and Risk reporting marts materialised from curated Service-layer views.
  - **Batch Exports**: PySpark writers to CSV, partitioned by business keys; output placed into application-specific service directories.
  - **Services**: FastAPI endpoints for data pulls and on-demand export requests.
- **Exception & Remediation Services**
  - **Quarantine Zone**: Automatically diverts schema failures, business-rule violations, and technical rejects into an isolated object store/Hive zone with full payload, validation errors, and correlation IDs.
  - **Remediation Workbench**: Portal workflows allow data stewards to triage, correct, and replay quarantined records via governed reprocessing APIs.
  - **Golden Record Guardrails**: Duplicate detection and survivorship policies ensure exception handling does not compromise downstream master datasets.
- **Observability & Governance**
  - Unified logging (structured JSON) flowing to ELK for source-to-service lineage, exception traces, and security events.
  - Metrics collector to Prometheus; Grafana/Superset dashboards cover throughput, latency percentiles, SLA adherence, exception queues, and capacity trends.
  - Data masking policies enforced via configuration-driven transformations with reversible tokenization where required.

### 3.1 Data Flow Diagram

```mermaid
flowchart LR
  subgraph ProductProcessors["Product Processors"]
    PP[Mortgage\nCIS\nBranded Cards\nDeposits\nPersonal Loan\nOther LOBs\nFFS\nAdjustment\nAccounting Engine]
  end

  PP --> I1

  subgraph DataHub["Data Hub"]
    I1[Ingestion Connectors]
    FW[PySpark Processing Framework]
    CF[Configuration Service\n(XML Pipelines)]
    DS[Data Store\n(Hive/Object Storage)]
  end

  FW --> DS
  CF --> FW
  I1 --> FW
  I1 --> DS

  subgraph DataLayers["Data Lake Layers"]
    L1[Staging]
    L2[Standardization]
    L3[Service]
  end

  DS --> L1
  L1 --> L2
  L2 --> L3

  subgraph Consumer["Consumer"]
    GEN[GFTS Genesis]
  end

  subgraph Reporting["Reporting"]
    FIN[Finance]
    RISK[Risk]
  end

  L3 --> GEN
  L3 --> FIN
  L3 --> RISK

  FW --> OBS[Observability & Governance]
  OBS --> Dashboards[Dashboards & Alerts]
```

**Flow Summary**
- Product processors (Mortgage, CIS, Branded Cards, Deposits, Personal Loan, Other LOBs, FFS, Adjustment, Accounting Engine) publish governed extracts that land in the ingestion connectors.
- The Data Lake applies PySpark-driven transformations orchestrated by XML-defined pipelines while the configuration service ensures all jobs use approved metadata.
- Staging, Standardization, and Service layers persist the refined datasets that supply the GFTS Genesis consumer platform plus Finance and Risk reporting domains.
- Observability services capture metrics, logs, and lineage across ingestion, processing, and delivery stages to drive dashboards and proactive alerting.

## 4. Pipeline Configuration Model
### 4.1 XML Pipeline Definition
Each data pipeline is defined in XML, stored per application and layer. Schema highlights:

```xml
<pipeline id="customer_staging_load" version="1.0" layer="staging">
  <metadata>
    <appName>customer-team</appName>
    <sla>PT2H</sla>
    <schedule>0 * * * *</schedule>
  </metadata>
  <sources>
    <csv id="customer_csv" path="s3://landing/customer/*.csv" header="true" delimiter="," inferSchema="false" loadPolicy="onDemand">
      <schemaRef>schemas/customer_staging.avsc</schemaRef>
    </csv>
    <database id="customer_db" jdbcUrl="jdbc:mysql://host/db" fetchSize="10000" loadPolicy="onDemand" schemaRef="schemas/customer_db.json">
      <sql><![CDATA[
        SELECT customer_id, status, updated_at
        FROM customer
        WHERE updated_at >= :runDate
      ]]></sql>
      <!-- Omit <sql> and provide <table name="..."/> for simple full-table loads -->
    </database>
  </sources>
  <transformations>
    <deduplicate source="customer_csv" keys="customer_id" keep="latest"/>
    <sort source="customer_csv" orderBy="customer_id"/>
    <join left="customer_csv" right="customer_db" type="left" condition="customer_csv.customer_id = customer_db.id"/>
    <lookup source="customer_csv" reference="country_dim" outputFields="country_name"/>
    <mask source="customer_csv" columns="ssn,email" strategy="tokenize"/>
    <aggregate source="customer_csv" groupBy="country_name" metrics="count:customers,sum:revenue"/>
  </transformations>
  <targets>
    <hive table="staging.customer_snapshot" mode="append" partitionBy="ingest_date" schemaRef="schemas/staging/customer_snapshot.avsc"/>
    <csv path="service/customer/export/%Y/%m/%d" mode="overwrite" compression="gzip"/>
    <database id="crm_service" jdbcUrl="jdbc:sqlserver://crm-host/db" mode="merge" writeBatchSize="5000" schemaRef="schemas/service/crm_export.json">
      <table name="dbo.customer_service_snapshot"/>
    </database>
  </targets>
</pipeline>
```

Key design considerations:
- **XSD Validation**: XSD enforces transformation ordering rules and optional/required attributes.
- **Reusable References**: `<schemaRef>` on files, databases, and targets reference the schema artifacts housed under application-level `config/.../schemas`.
- **Flexible Database Reads/Writes**: `<database>` sources may specify inline `<sql>` (with parameter binding) or fall back to `<table name="..."/>` for full-table ingestion. Targets can write to database tables using `mode` semantics consistent with Spark JDBC writers.
- **Extensibility**: New transformation nodes can be plugged in without altering orchestrator code (Strategy pattern).
- **Exception Policies**: Pipelines declare `<errorPolicy>` blocks defining reject handling (`quarantine`, `skip`, `halt`, `retry`) and default remediation SLA so the orchestrator can route failed records to the exception zone automatically.

#### Element Attribute Reference
- `pipeline`
  - `id`: unique string scoped to the application.
  - `version`: semantic version string (`major.minor.patch`).
  - `layer`: `staging | standardization | service`.
  - `<errorPolicy>`: optional block specifying `onRecordError` (`quarantine | skip`), `onStepError` (`retry | halt`), `quarantineTopic`, and `remediationSla`.
- `metadata`
  - `<appName>`: application identifier used for tagging lineage, billing, and rule-scoping.
  - `<sla>`: ISO-8601 duration (`PT2H`, `P1D`).
  - `<schedule>`: cron expression (`minute hour day-of-month month day-of-week`).
- `csv` (source/target)
  - `id`: reference name used in transformations (sources only).
  - `path`: object storage or filesystem URI with optional wildcards.
  - `header`: `true | false`.
  - `delimiter`: single-character delimiter (`,`, `|`, `\t`, etc.).
  - `inferSchema`: `true | false` (sources).
  - `compression`: `gzip | bzip2 | snappy | none` (targets optional).
  - `mode`: `append | overwrite | ignore | error` (targets).
  - `loadPolicy`: `eager | onDemand` (sources; controls when the engine materialises the dataset).
  - `<schemaRef>`: relative path under the layer-specific `schemas/` directory.
- `database` (source/target)
  - `id`: reference name (sources) or logical sink identifier (targets).
  - `jdbcUrl`: full JDBC connection string.
  - `userSecretRef`: name of credentials secret (optional).
  - `fetchSize` / `writeBatchSize`: positive integer tuning batched reads/writes.
  - `loadPolicy`: `eager | onDemand` (sources).
  - `mode`: `append | overwrite | merge | truncateInsert` (targets).
  - `isolationLevel`: `READ_COMMITTED | READ_UNCOMMITTED | REPEATABLE_READ | SERIALIZABLE` (optional).
  - `<sql>`: custom query body wrapped in CDATA (sources) supporting named parameters (`:runDate`).
  - `<table name="schema.table_name"/>`: alternative to `<sql>` for table-level operations.
  - `<schemaRef>`: relative schema definition path under the appropriate layer.
- `hive` (target)
  - `table`: fully qualified Hive table (e.g., `staging.customer_snapshot`).
  - `mode`: `append | overwrite | merge`.
  - `partitionBy`: comma-separated list of partition columns.
  - `format`: optional storage format override (`parquet | orc | delta`).
  - `<schemaRef>`: schema definition path used for enforcement/validation.
- `deduplicate`
  - `source`: source dataset id.
  - `keys`: comma-separated business keys.
  - `keep`: `first | last | latest`.
- `sort`
  - `source`: dataset id to sort.
  - `orderBy`: comma-separated columns.
  - `direction`: `asc | desc` (optional; `asc` default).
  - `nulls`: `first | last` (optional).
- `join`
  - `left`: left dataset id.
  - `right`: right dataset id.
  - `type`: `inner | left | right | full | semi | anti`.
  - `condition`: SQL-like expression referencing datasets.
- `lookup`
  - `source`: dataset id to enrich.
  - `reference`: lookup table id (resolves via metadata catalog).
  - `outputFields`: comma-separated columns to append.
  - `joinType`: `left | inner` (optional; `left` default).
- `mask`
  - `source`: dataset id to mask.
  - `columns`: comma-separated sensitive column names.
  - `strategy`: `tokenize | hash | redact | encrypt`.
  - `secretRef`: key management reference for reversible strategies (optional).
- `aggregate`
  - `source`: dataset id.
  - `groupBy`: comma-separated grouping columns.
  - `metrics`: comma-separated aggregator definitions (`count:alias`, `sum:revenue`, `avg:balance`, `max:updated_at`, `min:created_at`).
  - `window`: optional window specification (`daily`, `weekly`, cron-like) for rolling aggregates.

### 4.2 Job Pipeline Definition
Job pipelines chain multiple data pipelines with dependencies and failure policies.

```xml
<jobPipeline id="customer_master" version="2.0">
  <pipelines>
    <pipelineRef id="customer_staging_load" onFailure="retry" maxRetries="2"/>
    <pipelineRef id="customer_standardization" dependsOn="customer_staging_load" onFailure="halt"/>
    <pipelineRef id="customer_service_export" dependsOn="customer_standardization" onFailure="continue"/>
  </pipelines>
  <notifications>
    <email to="ops@company.com" on="failure"/>
    <webhook url="https://hooks.slack.com/services/..." on="completion"/>
  </notifications>
</jobPipeline>
```

- **Dependency Graph**: Converted to DAG (executed via Airflow) ensuring no cycles.
- **Failure Handling Strategies**: `retry`, `halt`, `skip`, `continue`, optionally `fallback`.
- **Parameterization**: Job-level variables (e.g., run-date) injected into pipelines.

## 5. Repository & Folder Structure

```
datalake/
├── docker/
│   ├── orchestration.Dockerfile
│   ├── pipeline-runner.Dockerfile
│   └── dashboard.Dockerfile
├── infra/
│   ├── terraform/                # Infrastructure as code (Spark cluster, storage, Kafka, etc.)
│   └── helm/                     # Kubernetes deployment charts
├── services/
│   ├── orchestrator/             # FastAPI orchestration service
│   ├── dashboard/                # Flask UI for operational metrics
│   ├── self_service/             # Portal backend and UI assets for guided pipeline creation
│   ├── remediation/              # Exception triage APIs, steward workflows, replay automation
│   └── workers/                  # PySpark job launcher and utility binaries
├── libs/
│   ├── pipeline_core/            # Base Spark pipeline template, step registry, XML parser
│   ├── connectors/               # CSV, JDBC, and future source/sink connectors
│   ├── transformations/
│       ├── deduplicate/
│       │   └── deduplicate.py
│       ├── sort/
│       │   └── sort.py
│       ├── join/
│       │   └── join.py
│       ├── lookup/
│       │   └── lookup.py
│       ├── merge/
│       │   └── merge.py
│       ├── aggregate/
│       │   └── aggregate.py
│       └── mask/
│           └── mask.py
│   └── data_quality/
│       ├── rules_engine.py       # Reusable validation rules and expectation library adapters
│       └── quarantine.py         # Helpers for routing bad records to the exception zone
├── config/
│   ├── common/
│   │   ├── schemas/              # Shared schema definitions (Avro, JSON schema)
│   │   └── lookups/              # Shared lookup tables or references
│   └── applications/
│       └── <appName>/
│           ├── staging/
│           │   ├── pipelines/    # XML pipeline definitions for staging
│           │   ├── jobs/         # XML job pipeline bundles for staging runs
│           │   ├── schemas/      # Layer-specific schemas referenced by staging pipelines
│           │   └── resources/    # Source-specific configs (ingestion endpoints, credentials references)
│           ├── standardization/
│           │   ├── pipelines/
│           │   ├── jobs/
│           │   ├── schemas/
│           │   └── resources/
│           ├── service/
│               ├── pipelines/
│               ├── jobs/
│               ├── schemas/
│               └── resources/
│           └── exceptions/
│               ├── rules/        # Data quality and business rule definitions
│               └── workflows/    # Steward remediation playbooks and escalation configs
├── data/
│   └── applications/
│       └── <appName>/
│           ├── staging/          # Source ingests for the app (raw tables/files)
│           ├── standardization/  # Refined, standardized datasets
│           ├── service/          # Published outputs ready for distribution
│           └── exceptions/       # Quarantined records with error context and audit trail
├── scripts/                      # CLI utilities (config validation, job triggers)
├── docs/
│   └── high-level-design.md
└── tests/
    ├── unit/
    └── integration/
```

**Configuration Segregation**
- Application-specific directories encapsulate input/output/pipeline configuration per app.
- Layer subfolders enforce lifecycle separation: staging (ingestion), standardization (transform), service (exports).
- Each layer houses a dedicated `schemas/` directory so XML definitions can resolve to the correct physical schema artifacts without cross-layer coupling.
- Exceptions configuration stores rule definitions and remediation workflows so operational policies remain versioned with the owning application.
- Data lake outputs persist under `data/applications/<appName>/...`, keeping operational artifacts alongside their owning configuration for lifecycle management. Quarantined data is stored beside production layers with access controls and retention settings.
- Git branching strategy and tagging manage configuration versions for deployments.

## 6. Scalability & Performance Strategies
- **Distributed Execution**: PySpark on a Kubernetes-backed cluster or managed Spark service; auto-scaling worker nodes based on queue depth.
- **Partitioning & Pruning**: Partition Hive tables by frequently queried columns (e.g., ingest_date) to minimize scan cost.
- **Caching & Broadcast Join**: Use Spark caching for repeated datasets; broadcast dimension tables for faster joins.
- **Vectorized Operations**: Use Pandas/NumPy only for small data or control-plane logic; delegate heavy lifting to Spark.
- **Asynchronous I/O**: Parallel ingestion from multiple file sources; multi-threaded download/upload using asyncio or Spark parallelism.
- **Containerized Isolation**: Separate Docker images per service; orchestrate with Kubernetes for horizontal scaling.
- **Workload Management**: Implement Spark dynamic allocation, YARN/Kubernetes queue quotas, and Airflow pool limits to isolate heavy product processors from latency-sensitive SLA jobs.
- **Tiered Storage**: Use storage classes aligned with data temperature; compact service-layer tables with OPTIMIZE/VACUUM jobs to sustain interactive performance at scale.
- **Template & Strategy Patterns**: Ensure new transformations plug in without code duplication; maintain performance-tuned implementations.

## 7. Operational Monitoring, Exception Handling & Run History
- **Run Metadata Store**: Each pipeline run emits start/end timestamps, row counts, success/failure flags, error reasons, SLA status, cluster utilisation, and cost-estimate tags into a `ops.pipeline_runs` Hive/Iceberg table.
- **Metrics Aggregation**: Airflow DAGs push run summaries, queue depth, executor utilisation, and checkpoint timings to Prometheus (e.g., `pipeline_runs_total`, `pipeline_failures_total`, `pipeline_duration_seconds`, `spark_executor_cpu_percent`).
- **Exception Telemetry**: Quarantine events publish to an `ops.quarantine_events` topic/table capturing validation rule IDs, impacted product processor, record counts, and remediation status. Self-service APIs support bulk replay once corrections are approved.
- **Operational Dashboards**: Grafana and/or Superset boards federate Prometheus metrics, Hive operational tables, and ELK logs to provide:
  - Pipeline throughput, latency percentiles, SLA adherence, and backlog visualisations.
  - Exception queues by severity, days outstanding, steward assignment, and reprocessing outcome.
  - Data quality scorecards summarising rule pass rates, drift indicators, and anomaly trends per product processor.
  - Cost and capacity heatmaps (compute hours, storage growth, object operations) to forecast scaling needs.
- **Alerting & Runbooks**: Dynamic thresholds trigger Slack/email/webhook alerts with embedded runbook links. PagerDuty/ITSM integrations auto-create incidents for priority failures.
- **Traceability & Correlation IDs**: Unique run IDs and record-level correlation hashes flow through logs, metrics, lineage catalog, and quarantined payloads to enable cross-platform troubleshooting.

## 8. Security, Governance, and Compliance
- **Data Masking & Encryption**: Tokenization, hashing, field-level encryption, and deterministic masking strategies defined per column in XML; enforced by Spark transformations and by-at-rest encryption (KMS-managed keys) for raw and curated zones.
- **Access Control**: Fine-grained ABAC/RBAC across product processors and data layers; service principals use least privilege when interacting with data lake, secrets, and metadata APIs. Integration with enterprise IdP enables SSO and MFA for portal access.
- **Audit Logging & Lineage**: Immutable audit trail captures every configuration change, portal action, data access, schema update, and remediation replay with user identity, timestamp, and before/after diff. Lineage catalog (e.g., OpenLineage/Marquez) links pipelines, datasets, and downstream consumers.
- **Compliance & Retention**: Configurable data retention policies manage lifecycle (hot → warm → archive → purge) and legal hold requirements per product processor. Automated retention jobs enforce GDPR/CCPA deletion and produce attestation reports.
- **Schema Governance & Data Quality**: Schema registry ensures producers/consumers adhere to contracts; automatic schema drift detection triggers quarantine plus steward notifications. Embedded data quality rule packs run during ingestion with policy-based blocking or warning thresholds.

## 9. Deployment & DevOps
- **CI/CD**: Automated linting, unit tests, XML schema validation, and integration tests on pull requests.
- **Artifact Management**: Docker images published to registry with semantic tags; Spark jobs packaged as wheel files or zip bundles.
- **Environment Promotion**: Dev → QA → Prod using configuration overlays; Airflow DAGs parameterized by environment.
- **Disaster Recovery**: Metadata and Hive tables replicated across regions; raw files retained in immutable storage with lifecycle policies.

## 10. Future Enhancements
- Real-time ingestion module via Spark Structured Streaming and Kafka.
- Machine-learning powered anomaly detection on pipeline metrics for proactive alerting.
- Data quality rules engine integrated with transformations (e.g., Great Expectations).

For a complete log of prompts and responses influencing this document, refer to `docs/prompts-of-high-level-design.md`.
