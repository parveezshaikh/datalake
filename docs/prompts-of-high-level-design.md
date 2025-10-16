# Prompts for High-Level Design

This log captures every recorded prompt, the corresponding response summary, and any follow-up actions related to `docs/high-level-design.md`. Append new sessions here as the design evolves.

## Session 000 - Initial Design
**Prompt/Input**  
To create / develop "Data Hub Application"

**Requirement**  
As a solution architect, provide a high-level design for a data-processing application.

**Features**
- The application should provide a data pipeline component where the data processing steps can be configured in XML.
- The application should provide a job pipeline component where multiple data pipeline components can be sequenced with dependencies.
- Production usage must handle thousands of file exchanges across applications; keep configuration, inputs, outputs, and data per application together. Segregate configurations into staging (ingest), standardization (transform), and service (distribution) layers.
- Prioritize scalability and performance with appropriate design patterns and libraries.
- Record every pipeline run and expose operational dashboards showing daily runs, success and failure counts, and failure reasons.

**Transformation Supported**
- Input from CSV files
- Input from database tables
- Deduplication and Sorting
- Sorting
- Join
- Lookup
- Merging
- Rollup or Aggregation
- Output to CSV files
- Data Masking

**Technology Stack**
- Hive or another NoSQL store
- Python
- Docker
- PySpark
- NumPy
- Pandas
- Flask
- FastAPI

**Repository**  
Create a new repository called `datalake` in `github.com/parveezshaikh/`.

**Prompt/Output**
- Authored a full high-level design for the Data Hub, detailing roles for orchestration, ingestion, processing, storage, delivery, and observability layers with the requested tech stack in docs/high-level-design.md:3-37.
- Captured XML-based pipeline and job-pipeline configuration models supporting all required transformations, dependency handling, and operational policies in docs/high-level-design.md:39-110.
- Proposed the segregated repository layout that groups staging, standardization, and service configurations per application alongside shared code and infrastructure assets in docs/high-level-design.md:111-149.
- Documented scalability/performance strategies, run-history telemetry, dashboard metrics, and alerting expectations in docs/high-level-design.md:150-169.
- Added security, governance, deployment workflow, and future enhancement considerations in docs/high-level-design.md:170-187.
- No tests were run (documentation-only change). The GitHub repository `parveezshaikh/datalake` was not created; manual action required.

**Next Steps**
- Initialize the GitHub repository and push the scaffold.
- Validate the design with platform and security stakeholders before implementation.

## Session 001 - Design Follow-up
**Prompt/Input**  
Refer the high-level-design.md file; the following questions target the XML Pipeline Definition section:

1. The structure proposes three stages—reading sources, applying transformations, creating outputs. Is execution strictly in the listed sequence? Example:
   ```xml
   <sources>
     <csv id="customer_csv" path="s3://landing/customer/*.csv" header="true" delimiter="," inferSchema="false">
       <schemaRef>schemas/customer_staging.avsc</schemaRef>
     </csv>
     <database id="customer_db" jdbcUrl="jdbc:mysql://host/db" table="customer" fetchSize="10000"/>
   </sources>
   ```
   Will this load the CSV then the database?
   ```xml
   <transformations>
     <deduplicate source="customer_csv" keys="customer_id" keep="latest"/>
     <sort source="customer_csv" orderBy="customer_id"/>
   </transformations>
   ```
   Will this run deduplication before sorting? Same question applies to `<targets>`.
2. Can the transformations stage be blank to allow straight-through passes?
3. Does loading all sources together create memory pressure, and would JIT loading be better?
4. Should `<database>` support SQL instead of just table names to avoid default `SELECT *`?

**Prompt/Output**
- Q1 - Execution order: The XML lists sources, transforms, and targets declaratively. The orchestrator compiles the configuration into a DAG to optimize loads (e.g., JIT or parallel) rather than executing strictly top-to-bottom. For simple cases the default run order follows the listing, but dependencies such as a source referenced in a transform drive sequencing—no change needed, just note that the engine controls evaluation (docs/high-level-design.md:44).
- Q2 - No-transform pipelines: Support straight-through passes by allowing `<transformations/>` to be empty or omitted. The schema should treat transformations as optional so extract-only or publish-only jobs remain valid (docs/high-level-design.md:56).
- Q3 - Memory considerations: Spark loads lazily; even if multiple sources are declared, the engine can stream or batch them. Add a `loadPolicy` attribute (e.g., eager vs onDemand) or source-level partition filters so large tables are fetched only when referenced, avoiding memory spikes (docs/high-level-design.md:50).
- Q4 - Database reads: Let pipelines provide SQL via an optional `<sql><![CDATA[...]]></sql>` child or `queryRef` so they are not forced into `SELECT *`. Fall back to `table` when SQL is not supplied to keep flexibility for projections, filters, joins, and CDC views (docs/high-level-design.md:54).

## Session 002 - Database Source Revision
**Prompt/Input**  
Can you change the design document per the response to Q4?

**Prompt/Output**
- Updated the database source example to show inline SQL support and documented the table-versus-SQL option so pipelines can project/filter without defaulting to `SELECT *` (docs/high-level-design.md:54-82).

## Session 003 - Repository Feedback and Enhancements
**Prompt/Input**  
Refer to the Repository & Folder Structure section:

1. Align the `data` subfolders with the `applications` hierarchy so `data/applications/<appName>/staging`, `standardization`, and `service` hold the actual files.
2. Create a dedicated folder and Python file per transformation under `transformations`.

Additionally, include the previously suggested self-service portal for business users to request new pipelines via guided wizards in the high-level design.

**Prompt/Output**
- Expanded the API and orchestration layer to include a self-service portal that validates inputs, scaffolds XML, and routes approvals (docs/high-level-design.md:16-20) and added a corresponding services/self_service component in the repository layout (docs/high-level-design.md:121-138).
- Broke out each transformation into its own subpackage with dedicated Python module under `libs/transformations/` to match the requested structure (docs/high-level-design.md:121-138).
- Realigned the data directory so application artifacts land under `data/applications/<appName>/staging|standardization|service` and documented the governance benefits (docs/high-level-design.md:157-175).
- Removed the portal from Future Enhancements, keeping that list focused on longer-term ideas now that the portal is part of the core design (docs/high-level-design.md:209-213).

**Next Steps**
- Review the portal concept with UX and governance stakeholders to refine the approval workflow.
- Update implementation backlogs or tickets to reflect the adjusted folder structure and transformation packaging.
