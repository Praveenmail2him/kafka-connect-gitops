# Enterprise Architecture Proposal: Scalable Kafka Connect Integration Platform

**Document Status:** Draft for Architecture Review Board (ARB)  
**Author:** Praveen Kumar KS

**Target Platform:** OpenShift / Kubernetes (Strimzi Operator) & Apache Kafka®  
**Scope:** Multi-Tenant Data Ingestion, Change Data Capture (CDC), and Downstream Egress Charter  

---

## Table of Contents
1. [Executive Summary](#1-executive-summary)
2. [Conceptual & Logical Architecture](#2-conceptual--logical-architecture)
3. [Cluster Topology & Workload Isolation Strategy](#3-cluster-topology--workload-isolation-strategy)
4. [Reliability, Delivery Semantics & Resilience Patterns](#4-reliability-delivery-semantics--resilience-patterns)
5. [Security & Governance Model](#5-security--governance-model)
6. [Reference Kubernetes Blueprint (Strimzi CRDs)](#6-reference-kubernetes-blueprint-strimzi-crds)
7. [GitOps Operating Model & Observability](#7-gitops-operating-model--observability)

---

## 1. Executive Summary

This proposal defines the reference architecture, cluster isolation topology, security governance, and GitOps operational model for an enterprise **Kafka Connect Platform**.

Rather than operating Kafka Connect as ad-hoc, monolithic worker pools, this charter establishes a **multi-tenant, declarative data integration tier**. By decoupling Connect execution from core Kafka brokers, isolating workloads by failure domain, standardizing wire formats via Schema Registry, and enforcing GitOps-driven deployments via the Strimzi Kubernetes Operator, the platform enables engineering teams to onboard high-throughput streaming pipelines safely with minimal operational overhead.

### Key Architectural Outcomes
* **Zero "Noisy Neighbor" Contention:** Workload-tiered Connect clusters prevent heavy batch sinks or experimental plugins from stalling low-latency database Change Data Capture (CDC).
* **Zero-Downtime Scaling & Upgrades:** Enforcement of Incremental Cooperative Rebalancing (`connect.protocol=compatible`) eliminates stop-the-world task pauses during rolling restarts.
* **Zero Plaintext Secrets:** All credentials are dynamically injected at runtime via HashiCorp Vault and Kafka Connect's `DirectoryConfigProvider`.
* **Self-Service with Guardrails:** Teams manage connectors declaratively as Kubernetes Custom Resources (`KafkaConnector`) validated by automated CI policy gates and synced via Argo CD.

---

## 2. Conceptual & Logical Architecture

The platform operates exclusively in **Distributed Mode** on Kubernetes, treating worker pods as stateless compute engines backed by compacted internal Kafka topics for coordination and state persistence.

```text
[External Sources]          [Kafka Connect Cluster (Distributed)]           [Kafka Core]             [External Sinks]
  (DBs / CDC / Logs)                                                                                  (Elastic / S3 / Warehouse)
         │                                                                                                     ▲
         ▼                  ┌───────────────────────────────────┐                                              │
  ┌─────────────┐           │  Worker 1   Worker 2   Worker N   │                                              │
  │ Source Sys  │──────────▶│ ┌─────────┐ ┌─────────┐           │           ┌──────────────┐                   │
  └─────────────┘           │ │Source   │ │Sink     │  (Tasks)  │──────────▶│ Data Topics  │───────────────────┤
                            │ │Task(s)  │ │Task(s)  │           │           └──────────────┘                   │
                            │ └────┬────┘ └────▲────┘           │                                              │
                            │      │           │                │           ┌──────────────┐                   │
                            │   [SMTs & Converters]             │◀─────────▶│Internal State│                   │
                            └──────┼───────────┼────────────────┘           │(Config/Offset│                   │
                                   │           │                            │ /Status)     │                   │
                                   ▼           ▼                            └──────────────┘                   │
                            ┌───────────────────────────────────┐                                              │
                            │     Schema Registry & DLQ         │──────────────────────────────────────────────┘
                            └───────────────────────────────────┘
```

### Core Architectural Layers
1. **Execution Layer (Workers & Tasks):** Stateless JVM worker pods managed by Strimzi. A **Connector** defines the logical integration job; **Tasks** are the parallel execution units distributed across workers.
2. **State & Coordination Layer:** Workers persist zero local state. Coordination, offsets, and task configurations are stored in three dedicated, compacted internal Kafka topics (`*-configs`, `*-offsets`, `*-status`) with `replication.factor=3` and `min.insync.replicas=2`.
3. **In-Flight Transformation & Serialization Layer:**
   * **Single Message Transforms (SMTs):** Lightweight, stateless record mutations (PII masking, field routing, timestamp normalization) executed in-memory before serialization.
   * **Converters & Schema Registry:** Decouples connectors from wire payloads. All production pipelines standardize on **Avro** or **Protobuf** with `BACKWARD` or `FULL` compatibility enforced at the Schema Registry.

---

## 3. Cluster Topology & Workload Isolation Strategy

Running all connectors inside a single shared cluster creates severe blast-radius risks (e.g., an S3 sink buffering large micro-batches causing a JVM `OutOfMemoryError` that drops a critical database CDC task). The platform mandates **four isolated cluster tiers**:

| Cluster Tier | Target Workloads | Compute & JVM Profile | Isolation Rationale |
| :--- | :--- | :--- | :--- |
| **Tier 1: Critical CDC** | Database CDC (Debezium for PostgreSQL, Oracle, and SQL Server) | High Memory (6–8 GiB), Low GC Pause (`G1GC`), Guaranteed QoS | Prevents binlog/WAL read stalls that cause source database transaction log bloat and disk exhaustion. |
| **Tier 2: High-Throughput Sinks** | Object Storage (S3, HDFS), Data Warehouses | High Network I/O, Large Heap (8–12 GiB) | Isolates heavy micro-batch buffering, Parquet/ORC compression, and large flush payloads from real-time streams. |
| **Tier 3: Operational & Search** | Elasticsearch, OpenSearch, Redis, REST Sinks | Moderate CPU/Network, High Task Concurrency | Shields core ingestion streams from downstream HTTP rate limits, indexing backpressure, and transient API timeouts. |
| **Tier 4: Sandbox & Staging** | Dev/QA, Unverified or Custom Community Plugins | Minimal / Horizontal Pod Autoscaled | Contains classloader leaks, malformed schemas, and experimental connectors away from production SLAs. |

---

## 4. Reliability, Delivery Semantics & Resilience Patterns

To guarantee data integrity across heterogeneous systems, every connector onboarded to the platform must adhere to the following resilience standards:

* **Delivery Guarantees & Idempotency:**
  * **Sources:** Log-based CDC connectors capture committed database transactions with **at-least-once** delivery (or Exactly-Once Source Support via KIP-618 where supported).
  * **Sinks:** All sink connectors must enforce **idempotent writes** using deterministic primary keys (`pk.mode=record_key`) and `upsert` write modes so offset replays following a rebalance never produce duplicate records.
* **Dead Letter Queues (DLQ) & Poison Pill Isolation:**
  * Sink connectors must configure `errors.tolerance=all` paired with a dedicated DLQ topic (`dlq.<connector-name>`) and `errors.deadletterqueue.context.headers.enable=true`.
  * Malformed records or schema violations are routed to the DLQ with diagnostic exception headers rather than halting the entire pipeline.
  * `errors.log.include.messages` must remain `false` in production to prevent sensitive payload leakage into centralized log aggregators.
* **Incremental Cooperative Rebalancing:**
  * All clusters enforce `connect.protocol=compatible`. During pod scaling or rolling upgrades, only the specific tasks assigned to departing/arriving workers are paused and reassigned, keeping all other tasks actively streaming.

---

## 5. Security & Governance Model

1. **Immutable Plugin Packaging:**
   * Connector JARs are never downloaded dynamically at runtime. Platform CI builds immutable, signed container images per cluster tier, packaging exact, vulnerability-scanned plugin versions into isolated subdirectories under `/opt/kafka/plugins/`.
2. **Dynamic Secrets Externalization:**
   * Plaintext credentials in Git or CRD manifests are strictly prohibited. Credentials are stored in **HashiCorp Vault**, mounted into worker pods as ephemeral volumes, and resolved at runtime via Kafka Connect's `DirectoryConfigProvider` (`${directory:/opt/kafka/external-configuration/<secret>:<key>}`).
3. **API Lockdown & Least-Privilege ACLs:**
   * Direct developer access to the Connect REST API (`port 8083`) is blocked via `NetworkPolicy` resources; all configuration mutations flow exclusively through the Strimzi Operator.
   * Each Connect cluster authenticates to Kafka using dedicated **mTLS** or **SASL/SCRAM** service principals scoped via Kafka ACLs strictly to its authorized source/sink topics and internal state topics.

---

## 6. Reference Kubernetes Blueprint (Strimzi CRDs)

### 6.1 Immutable Worker Image (`Dockerfile`)

```dockerfile
FROM quay.io/strimzi/kafka:0.43.0-kafka-3.8.0

USER root:root

# Isolate plugin classloaders into dedicated directories
RUN mkdir -p /opt/kafka/plugins/debezium-postgres \
             /opt/kafka/plugins/s3-sink \
             /opt/kafka/plugins/jdbc-sink

COPY ./plugins/debezium-connector-postgres-2.7.3.Final/ /opt/kafka/plugins/debezium-postgres/
COPY ./plugins/kafka-connect-s3-10.5.13/ /opt/kafka/plugins/s3-sink/
COPY ./plugins/kafka-connect-jdbc-10.7.6/ /opt/kafka/plugins/jdbc-sink/

# Enforce non-root permissions for OpenShift restricted-v2 SCC compatibility
RUN chown -R 1001:0 /opt/kafka/plugins && chmod -R g+rwX /opt/kafka/plugins

USER 1001
```

### 6.2 Tier-1 CDC Cluster Definition (`KafkaConnect` CRD)

```yaml
apiVersion: kafka.strimzi.io/v1beta2
kind: KafkaConnect
metadata:
  name: connect-tier1-cdc
  namespace: kafka-connect-prod
  annotations:
    strimzi.io/use-connector-resources: "true"
spec:
  version: 3.8.0
  replicas: 3
  image: registry.internal.corp/platform/kafka-connect-cdc:3.8.0-r4
  bootstrapServers: prod-kafka-bootstrap.kafka-core.svc:9093
  tls:
    trustedCertificates:
      - secretName: prod-kafka-cluster-ca-cert
        certificate: ca.crt
  authentication:
    type: tls
    certificateAndKey:
      secretName: connect-tier1-cdc-mtls
      certificate: user.crt
      key: user.key
  config:
    group.id: connect-tier1-cdc-group
    offset.storage.topic: connect-tier1-cdc-offsets
    config.storage.topic: connect-tier1-cdc-configs
    status.storage.topic: connect-tier1-cdc-status
    config.storage.replication.factor: 3
    offset.storage.replication.factor: 3
    status.storage.replication.factor: 3
    connect.protocol: compatible
    key.converter: io.confluent.connect.avro.AvroConverter
    value.converter: io.confluent.connect.avro.AvroConverter
    key.converter.schema.registry.url: https://schema-registry.kafka-core.svc:8081
    value.converter.schema.registry.url: https://schema-registry.kafka-core.svc:8081
    key.converter.schemas.enable: true
    value.converter.schemas.enable: true
    config.providers: directory
    config.providers.directory.class: org.apache.kafka.common.config.provider.DirectoryConfigProvider
  resources:
    requests:
      cpu: "2"
      memory: 6Gi
    limits:
      cpu: "4"
      memory: 8Gi
  jvmOptions:
    -Xms: 4096m
    -Xmx: 4096m
    -XX:
      UseG1GC: "true"
      MaxGCPauseMillis: "20"
      InitiatingHeapOccupancyPercent: "35"
  externalConfiguration:
    volumes:
      - name: source-db-secrets
        secret:
          secretName: vault-cdc-source-credentials
  metricsConfig:
    type: jmxPrometheusExporter
    valueFrom:
      configMapKeyRef:
        name: connect-metrics-config
        key: metrics-config.yml
  template:
    pod:
      topologySpreadConstraints:
        - maxSkew: 1
          topologyKey: topology.kubernetes.io/zone
          whenUnsatisfiable: DoNotSchedule
          labelSelector:
            matchLabels:
              strimzi.io/cluster: connect-tier1-cdc
      affinity:
        podAntiAffinity:
          requiredDuringSchedulingIgnoredDuringExecution:
            - labelSelector:
                matchLabels:
                  strimzi.io/cluster: connect-tier1-cdc
              topologyKey: kubernetes.io/hostname
```

### 6.3 Declarative Connector Definition (`KafkaConnector` CRD)

```yaml
apiVersion: kafka.strimzi.io/v1beta2
kind: KafkaConnector
metadata:
  name: orders-cdc
  namespace: kafka-connect-prod
  labels:
    strimzi.io/cluster: connect-tier1-cdc
spec:
  class: io.debezium.connector.postgresql.PostgresConnector
  tasksMax: 1
  autoRestart:
    enabled: true
    maxRestarts: 5
  config:
    database.hostname: orders-db.example.com
    database.port: "5432"
    database.dbname: orders
    database.user: "${directory:/opt/kafka/external-configuration/source-db-secrets:username}"
    database.password: "${directory:/opt/kafka/external-configuration/source-db-secrets:password}"
    topic.prefix: orders.cdc
    table.include.list: "public.orders,public.order_items"
    schema.history.internal.kafka.bootstrap.servers: prod-kafka-bootstrap.kafka-core.svc:9093
    schema.history.internal.kafka.topic: schema-changes.orders
    errors.tolerance: all
    errors.deadletterqueue.topic.name: dlq.orders-cdc
    errors.deadletterqueue.topic.replication.factor: 3
    errors.deadletterqueue.context.headers.enable: true
    errors.log.enable: true
    errors.log.include.messages: false
    transforms: maskPII
    transforms.maskPII.type: org.apache.kafka.connect.transforms.MaskField$Value
    transforms.maskPII.fields: "CUSTOMER_SSN,CREDIT_CARD_NUM"
    transforms.maskPII.replacement: "****"
```

---

## 7. GitOps Operating Model & Observability

### 7.1 Repository Layout & Ownership

```text
kafka-connect-gitops/
├── platform/                         # Owned by Platform / SRE Team
│   ├── base/
│   │   ├── connect-tier1-cdc.yaml
│   │   ├── connect-tier2-sinks.yaml
│   │   └── metrics-configmap.yaml
│   └── overlays/
│       ├── stage/
│       └── prod/
└── connectors/                       # Owned by Domain Teams (CODEOWNERS enforced)
    ├── tier1-cdc/
    │   └── orders-cdc.yaml
    └── tier2-sinks/
        └── s3-audit-archive-sink.yaml
```

### 7.2 CI/CD Validation & Reconciliation Flow

```text
[Developer PR] ──▶ [CI Validation Gate] ──▶ [Merge to main] ──▶ [Argo CD Sync] ──▶ [Strimzi Operator]
                        │                                                                 │
                        ├─ 1. kubeconform / CRD schema lint                               ├─ Reconciles KafkaConnector CRD
                        ├─ 2. Secret leak scan (gitleaks)                                 ├─ Calls internal Connect REST API
                        ├─ 3. Dry-run PUT /connector-plugins/{class}/config/validate      └─ Triggers Cooperative Rebalance
                        └─ 4. Policy check (OPA/Kyverno guardrails)
```

1. **Automated Pull Request Gates:** Every PR executes schema validation (`kubeconform`), secret scanning (`gitleaks`), OPA/Kyverno policy checks (enforcing DLQ settings, `tasksMax` caps, and `errors.log.include.messages: false`), and a dry-run configuration validation against a staging Connect worker's `/config/validate` REST endpoint.
2. **Declarative Reconciliation:** Upon merge to `main`, Argo CD syncs the `KafkaConnector` manifest into Kubernetes, and the Strimzi Operator reconciles the connector state via the internal Connect REST API.

### 7.3 Production Observability & Golden Signals
All worker pods expose JMX metrics scraped by Prometheus and routed to centralized dashboards and alerting:

| Signal Category | Key JMX Metric | Alert Threshold / Action |
| :--- | :--- | :--- |
| **Task Availability** | `kafka_connect_connector_status` / `task_status` | Alert immediately if any task enters `FAILED` or `UNASSIGNED` state for greater than 2 minutes. |
| **Consumer Lag (Sinks)** | `kafka_consumer_records_lag_max` | Warning when lag exceeds SLA window; scale `tasksMax` up to target topic partition count. |
| **Poison Pill Rate** | `kafka_connect_task_error_deadletterqueue_produce_requests` | Alert on non-zero rate so data owners inspect DLQ headers and remediate upstream schema drift. |
| **Rebalance Stability** | `kafka_connect_worker_rebalance_rebalance_avg_time_ms` | Alert if frequent or prolonged rebalances occur (indicates JVM GC pauses or network instability). |