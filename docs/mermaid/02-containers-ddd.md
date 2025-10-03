%%{init: {'theme':'default'}}%%
```mermaid
flowchart TB
  classDef adapter fill:#d0ebff,stroke:#4a4a4a;
  classDef app fill:#d3f9d8,stroke:#4a4a4a;
  classDef domain fill:#ffd8a8,stroke:#4a4a4a;
  classDef infra fill:#ffe3e3,stroke:#4a4a4a;

  subgraph Adapters["Adapters (Inbound/Outbound)"]
    MQTT[mqtt]:::adapter
    TCP[netproto]:::adapter
    Admin[admin]:::adapter
    Expo[metrics]:::adapter
  end

  subgraph Application["Application (use cases)"]
    ProduceSvc[produce]:::app
    FetchSvc[fetch]:::app
    OffsetSvc[offsets]:::app
    MetadataSvc[metadata]:::app
  end

  subgraph Domain["Domain (aggregates)"]
    TopicAgg[topic]:::domain
    PartitionAgg[partition]:::domain
    GroupAgg[group]:::domain
    RecordModel[record]:::domain
  end

  subgraph Infrastructure["Infrastructure"]
    Storage[storage (WAL/Index, snappy/zstd)]:::infra
    Hashing[hashing (xxhash + jump)]:::infra
    Consistent[placement (buraksezer/consistent)]:::infra
    Raft[consensus (hashicorp/raft, future)]:::infra
    Logs[zap logs]:::infra
    Pprof[pprof]:::infra
    Prom[prometheus client]:::infra
  end

  MQTT --> ProduceSvc
  MQTT --> FetchSvc
  TCP  --> ProduceSvc
  TCP  --> FetchSvc
  Admin --> MetadataSvc

  ProduceSvc --> TopicAgg
  ProduceSvc --> PartitionAgg
  FetchSvc --> PartitionAgg
  OffsetSvc --> GroupAgg
  MetadataSvc --> TopicAgg

  PartitionAgg --> Storage
  GroupAgg --> Storage
  TopicAgg --> Storage

  Hashing --> PartitionAgg
  Consistent --> TopicAgg
  Raft --> TopicAgg

  ProduceSvc --> Logs
  FetchSvc --> Logs
  Storage --> Prom
  Storage --> Pprof
```