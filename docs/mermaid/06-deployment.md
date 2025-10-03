```mermaid
flowchart LR
  classDef node fill:#e6f7ff,stroke:#4a4a4a;
  classDef client fill:#fdf5e6,stroke:#333;
  classDef infra fill:#ffe3e3,stroke:#4a4a4a;

  subgraph Clients
    P([Producers]):::client
    C([Consumers]):::client
    MATLAB[[MATLAB]]:::client
  end

  subgraph SingleNode["Single-node (MVP)"]
    A[Broker Node A Adapters + App + Domain + WAL/Index(zap, prometheus, pprof)]:::node
  end

  P -->|MQTT/TCP| A
  C -->|MQTT/TCP| A
  MATLAB -->|MQTT| A

  %% Evolution to cluster
  subgraph Cluster["Future: Cluster"]
    A2[Broker A]:::node
    B2[Broker B]:::node
    C2[Broker C]:::node
    RC[(hashicorp/raft\nmetadata consensus)]:::infra
    CH[(buraksezer/consistent\npartition→node)]:::infra
  end

  A2 <---> B2
  B2 <---> C2
  C2 <---> A2
  RC --- A2
  RC --- B2
  RC --- C2
  CH --- A2
  CH --- B2
  CH --- C2
```