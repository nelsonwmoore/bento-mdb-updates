# MDB Updates

The following diagram maps out the system components and processes:

```mermaid
flowchart TD
    %% Primary Update Flow
    A[New MDFs Available] --> B[Detect New/Updated MDFs]
    B --> C[Update Model Records<br/crdc_models.yml and datahub_models.yml]
    C --> D[Generate Liquibase Changelogs]
    D --> D1[Generate Model Changelog<br/>make_model_changelog]
    D --> D2[Generate & Enrich Model CDE Spec<br/>cde_pf_flow.py]
    D2 --> D3[Convert Model CDE Spec to Changelog<br/>cde_pv_changelog_flow.py]
    D1 & D3 --> E[Save Changelogs to GitHub<br/>bento-mdb repository]
    E --> F[Run Changelogs via Liquibase<br/>to Update MDB]
    F --> G{Model in Data Hub?}
    G -- Yes --> H[Generate JSON with PVs & Synonyms<br/>STS API]
    H --> I[Share JSON to GitHub<br/>crdc-datahub-terms repo]
    G -- No --> J[End Primary Flow]

    %% Secondary Update Flow (Weekly/On-Demand)
    K[Weekly/On-Demand Trigger] --> L[Get Set of CDE PVs from DataHub Models]
    L --> M[Extract NCIt Concept Codes from MDB]
    M --> N[Fetch Current Synonyms for each PV]
    N --> O[Download/Check Updated NCIt/NCIm TSV]
    O --> P{New/Changed Synonyms?}
    P -- Yes --> Q[Generate Cypher/Changelog<br/>to Update MDB]
    Q --> R[Determine Affected Model/Versions]
    R --> S[Update & Share JSON for Data Hub]
    P -- No --> T[End Secondary Flow]

    %% Connecting Flows
    I --> U[Primary Flow Complete]
    S --> U
```