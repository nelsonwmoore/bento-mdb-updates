# Steps to use prefect deployment to load Neo4j database from S3 dump

1. Register Prefect deployment with work pool with `prefect deploy`
2. Run deployment with parameters:
    ```console
    prefect deployment run neo4j-database-load/neo4j-database-load --param cluster="fnl-mdb-dev-ecs" --param s3_bucket="fnl-mdb-data" --param s3_key="mdb-dev-20250617.dmp" --param database_name="neo4j" --param task_definition_family="fnlmdbdevneo4jtaskDef" --param mdb_id="fnl-mdb-dev"
    ```
    a. If on FNL side, use `fnl-mdb-dev-ecs` for `cluster`, `fnlmdbdevneo4jtaskDef` for `task_definition_family`, and `fnl-mdb-dev` for `mdb_id`
    
    b. If on Cloud One side, use `?` for `cluster`, `?` for `task_definition_family`, and  `cloud-one-mdb-dev` for `mdb_id` and 