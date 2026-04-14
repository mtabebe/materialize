psql -h localhost -p 6877 -U mz_system materialize -f misc/ontology/create_schema.sql
psql -h localhost -p 6877 -U mz_system materialize -f misc/ontology/entity_types.sql
psql -h localhost -p 6877 -U mz_system materialize -f misc/ontology/properties.sql
psql -h localhost -p 6877 -U mz_system materialize -f misc/ontology/semantic_types.sql
psql -h localhost -p 6877 -U mz_system materialize -f misc/ontology/link_types.sql
