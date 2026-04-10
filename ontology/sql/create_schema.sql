-- Create the mz_ontology schema and tables
-- Run first, then load data files in any order
-- Usage: psql -h localhost -p 6877 -U mz_system materialize -f create_schema.sql

CREATE SCHEMA IF NOT EXISTS ma_mz_ontology;

CREATE TABLE IF NOT EXISTS ma_mz_ontology.entity_types (
    name        text NOT NULL,
    relation    text NOT NULL,
    properties  jsonb,
    description text
);

CREATE TABLE IF NOT EXISTS ma_mz_ontology.semantic_types (
    name        text NOT NULL,
    sql_type    text NOT NULL,
    description text
);

CREATE TABLE IF NOT EXISTS ma_mz_ontology.properties (
    entity_type     text NOT NULL,
    column_name     text NOT NULL,
    semantic_type   text,
    description     text
);

CREATE TABLE IF NOT EXISTS ma_mz_ontology.link_types (
    name            text NOT NULL,
    source_entity   text NOT NULL,
    target_entity   text NOT NULL,
    properties      jsonb,
    description     text
);
