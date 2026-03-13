# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from textwrap import dedent

from materialize.feature_benchmark.action import Action, TdAction
from materialize.feature_benchmark.measurement_source import MeasurementSource, Td
from materialize.feature_benchmark.scenario import Scenario, ScenarioBig


class DDL(Scenario):
    """Benchmarks for basic DDL operations on a small catalog."""

    pass


class CreateTable(DDL):
    """Measure the time it takes to CREATE TABLE on a small catalog."""

    def before(self) -> Action:
        return TdAction("> DROP TABLE IF EXISTS bench_t CASCADE;")

    def benchmark(self) -> MeasurementSource:
        return Td(
            dedent(
                """
                > SELECT 1
                  /* A */;
                1
                > CREATE TABLE bench_t (f1 INTEGER)
                  /* B */
                """
            )
        )


class CreateMaterializedView(DDL):
    """Measure the time it takes to CREATE MATERIALIZED VIEW on a small catalog."""

    def init(self) -> Action:
        return TdAction(
            dedent(
                """
                > CREATE TABLE IF NOT EXISTS mv_source (f1 INTEGER);
                > INSERT INTO mv_source VALUES (1);
                """
            )
        )

    def before(self) -> Action:
        return TdAction("> DROP MATERIALIZED VIEW IF EXISTS bench_mv;")

    def benchmark(self) -> MeasurementSource:
        return Td(
            dedent(
                """
                > SELECT 1
                  /* A */;
                1
                > CREATE MATERIALIZED VIEW bench_mv AS SELECT COUNT(*) FROM mv_source
                  /* B */
                """
            )
        )


class CreateIndex(DDL):
    """Measure the time it takes to CREATE INDEX on a small catalog."""

    def init(self) -> Action:
        return TdAction(
            dedent(
                """
                > CREATE TABLE IF NOT EXISTS idx_source (f1 INTEGER);
                > INSERT INTO idx_source VALUES (1);
                """
            )
        )

    def before(self) -> Action:
        return TdAction("> DROP INDEX IF EXISTS bench_idx;")

    def benchmark(self) -> MeasurementSource:
        return Td(
            dedent(
                """
                > SELECT 1
                  /* A */;
                1
                > CREATE INDEX bench_idx ON idx_source (f1)
                  /* B */
                """
            )
        )


class DDLLargeCatalog(ScenarioBig):
    """Benchmarks for DDL operations after populating a large catalog.

    Inherits from ScenarioBig since the init phase is expensive.
    Override NUM_CATALOG_OBJECTS in subclasses to control catalog size directly
    (e.g., 500, 25, 100000) without being constrained to powers of 10.
    """

    NUM_CATALOG_OBJECTS: int = 500
    FIXED_SCALE = True

    def init(self) -> list[Action]:
        n = self.NUM_CATALOG_OBJECTS

        create_tables = "\n".join(
            f"> CREATE TABLE cat_t{i} (f1 INTEGER);" for i in range(n)
        )

        return [
            TdAction(
                dedent(
                    f"""
                    $ postgres-connect name=mz_system url=postgres://mz_system:materialize@${{testdrive.materialize-internal-sql-addr}}
                    $ postgres-execute connection=mz_system
                    ALTER SYSTEM SET max_objects_per_schema = {n * 3};
                    ALTER SYSTEM SET max_tables = {n * 3};
                    ALTER SYSTEM SET max_materialized_views = {n * 3};
                    """
                )
            ),
            TdAction(create_tables),
        ]


class CreateTableLargeCatalog(DDLLargeCatalog):
    """Measure CREATE TABLE time with a large existing catalog."""

    def before(self) -> Action:
        return TdAction("> DROP TABLE IF EXISTS bench_t CASCADE;")

    def benchmark(self) -> MeasurementSource:
        return Td(
            dedent(
                """
                > SELECT 1
                  /* A */;
                1
                > CREATE TABLE bench_t (f1 INTEGER)
                  /* B */
                """
            )
        )


class CreateMaterializedViewLargeCatalog(DDLLargeCatalog):
    """Measure CREATE MATERIALIZED VIEW time with a large existing catalog."""

    def init(self) -> list[Action]:
        actions = super().init()
        actions.append(
            TdAction(
                dedent(
                    """
                    > CREATE TABLE IF NOT EXISTS mv_source (f1 INTEGER);
                    > INSERT INTO mv_source VALUES (1);
                    """
                )
            )
        )
        return actions

    def before(self) -> Action:
        return TdAction("> DROP MATERIALIZED VIEW IF EXISTS bench_mv;")

    def benchmark(self) -> MeasurementSource:
        return Td(
            dedent(
                """
                > SELECT 1
                  /* A */;
                1
                > CREATE MATERIALIZED VIEW bench_mv AS SELECT COUNT(*) FROM mv_source
                  /* B */
                """
            )
        )


class CreateIndexLargeCatalog(DDLLargeCatalog):
    """Measure CREATE INDEX time with a large existing catalog."""

    def init(self) -> list[Action]:
        actions = super().init()
        actions.append(
            TdAction(
                dedent(
                    """
                    > CREATE TABLE IF NOT EXISTS idx_source (f1 INTEGER);
                    > INSERT INTO idx_source VALUES (1);
                    """
                )
            )
        )
        return actions

    def before(self) -> Action:
        return TdAction("> DROP INDEX IF EXISTS bench_idx;")

    def benchmark(self) -> MeasurementSource:
        return Td(
            dedent(
                """
                > SELECT 1
                  /* A */;
                1
                > CREATE INDEX bench_idx ON idx_source (f1)
                  /* B */
                """
            )
        )
