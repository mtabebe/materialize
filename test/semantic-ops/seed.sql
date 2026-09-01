-- Copyright Materialize, Inc. and contributors. All rights reserved.
--
-- Use of this software is governed by the Business Source License
-- included in the LICENSE file at the root of this repository.
--
-- As of the Change Date specified in that file, in accordance with
-- the Business Source License, use of this software will be governed
-- by the Apache License, Version 2.0.

-- The escalation radar: which of our revenue is sitting behind an open sev1?

CREATE TABLE accounts (name text, arr numeric);

-- One row per real provider call. A table rather than a log line so that the
-- counter and the views sit in one session: beats 3, 4 and 5 are all "this number
-- does not move", which is far more convincing beside the answer than in a log.
CREATE TABLE model_calls (input text, column_name text, at timestamptz);

-- The declaration. `severity` and `account_name` are columns of `tickets` that no
-- part of Materialize computes: the planner reads these expressions to learn the
-- input column, the kind and the arguments, then rewrites them away. Rows land in
-- `tickets_raw`, the worker fills `tickets_ai_store_raw`, and `tickets` joins them.
CREATE TABLE tickets (body text, status text)
  ENRICH WITH (
    severity     = ai_classify(body, ARRAY['sev1', 'sev2', 'sev3']),
    account_name = ai_extract(body, 'canonical customer company name')
  );

-- Ordinary IVM over enriched columns, which is the point: once the labels are
-- columns, nothing downstream knows a model was involved.
CREATE MATERIALIZED VIEW revenue_at_risk AS
  SELECT
      count(*)                    AS open_sev1,
      coalesce(sum(a.arr), 0)     AS arr_at_risk
  FROM tickets t
  JOIN accounts a ON a.name = t.account_name
  WHERE t.severity = 'sev1' AND t.status = 'open';

INSERT INTO accounts VALUES
  ('Acme', 1200000),
  ('Globex', 450000),
  ('Initech', 80000),
  ('Umbrella', 2600000);
