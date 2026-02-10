CREATE OR REPLACE TEMP VIEW kpi_workflow_efficiency AS
SELECT
  insurance_vertical,
  ROUND(AVG(ops_total_steps_count), 1) AS avg_steps_per_claim,
  ROUND(AVG(DATEDIFF(ops_last_transition_ts, ops_first_transition_ts)*1.0/ops_total_steps_count), 1) AS avg_days_between_steps,
  ROUND(AVG(ops_time_to_latest_status), 1) AS avg_resolution_days,
  ROUND(100.0 * AVG(ops_reached_settled_flag), 1) AS settled_rate_pct,
  ROUND(AVG(ops_cnt_doc_missing_status), 1) AS avg_doc_friction
FROM int_claim_master
GROUP BY insurance_vertical;
