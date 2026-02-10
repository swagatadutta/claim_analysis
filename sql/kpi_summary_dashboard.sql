CREATE OR REPLACE TEMP VIEW kpi_summary_dashboard AS
SELECT
  'Summary' AS report_type,
  insurance_vertical,
  COUNT(*) AS total_claims,
  ROUND(AVG(ops_time_to_latest_status), 1) AS avg_resolution_days,
  ROUND(AVG(ops_total_steps_count), 1) AS avg_workflow_steps,
  ROUND(100.0 * AVG(ops_reached_settled_flag), 1) AS settled_rate_pct,
  ROUND(100.0 * AVG(fin_has_completed_payout_flag), 1) AS payout_rate_pct,
  ROUND(SUM(fin_total_payout_amount_usd), 0) AS total_payout_usd,
  ROUND(AVG(ops_days_in_latest_step), 1) AS avg_days_current_status,
  ROUND(AVG(ops_cnt_doc_missing_status), 1) AS avg_doc_friction,
  -- Bottleneck alert
  SUM(CASE WHEN ops_days_in_latest_step > 14 and ops_is_terminal_status=0 THEN 1 ELSE 0 END) AS claims_stuck_14days
FROM int_claim_master
group by insurance_vertical;
