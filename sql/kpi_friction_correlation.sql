CREATE OR REPLACE TEMP VIEW kpi_friction_correlation AS
SELECT
  insurance_vertical,
  CORR(ops_cnt_doc_missing_status, fin_total_payout_amount_usd) AS doc_friction_payout_corr,
  CORR(ops_cnt_action_required_status, ops_days_in_latest_step) AS action_friction_delay_corr,
  CORR(ops_total_steps_count, ops_time_to_latest_status) AS steps_resolution_corr,
  AVG(fin_total_payout_amount_usd) AS avg_payout_usd,
  AVG(ops_cnt_doc_missing_status) AS avg_doc_requests
FROM int_claim_master
GROUP BY insurance_vertical;
