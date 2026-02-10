CREATE OR REPLACE TEMP VIEW kpi_step_timing AS

SELECT
  icm.insurance_vertical,
  fcs.status_category,
  fcs.is_current_step,
  AVG(fcs.days_in_step) AS avg_days_in_status,
  PERCENTILE_APPROX(fcs.days_in_step, 0.5) AS median_days_in_status,
  PERCENTILE_APPROX(fcs.days_in_step, 0.95) AS p95_days_in_status,
  COUNT(distinct icm.claim_id) AS claims_in_status,
  COUNT(distinct CASE WHEN fin_has_completed_payout_flag=1 then icm.claim_id END)*0.1/COUNT(distinct icm.claim_id) as payout_completion_rate_pct
FROM int_claim_master icm
LEFT JOIN fact_claim_status fcs 
  on icm.claim_id=fcs.claim_id
GROUP BY icm.insurance_vertical, fcs.status_category, fcs.is_current_step;
