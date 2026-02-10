CREATE OR REPLACE TEMP VIEW kpi_bottleneck_analysis AS
SELECT
  icm.insurance_vertical,
  fcs.status_category,
  icm.ops_reached_settled_flag,
  ROUND(AVG(fcs.days_in_step), 1) AS avg_days_stuck,
  PERCENTILE_APPROX(fcs.days_in_step, 0.9) AS p90_days_stuck,
  COUNT(distinct fcs.claim_id) AS claims_stuck,
  COUNT(distinct CASE WHEN days_in_step >= 7 THEN fcs.claim_id END) AS claims_stuck_7plus_days
FROM fact_claim_status fcs 
  left join int_claim_master icm
  on fcs.claim_id=icm.claim_id
GROUP BY icm.insurance_vertical, fcs.status_category, icm.ops_reached_settled_flag
ORDER BY avg_days_stuck DESC;
