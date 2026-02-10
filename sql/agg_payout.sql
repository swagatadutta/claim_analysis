CREATE OR REPLACE TEMP VIEW agg_payout AS

WITH base_query(
  select *, 
  MIN(transaction_ts) OVER (PARTITION BY claim_reference) as first_transaction_ts,
  MAX(transaction_ts) OVER (PARTITION BY claim_reference) as last_transaction_ts,
  MIN(CASE WHEN UPPER(payout_status) LIKE '%COMPLETED%' THEN transaction_ts end) OVER (PARTITION BY claim_reference) first_payout_ts,
  MAX(CASE WHEN UPPER(payout_status) LIKE '%COMPLETED%' THEN transaction_ts end) OVER (PARTITION BY claim_reference) last_payout_ts,
  RANK() OVER (PARTITION BY claim_reference ORDER BY transaction_ts ASC) payment_status_rank -- calculate with payment_reference granularity? 1 payment reference has multiple rows -- discrepancy
  from fact_claim_payout fcp
)

SELECT
  fcp.claim_reference,  
  MAX(1) AS has_any_payout_flag,
  MAX(CASE WHEN UPPER(fcp.payout_status) LIKE '%COMPLETED%' THEN 1 ELSE 0 END) AS has_completed_payout_flag,
  SUM(CASE WHEN UPPER(fcp.payout_status) LIKE '%COMPLETED%' THEN fcp.payout_amount_usd ELSE 0 END) AS total_payout_amount_usd,  -- discovery 1 claim can have multiple payouts
  COUNT(DISTINCT CASE WHEN UPPER(fcp.payout_status) LIKE '%COMPLETED%' THEN fcp.payment_reference END) AS total_payouts,
  COUNT(DISTINCT fcp.payment_reference) AS total_payout_attempts,
  AVG(CASE WHEN UPPER(fcp.payout_status) LIKE '%COMPLETED%' THEN ABS(DATEDIFF(fcp.first_transaction_ts, fcp.transaction_ts)) END) avg_payout_days, -- avg days between first claim approved vs payout days
  MIN(CASE WHEN UPPER(fcp.payout_status) LIKE '%COMPLETED%' THEN payment_status_rank END) min_payout_steps_to_first_payout, -- step = payout attempts 
  MAX(CASE WHEN UPPER(fcp.payout_status) LIKE '%COMPLETED%' THEN payment_status_rank END) max_payout_steps_to_last_payout,
  ABS(DATEDIFF(MIN(fcp.transaction_ts), MAX(fcp.transaction_ts))) as time_to_latest_payout, -- claim start to payment complete ? or 1st payment attempt to payment complete or claim approved to payment complete
  MAX(fcp.first_transaction_ts) AS first_transaction_ts,
  MAX(fcp.last_transaction_ts) AS last_transaction_ts,
  MAX(fcp.first_payout_ts) AS first_payout_ts,
  MAX(fcp.last_payout_ts) AS last_payout_ts
FROM base_query fcp
GROUP BY fcp.claim_reference;  