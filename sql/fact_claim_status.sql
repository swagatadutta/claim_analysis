CREATE OR REPLACE TEMP VIEW fact_claim_status AS
WITH base_query as
(SELECT
  ClaimId AS claim_id,
  ClaimReference AS claim_reference,
  CAST(TransitionDate AS TIMESTAMP) AS transition_ts,
  TransitionType,
  CurrentStatusCategory AS status_category,
  CurrentStatusLabel AS status_label,
  ROW_NUMBER() OVER (PARTITION BY ClaimId ORDER BY CAST(TransitionDate AS TIMESTAMP)) AS step_sequence,
  LAG(CAST(TransitionDate AS TIMESTAMP)) OVER (PARTITION BY ClaimId ORDER BY CAST(TransitionDate AS TIMESTAMP)) AS prev_transition_ts
FROM raw_claim_logs)

SELECT *, 
  DATEDIFF(transition_ts,prev_transition_ts) as days_in_step,
  transition_ts = MAX(transition_ts) OVER (PARTITION BY claim_id) AS is_current_step
FROM base_query;
