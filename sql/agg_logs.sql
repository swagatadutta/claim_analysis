CREATE OR REPLACE TEMP VIEW agg_logs AS
SELECT
  claim_id,
  MIN(transition_ts) AS first_transition_ts,
  MAX(transition_ts) AS last_transition_ts,
  MAX(CASE WHEN lower(status_label) LIKE '%approve%' THEN 1 ELSE 0 END) AS reached_approved_flag,
  MIN(CASE WHEN lower(status_label) LIKE '%approve%' THEN transition_ts END) AS reached_approve_flag_first_ts,
  MAX(CASE WHEN lower(status_category) LIKE '%settled%' THEN 1 ELSE 0 END) AS reached_settled_flag,
  MIN(CASE WHEN lower(status_category) LIKE '%settled%' THEN transition_ts END) AS reached_settled_flag_first_ts,
  MAX(CASE WHEN lower(status_category) LIKE '%payment complete%' or lower(status_label) LIKE '%paid%' THEN 1 ELSE 0 END) AS reached_payment_complete_flag,
  MIN(CASE WHEN lower(status_category) LIKE '%payment complete%' or lower(status_label) LIKE '%paid%' THEN transition_ts END) AS reached_payment_complete_flag_first_ts,
  ROUND(AVG(CASE 
    WHEN step_sequence > 1 AND prev_transition_ts IS NOT NULL 
    THEN (UNIX_TIMESTAMP(transition_ts) - UNIX_TIMESTAMP(prev_transition_ts)) / 86400.0 
  END), 2) AS avg_days_between_steps,
  MAX(step_sequence) AS total_steps_count,
  SUM(CASE WHEN lower(status_category) LIKE 'action required' THEN 1 ELSE 0 END) AS cnt_action_required_status,
  SUM(CASE WHEN LOWER(status_label) LIKE '%document%' OR LOWER(status_label) LIKE '%missing%' THEN 1 ELSE 0 END) AS cnt_doc_missing_status
FROM fact_claim_status
GROUP BY claim_id;
 