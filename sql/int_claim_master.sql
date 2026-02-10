CREATE OR REPLACE TEMP VIEW int_claim_master AS
SELECT 
  fc.claim_id, fc.claim_reference, fc.quote_id, fc.insurance_vertical, fc.claim_type_category,
  fc.reported_date, fc.loss_date, fc.policy_start_date, fc.policy_end_date,
  
  -- latest step
  fcs.status_category AS ops_latest_status_category, 
  fcs.status_label AS ops_latest_status_label,
  fcs.days_in_step AS ops_days_in_latest_step,
  CASE WHEN LOWER(fcs.status_category) in ('closed', 'declined', 'settled', 'withdrawn', 'invalid', 'duplicate complaint') THEN 1 ELSE 0 END AS ops_is_terminal_status,

  -- Core timing
  al.first_transition_ts AS ops_first_transition_ts, 
  al.last_transition_ts AS ops_last_transition_ts, 
  al.avg_days_between_steps AS ops_avg_days_between_steps, 
  al.total_steps_count AS ops_total_steps_count,
  
  -- Resolution days 
  ABS(DATEDIFF(al.first_transition_ts, al.last_transition_ts)) AS ops_time_to_latest_status,
  
  -- Flags & friction
  COALESCE(al.reached_settled_flag, 0) AS ops_reached_settled_flag,
  COALESCE(al.reached_payment_complete_flag, 0) AS ops_reached_payment_complete_flag,
  COALESCE(al.cnt_action_required_status, 0) AS ops_cnt_action_required_status,
  COALESCE(al.cnt_doc_missing_status, 0) AS ops_cnt_doc_missing_status,
  
  -- Payments
  COALESCE(ap.has_any_payout_flag, 0) AS fin_has_any_payout_flag,
  COALESCE(ap.has_completed_payout_flag, 0) AS fin_has_completed_payout_flag,
  COALESCE(ap.total_payout_amount_usd, 0) AS fin_total_payout_amount_usd,
  COALESCE(ap.total_payouts, 0) AS fin_total_payouts,
  COALESCE(ap.avg_payout_days, 0) AS fin_avg_payout_days,
  COALESCE(ap.min_payout_steps_to_first_payout, 0) AS fin_min_payout_steps_to_first_payout,
  COALESCE(ap.max_payout_steps_to_last_payout, 0) AS fin_max_payout_steps_to_last_payout,
  COALESCE(ap.time_to_latest_payout, 0) AS fin_time_to_latest_payout, 
  ap.first_payout_ts AS fin_first_payout_ts,
  ap.last_payout_ts AS fin_last_payout_ts,

  -- Ops-payment composite
  DATEDIFF(al.reached_settled_flag_first_ts, ap.last_payout_ts) AS biz_time_to_latest_payout_from_settled,
  DATEDIFF(al.first_transition_ts, ap.last_payout_ts) AS biz_time_to_latest_payout_from_claim_start

FROM fact_claim fc
LEFT JOIN agg_logs al ON fc.claim_id = al.claim_id
LEFT JOIN fact_claim_status fcs ON fc.claim_id = fcs.claim_id AND fcs.is_current_step = true
LEFT JOIN agg_payout ap ON fc.claim_reference = ap.claim_reference;

--make ops tags and fin tags clear with prefixes, we can have biz tags which combines data for both ops and fin