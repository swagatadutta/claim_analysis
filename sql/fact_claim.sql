CREATE OR REPLACE TEMP VIEW fact_claim AS
SELECT
  ClaimId AS claim_id,
  ClaimReference AS claim_reference,
  QuoteId AS quote_id,
  InsuranceVertical AS insurance_vertical,
  ClaimStatus AS claim_status,
  ClaimTypeCategory AS claim_type_category,
  ReportedDate AS reported_date,
  LossDate AS loss_date,
  PolicyStartDate AS policy_start_date,
  PolicyEndDate AS policy_end_date,
  
  -- NEW: Policy coverage flags
  CASE 
    WHEN TO_DATE(ReportedDate) BETWEEN TO_DATE(PolicyStartDate) AND TO_DATE(PolicyEndDate)
    THEN true ELSE false 
  END AS report_date_within_policy,
  
  CASE 
    WHEN TO_DATE(LossDate) BETWEEN TO_DATE(PolicyStartDate) AND TO_DATE(PolicyEndDate) 
    THEN true ELSE false 
  END AS loss_date_within_policy,
  
  -- Active flag
  CASE 
    WHEN TO_DATE(PolicyEndDate) >= CURRENT_DATE() THEN true 
    ELSE false 
  END AS is_current
FROM raw_claims;
