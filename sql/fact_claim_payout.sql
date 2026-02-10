CREATE OR REPLACE TEMP VIEW fact_claim_payout AS
SELECT
  ClaimReference AS claim_reference,
  QuoteId AS quote_id,
  CAST(TransactionDate AS TIMESTAMP) AS transaction_ts,
  PaymentReference AS payment_reference,
  PayoutStatus AS payout_status,
  CAST(PayoutAmount AS DOUBLE) AS payout_amount_usd,
  ROW_NUMBER() OVER (PARTITION BY ClaimReference ORDER BY CAST(TransactionDate AS TIMESTAMP)) AS payment_sequence
FROM raw_claim_payouts;
