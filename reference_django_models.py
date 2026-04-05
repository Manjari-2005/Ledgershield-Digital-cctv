"""
Reference Django model architecture for salami-slicing detection (Django ORM + TigerGraph sync).

Not used by the Flask dashboard — copy patterns into your Django project.

Suggested entities
------------------
- Merchant: mid, name
- Account: iban or internal id, kyc_tier
- Transaction: txn_id, created_at, amount_inr, source_account (FK), destination_key
  (CharField: merchant_id OR sink account), optional merchant (FK), status
- VelocityWindow: bucket_start, destination_key, micro_tx_count, micro_volume_inr
  (materialized by aggregator every 5 minutes)
- CircuitBreakerState: destination_key, window_start, unique_sources, tripped_at,
  status in (monitoring, pending_verification)
- FraudNotification: transaction (FK), channel (push|sms|email), payload (JSON)

Indexes: (destination_key, created_at) on Transaction; created_at for time-range scans.

TigerGraph: upsert Transaction vertices and SENT_TO edges to Destination/Merchant for graph velocity;
keep Django as system of record, TG for cross-ring linkage with LedgerShield.
"""
