-- BigQuery SQL Queries


-- Optimism Batcher Transactions
-- Run on 16/02/2024
SELECT
  `hash`,
  block_timestamp, 
  gas AS gas_used, 
  receipt_gas_used AS gas_used_final, 
  gas_price/1000000000 AS gas_price_gwei,
  receipt_effective_gas_price/1000000000 AS gas_price_final_gwei,
  max_priority_fee_per_gas/1000000000 AS max_priority_fee_gwei,
FROM `bigquery-public-data.crypto_ethereum.transactions` 
WHERE 
  (block_timestamp >= CAST(DATE_SUB(CURRENT_DATE(), INTERVAL 6 MONTH) AS TIMESTAMP)) AND
  (receipt_status = 1) AND
  (from_address = '0x6887246668a3b87f54deb3b94ba47a6f63f32985') -- Batcher transactions on mainnet
ORDER BY block_number


-- Optimism Proposer Transactions
-- Run on 16/02/2024
SELECT
  `hash`,
  block_timestamp, 
  gas AS gas_used, 
  receipt_gas_used AS gas_used_final, 
  gas_price/1000000000 AS gas_price_gwei,
  receipt_effective_gas_price/1000000000 AS gas_price_final_gwei,
  max_priority_fee_per_gas/1000000000 AS max_priority_fee_gwei,
FROM `bigquery-public-data.crypto_ethereum.transactions` 
WHERE 
  (block_timestamp >= CAST(DATE_SUB(CURRENT_DATE(), INTERVAL 6 MONTH) AS TIMESTAMP)) AND
  (receipt_status = 1) AND
  (from_address = '0x473300df21d047806a082244b417f96b32f13a33') -- Proposer transactions on mainnet
ORDER BY block_number


-- Optimism gas fee prices per block
-- Run on 14/02/2024
SELECT 
  ANY_VALUE(tx.block_timestamp) AS timestamp, 
  tx.block_hash, 
  COUNT(*) AS tx_count,
  MAX(tx.max_priority_fee_per_gas)/1000000000 AS max_miner_tip_gwei, 
  MIN(tx.max_priority_fee_per_gas)/1000000000 AS min_miner_tip_gwei, 
  AVG(tx.max_priority_fee_per_gas)/1000000000 AS avg_miner_tip_gwei,
  ANY_VALUE(bl.base_fee_per_gas)/1000000000 as base_fee_gwei
FROM 
  `bigquery-public-data.goog_blockchain_optimism_mainnet_us.transactions` AS tx, 
  `bigquery-public-data.goog_blockchain_optimism_mainnet_us.blocks` AS bl 
WHERE 
  (tx.max_priority_fee_per_gas IS NOT NULL) AND
  (tx.block_hash = bl.block_hash) AND
  (tx.block_timestamp >= CAST(DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH) AS TIMESTAMP))
GROUP BY tx.block_hash
ORDER BY timestamp


-- Optimism priority fees per transaction
-- Run on 14/02/2024
SELECT 
  block_timestamp AS timestamp, 
  transaction_hash, 
  max_priority_fee_per_gas/1000000000 AS max_priority_fee_gwei,
  max_fee_per_gas/1000000000 AS max_gas_fee_gwei
FROM 
  `bigquery-public-data.goog_blockchain_optimism_mainnet_us.transactions`
WHERE 
  (max_priority_fee_per_gas IS NOT NULL) AND
  (block_timestamp >= CAST(DATE_SUB(CURRENT_DATE(), INTERVAL 15 DAY) AS TIMESTAMP))
ORDER BY timestamp


-- Ethereum gas fee prices per block
-- Run on 16/02/2024
SELECT 
  ANY_VALUE(tx.block_timestamp) AS timestamp, 
  tx.block_hash, 
  COUNT(*) AS tx_count,
  MAX(tx.max_priority_fee_per_gas)/1000000000 AS max_miner_tip_gwei, 
  MIN(tx.max_priority_fee_per_gas)/1000000000 AS min_miner_tip_gwei, 
  AVG(tx.max_priority_fee_per_gas)/1000000000 AS avg_miner_tip_gwei,
  ANY_VALUE(bl.base_fee_per_gas)/1000000000 as base_fee_gwei
FROM 
  `bigquery-public-data.crypto_ethereum.transactions` AS tx, 
  `bigquery-public-data.crypto_ethereum.blocks` AS bl 
WHERE 
  (tx.max_priority_fee_per_gas IS NOT NULL) AND
  (tx.block_hash = bl.hash) AND
  (tx.block_timestamp >= CAST(DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH) AS TIMESTAMP)) AND
  (tx.receipt_status = 1)
GROUP BY tx.block_hash
ORDER BY timestamp


-- Ethereum priority fees per transaction
-- Run on 16/02/2024
SELECT 
  block_timestamp AS timestamp, 
  max_priority_fee_per_gas/1000000000 AS max_priority_fee_gwei,
  max_fee_per_gas/1000000000 AS max_gas_fee_gwei
FROM 
  `bigquery-public-data.crypto_ethereum.transactions`
WHERE 
  (max_priority_fee_per_gas IS NOT NULL) AND
  (block_timestamp >= CAST(DATE_SUB(CURRENT_DATE(), INTERVAL 15 DAY) AS TIMESTAMP)) AND
  (receipt_status = 1)
ORDER BY timestamp