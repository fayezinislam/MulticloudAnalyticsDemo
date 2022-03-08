EXPORT DATA WITH CONNECTION `aws-us-east-1.bq_omni_staging_write` 
  OPTIONS(uri="s3://bq-omni-staging/financial-services/output/timeseries_*.csv", header=true, format="CSV") AS
  WITH TickData AS (
    SELECT
    symbol,
    issuer_name	as issuer,
    trade_price as price,
    trade_size as volume,
    timestamp
    FROM `fsi-select-demo.bq_omni_staging_s3.TRADES_WITH_ISSUER`
    WHERE symbol = 'NRG'
    AND Date(timestamp, "America/New_York")>='2013-01-01')

SELECT
    Date(timestamp, "America/New_York") AS date,
    symbol,
    issuer,
    ARRAY_AGG(price ORDER BY timestamp LIMIT 1) [SAFE_OFFSET(0)] open,
    MAX(price) high,
    MIN(price) low,
    ARRAY_AGG(price ORDER BY timestamp DESC LIMIT 1) [SAFE_OFFSET(0)] close,
    ROUND(SUM(price*volume)/SUM(volume), 2) AS vwap
    FROM TickData
    WHERE TIME(timestamp, "America/New_York") BETWEEN '09:30:00.000000'AND '16:00:00.000000'
    GROUP BY symbol, issuer, date 
    ORDER BY date DESC   
;