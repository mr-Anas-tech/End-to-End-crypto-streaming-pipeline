USE GoldAnalytics;
GO

CREATE OR ALTER VIEW dim_crypto_assets AS
SELECT *
FROM OPENROWSET(
    BULK 'dim_crypto_assets',
    DATA_SOURCE = 'GoldStorageSource',
    FORMAT = 'DELTA'
) AS [marts];
GO

SELECT  * FROM dim_crypto_assets;


CREATE OR ALTER VIEW fct_crypto_hourly_metrics AS
SELECT *
FROM OPENROWSET(
    BULK 'fct_crypto_hourly_metrics',
    DATA_SOURCE = 'GoldStorageSource',
    FORMAT = 'DELTA'
) AS [marts];
GO

select TOP 10 * from fct_crypto_hourly_metrics;

SELECT 
COUNT_BIG(*) as total
from fct_crypto_hourly_metrics;