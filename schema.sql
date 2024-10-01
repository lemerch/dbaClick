CREATE DATABASE app ON CLUSTER 'example';

CREATE TABLE app.test_local ON CLUSTER '{cluster}'
(
    `key` UInt32,
    `txt` String,
    `ts` DateTime
)
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{cluster}/{shard}/table', '{replica}')
PARTITION BY toYYYYMM(ts)
PRIMARY KEY (key)
ORDER BY key;

CREATE TABLE IF NOT EXISTS app.test_main ON CLUSTER '{cluster}' AS app.test_local
ENGINE = Distributed('{cluster}', app, test_local, key);

INSERT INTO app.test_main values
(1, 'a', '2024-01-01 12:00:00'),
(2, 'a', '2024-01-01 12:00:00'),
(3, 'a', '2024-01-01 12:00:00'),
(4, 'a', '2024-01-01 12:00:00');

optimize table app.test_local ON cluster 'example';

SELECT * FROM app.test_local;
SELECT * FROM app.test_main;

SELECT * FROM app.test_local final;
SELECT * FROM app.test_main final;

INSERT INTO app.test_main values
(1, 'c', '2024-02-01 12:00:00'),
(2, 'd', '2024-02-01 12:00:00'),
(3, 'a', '2024-02-01 12:00:00'),
(4, 'a', '2024-02-01 12:00:00');

truncate TABLE app.test_local ON cluster 'example';
DROP TABLE app.test_main ON cluster 'example';
DROP TABLE app.test_local ON cluster 'example';