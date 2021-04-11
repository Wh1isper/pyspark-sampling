CREATE DATABASE IF NOT EXISTS sampling;
USE sampling;
# DROP TABLE IF EXISTS sampling_job;
CREATE TABLE IF NOT EXISTS sampling_job
(
    job_id       BIGINT AUTO_INCREMENT PRIMARY KEY,
    path         VARCHAR(999) NOT NULL,
    method       TEXT         NOT NULL,
    request_data TEXT         NOT NULL,
    status_code  INT          NOT NULL,
    simpled_path VARCHAR(999),
    msg          TEXT,
    start_time   DATETIME,
    end_time     DATETIME
);

CREATE UNIQUE INDEX sampling_job_job_id_uindex
    ON sampling_job (job_id);

ALTER TABLE sampling_job
    AUTO_INCREMENT = 10000;

