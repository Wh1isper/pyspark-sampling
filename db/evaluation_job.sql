CREATE DATABASE IF NOT EXISTS sampling;
USE sampling;
# DROP TABLE IF EXISTS evaluation_job;

CREATE TABLE IF NOT EXISTS evaluation_job
(
    job_id       BIGINT AUTO_INCREMENT PRIMARY KEY,
    path         VARCHAR(999) NOT NULL,
    source_path  VARCHAR(999),
    method       TEXT         NOT NULL,
    request_data TEXT         NOT NULL,
    status_code  INT          NOT NULL,
    result       TEXT,
    msg          TEXT,
    start_time   DATETIME,
    end_time     DATETIME
);

CREATE UNIQUE INDEX evaluation_job_job_id_uindex
    ON evaluation_job (job_id);

ALTER TABLE evaluation_job
    AUTO_INCREMENT = 50000;
