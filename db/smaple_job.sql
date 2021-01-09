CREATE DATABASE IF NOT EXISTS sampling;
USE sampling;
DROP TABLE IF EXISTS sampling_job;
CREATE TABLE sampling_job
(
    job_id       BIGINT AUTO_INCREMENT PRIMARY KEY,
    path         VARCHAR(999) NOT NULL,
    method       INT          NOT NULL,
    fraction     VARCHAR(100) NOT NULL,
    col_key      VARCHAR(999),
    file_type    INT          NOT NULL,
    with_header  BOOLEAN      NOT NULL,
    seed         INT          NOT NULL,
    simpled_path VARCHAR(999),
    msg          TEXT,
    start_time   DATETIME,
    end_time     DATETIME
);

CREATE UNIQUE INDEX sampling_job_job_id_uindex
    ON sampling_job (job_id);

ALTER TABLE sampling_job
    AUTO_INCREMENT = 10000



