"""
database orm
"""
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, ForeignKey, MetaData, Table
from sqlalchemy import (BigInteger, SmallInteger, DateTime, VARCHAR, CHAR, Text, INT, FLOAT, BOOLEAN)

Meta = MetaData()
Base = declarative_base()

SampleJobTable = Table('sampling_job', Meta,
                       Column("job_id", BigInteger, primary_key=True, autoincrement=True),
                       Column("path", VARCHAR(999)),
                       Column("method", INT),
                       Column("request_data", Text),
                       Column("simpled_path", VARCHAR(999)),
                       Column("status_code", INT),
                       Column("msg", Text),
                       Column("start_time", DateTime),
                       Column("end_time", DateTime),
                       )

EvaluationJobTable = Table('evaluation_job', Meta,
                           Column("job_id", BigInteger, primary_key=True, autoincrement=True),
                           Column("path", VARCHAR(999)),
                           Column("source_path", VARCHAR(999)),
                           Column("method", INT),
                           Column("request_data", Text),
                           Column("result", Text),
                           Column("status_code", INT),
                           Column("msg", Text),
                           Column("start_time", DateTime),
                           Column("end_time", DateTime),
                           )
