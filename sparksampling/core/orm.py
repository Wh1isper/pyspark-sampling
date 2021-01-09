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
                       Column("fraction", VARCHAR(100)),
                       Column("col_key", VARCHAR(999)),
                       Column("file_type", INT),
                       Column("with_header", BOOLEAN),
                       Column("seed", INT),
                       Column("simpled_path", VARCHAR(999)),
                       Column("msg", Text),
                       Column("start_time", DateTime),
                       Column("end_time", DateTime),
                       )
