from sqlalchemy import Integer, String, ForeignKey, TIMESTAMP, Text, SmallInteger, DateTime, LargeBinary, BigInteger, Boolean
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship, declared_attr
from sqlalchemy.dialects.mysql import MEDIUMTEXT
from datetime import datetime, timezone
from act.arc.dbModels import Base, ArcJob, Proxy
from typing import Optional

class PandaJob(Base):
    __tablename__ = 'pandajobs'

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    modified: Mapped[datetime] = mapped_column(TIMESTAMP, nullable=False, default=lambda: datetime.now(timezone.utc), onupdate=lambda: datetime.now(timezone.utc))
    created: Mapped[datetime] = mapped_column(TIMESTAMP, nullable=False, default=lambda: datetime.now(timezone.utc))
    pandajob: Mapped[str] = mapped_column(Text().with_variant(MEDIUMTEXT, 'mysql'))
    pandaid: Mapped[int] = mapped_column(BigInteger, unique=True, index=True)
    siteName: Mapped[str] = mapped_column(String(255), index=True)
    prodSourceLabel: Mapped[str] = mapped_column(String(255))
    arcjobid: Mapped[int] = mapped_column(Integer, index=True)
    condorjobid: Mapped[int] = mapped_column(Integer, index=True)
    pandastatus: Mapped[str] = mapped_column(String(255), index=True)
    actpandastatus: Mapped[str] = mapped_column(String(255), index=True)
    theartbeat: Mapped[datetime] = mapped_column(TIMESTAMP)
    priority: Mapped[int] = mapped_column(Integer)
    node: Mapped[str] = mapped_column(String(255))
    startTime: Mapped[datetime] = mapped_column(TIMESTAMP)
    endTime: Mapped[datetime] = mapped_column(TIMESTAMP)
    computingElement: Mapped[str] = mapped_column(String(255))
    proxyid: Mapped[int] = mapped_column(Integer, ForeignKey('proxies.id'))
    sendhb: Mapped[int] = mapped_column(Boolean, default=True)
    corecount: Mapped[int] = mapped_column(Integer)
    metadata_: Mapped[bytes] = mapped_column("metadata", LargeBinary)
    error: Mapped[str] = mapped_column(Text().with_variant(MEDIUMTEXT, 'mysql'))

    arcjob: Mapped[ArcJob] = relationship(ArcJob, primaryjon= "ArcJob.id==PandaJob.arcjobid")
    proxy: Mapped[Proxy] = relationship()

class PandaArchive(Base):
    __tablename__ = 'pandaarchives'

    pandajob: Mapped[str] = mapped_column(Text().with_variant(MEDIUMTEXT, 'mysql'))
    pandaid: Mapped[int] = mapped_column(BigInteger)
    siteName: Mapped[str] = mapped_column(String(255))
    actpandastatus: Mapped[str] = mapped_column(String(255))
    startTime: Mapped[datetime] = mapped_column(TIMESTAMP)
    endTime: Mapped[datetime] = mapped_column(TIMESTAMP)