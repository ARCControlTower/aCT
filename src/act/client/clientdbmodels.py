from sqlalchemy import Integer, String, ForeignKey, TIMESTAMP, Text, SmallInteger, DateTime, LargeBinary
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship, declared_attr
import datetime
from act.arc.aCTDBARCModels import Base, ArcJob, Proxy
from typing import Optional

class ClientJob(Base):
    __tablename__ = 'clientjobs'
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    modified: Mapped[datetime.datetime] = mapped_column(TIMESTAMP, nullable=False, default=lambda: datetime.datetime.now(datetime.UTC), onupdate=lambda: datetime.datetime.now(datetime.UTC))
    created: Mapped[Optional[datetime.datetime]] = mapped_column(TIMESTAMP)
    jobname: Mapped[Optional[str]] = mapped_column(String(255))
    jobdesc: Mapped[Optional[str]] = mapped_column(Text)
    clusterlist: Mapped[Optional[str]] = mapped_column(String(1024))
    arcjobid: Mapped[Optional[int]] = mapped_column(Integer, ForeignKey('arcjobs.id'))
    proxyid: Mapped[Optional[int]] = mapped_column(Integer, ForeignKey('proxies.id'))

    arcjob: Mapped['ArcJob'] = relationship()
    proxy: Mapped['Proxy'] = relationship()