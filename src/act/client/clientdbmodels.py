from sqlalchemy import Integer, String, ForeignKey, TIMESTAMP, Text, SmallInteger, DateTime, LargeBinary
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship, declared_attr
import datetime
from arc.aCTDBARCModels import Base, ArcJob, Proxy

class ClientJob(Base):
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    modified: Mapped[datetime.datetime] = mapped_column(TIMESTAMP, nullable=False, default=lambda: datetime.datetime.now(datetime.UTC), onupdate=lambda: datetime.datetime.now(datetime.UTC))
    created: Mapped[datetime.datetime | None] = mapped_column(TIMESTAMP)
    jobname: Mapped[str | None] = mapped_column(String(255))
    jobdesc: Mapped[str | None] = mapped_column(Text)
    clusterlist: Mapped[str | None] = mapped_column(String(1024))
    arcjobid: Mapped[int | None] = mapped_column(Integer, ForeignKey('arcjobs.id'))
    proxyid: Mapped[int | None] = mapped_column(Integer, ForeignKey('proxies.id'))

    arcjob: Mapped['ArcJob'] = relationship()
    proxy: Mapped['Proxy'] = relationship()