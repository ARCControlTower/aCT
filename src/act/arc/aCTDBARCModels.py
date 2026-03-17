from sqlalchemy import Integer, String, ForeignKey, TIMESTAMP, Text, SmallInteger, DateTime, LargeBinary
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship, declared_attr
from datetime import datetime, timezone
import re
import arc
from typing import Optional

class Base(DeclarativeBase):
    pass

class ArcJobMixin:
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    modified: Mapped[datetime] = mapped_column(TIMESTAMP, nullable=False, default=lambda: datetime.now(timezone.utc), onupdate=lambda: datetime.now(timezone.utc))
    created: Mapped[datetime] = mapped_column(TIMESTAMP, nullable=False, default=lambda: datetime.now(timezone.utc))
    arcstate: Mapped[Optional[str]] = mapped_column(String(12), index=True)
    tarcstate: Mapped[Optional[datetime]] = mapped_column(TIMESTAMP)
    tstate: Mapped[Optional[datetime]] = mapped_column(TIMESTAMP)
    cluster: Mapped[Optional[str]] = mapped_column(String(255))
    clusterlist: Mapped[Optional[str]] = mapped_column(String(1024))
    jobdesc: Mapped[Optional[int]] = mapped_column(Integer, ForeignKey('jobdescriptions.id')) # rename to jobdescid
    attemptsleft: Mapped[Optional[int]] = mapped_column(Integer)
    downloadfiles: Mapped[Optional[str]] = mapped_column(String(255))
    proxyid: Mapped[Optional[int]] = mapped_column(Integer, ForeignKey('proxies.id'))
    appjobid: Mapped[Optional[str]] = mapped_column(String(16))
    priority: Mapped[Optional[int]] = mapped_column(SmallInteger)
    fairshare: Mapped[Optional[str]] = mapped_column(String(50))

    @declared_attr
    def jobdescobj(col):
        return relationship('JobDescription', back_populates='arcjob')
    
    @declared_attr
    def proxy(col):
        return relationship('Proxy', back_populates='arcjobs')
    
def dynamicArcJob():
    jobattrmap = {int: Integer,
                str: String(255),
                arc.JobState: String(255),
                arc.StringList: String(1024),
                arc.URL: String(255),
                arc.Period: Integer,
                arc.Time: DateTime,
                arc.StringStringMap: String(1024)}
    ignoremems=['STDIN',
                'STDOUT',
                'STDERR',
                'STAGEINDIR',
                'STAGEOUTDIR',
                'SESSIONDIR',
                'JOBLOG',
                'JOBDESCRIPTION',
                'JobDescriptionDocument',
                'LOGDIR'
                ]
    attr = {'__tablename__': 'arcjobs'}
    j=arc.Job()
    for i in dir(j):
        if re.match('^__',i):
            continue
        if i in ignoremems:
            continue
        if type(getattr(j, i)) in jobattrmap:
            attr[i] = mapped_column(jobattrmap[type(getattr(j, i))])
    
    return type('ArcJob', (Base, ArcJobMixin), attr)

ArcJob = dynamicArcJob()

class JobDescription(Base):
    __tablename__ = 'jobdescriptions'

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    jobdescription: Mapped[Optional[str]] = mapped_column(Text)

    arcjob: Mapped['ArcJob'] = relationship(back_populates='jobdescobj')

class Proxy(Base):
    __tablename__ = 'proxies'

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    proxy: Mapped[Optional[str]] = mapped_column(Text)
    expirytime: Mapped[Optional[datetime]] = mapped_column(DateTime)
    proxypath: Mapped[Optional[str]] = mapped_column(String(255))
    dn: Mapped[Optional[str]] = mapped_column(String(255))
    attribute: Mapped[Optional[str]] = mapped_column(String(255))
    proxytype: Mapped[Optional[str]] = mapped_column(String(255))
    myproxyid: Mapped[Optional[str]] = mapped_column(String(255))

    arcjobs: Mapped[list['ArcJob']] = relationship('ArcJob', back_populates='proxy')