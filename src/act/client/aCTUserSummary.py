
import arc
from act.arc.aCTDBArcNEW import aCTDBArc
from act.client.clientdb import ClientDB
from act.common.aCTConfig import aCTConfigARC
from act.common.aCTProcess import aCTProcess
from sqlalchemy import select,func,insert,delete
from act.client.dbModels import Proxy, ArcJob, UserSummary


class aCTUserSummary(aCTProcess):
    """Object that runs until interrupted and periodically submits new jobs."""

    # overriding to prevent cluster argument
    def __init__(self):
        super().__init__()

    def loadConf(self):
        self.conf = aCTConfigARC()

    def setup(self):
        super().setup()
        self.db = ClientDB(self.log)

    def wait(self):
        super().wait(limit=30)

    def process(self):
        """
        Creates summary of user jobs counts every 30s
        """
        self.stopOnFlag()
        name = func.substring_index(Proxy.dn, 'CN=', -1)

        cn = func.concat(
            func.left(name, 1),
            '.',
            func.substring_index(name, ' ', -1)
        ).label('cn')

        subq = (select(Proxy.id,cn,ArcJob.cluster,ArcJob.arcstate,func.count().label('cnt'))
                .join(Proxy.arcjobs)
                .group_by(Proxy.id,cn,ArcJob.cluster,ArcJob.arcstate)).subquery()

        stmt = (select(subq.c.id,subq.c.cn,subq.c.cluster,func.json_objectagg(subq.c.arcstate,subq.c.cnt).label('states'))
                .group_by(subq.c.id,subq.c.cn,subq.c.cluster)
                .order_by(subq.c.id,subq.c.cn,subq.c.cluster))
        
        insert_stmt = insert(UserSummary).from_select(['id', 'cn', 'cluster', 'states'],stmt)

        with self.db.Session.begin() as session:
            session.execute(delete(UserSummary))
            session.execute(insert_stmt)

        self.log.info('Successfully created summary')

    def finish(self):
        super().finish()
