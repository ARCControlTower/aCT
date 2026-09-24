
import arc
from act.arc.aCTDBArcNEW import aCTDBArc
from act.client.clientdb import ClientDB
from act.common.aCTConfig import aCTConfigARC
from act.common.aCTProcess import aCTProcess
from sqlalchemy import select,func,insert,delete,case
from act.client.dbModels import Proxy, ArcJob, UserSummary

# arcstate values that group together several ARC-reported States (e.g.
# 'submitted' covers ARC's Accepted/Preparing/Submitting/Queuing). For
# these, use the more granular ARC State instead of the arcstate label.
# Every other arcstate (tosubmit, cancelled, done, ...) has no meaningful
# or no more detailed corresponding State, so it's kept as-is.
EXPAND_TO_ARC_STATE = ('submitted', 'running', 'finishing', 'holding')


class aCTUserSummary(aCTProcess):
    """Object that runs until interrupted and periodically creates summary of user jobs."""

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

        combinedState = case(
            (ArcJob.arcstate.in_(EXPAND_TO_ARC_STATE), func.coalesce(ArcJob.State, ArcJob.arcstate)),
            else_=ArcJob.arcstate
        ).label('combinedstate')

        subq = (select(Proxy.id,cn,ArcJob.cluster,combinedState,func.count().label('cnt'))
                .join(Proxy.arcjobs)
                .group_by(Proxy.id,cn,ArcJob.cluster,combinedState)).subquery()

        stmt = (select(subq.c.id,subq.c.cn,subq.c.cluster,func.json_objectagg(subq.c.combinedstate,subq.c.cnt).label('states'))
                .group_by(subq.c.id,subq.c.cn,subq.c.cluster)
                .order_by(subq.c.id,subq.c.cn,subq.c.cluster))
        
        insert_stmt = insert(UserSummary).from_select(['id', 'cn', 'cluster', 'states'],stmt)

        with self.db.Session.begin() as session:
            session.execute(delete(UserSummary))
            session.execute(insert_stmt)

        self.log.info('Successfully created summary')

    def finish(self):
        super().finish()
