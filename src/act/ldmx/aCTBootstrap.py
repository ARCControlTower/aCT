import os
from act.common.aCTLogger import aCTLogger
from act.ldmx.aCTDBLDMX import aCTDBLDMX

def bootstrap():

    logger = aCTLogger('aCTBootstrap')
    log = logger()
    dbldmx = aCTDBLDMX(log)
    if os.getuid() != 0:
        print('Error: You must be root user to create LDMX tables!')
        return
    if not dbldmx.createTables():
        print('Failed to create LDMX tables, see aCTBootstrap.log for details')

    # Add root user
    dbldmx.insertUser(0, 'root', 'admin')
    print('Created root ldmx user')