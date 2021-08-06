import logging
import os
import datetime as dt
JobRunDate = int(str(dt.date.today().strftime("%Y%m%d")))


logging.basicConfig(level=logging.INFO,format='%(asctime)s %(levelname)s %(message)s',datefmt='%m/%d/%Y %H:%M:%S',
                    filename='/data/lake/clmds/projects/Prod_Support/Execution/Prod/Logs/logs_'+str(JobRunDate) + '.log',filemode='w')
log = logging.getLogger('logger')
log.setLevel(logging.INFO)
fileName='/data/lake/clmds/projects/Prod_Support/Execution/Prod/Logs/logs_'+str(JobRunDate)
fileHandler = logging.FileHandler("{}.log".format(fileName))
fileHandler.setFormatter(logging.Formatter('%(asctime)s %(levelname)s %(message)s', '%m/%d/%Y %H:%M:%S'))
log.addHandler(fileHandler)
