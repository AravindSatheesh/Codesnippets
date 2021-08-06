def checkdata(p_DSName, p_ModelName, p_JobName, p_EventName, p_JobRunDate, conn_db, ProdName, USERID):
    
    Status = ""
    
    p_Message = "DATASET: {} WAS VERIFIED: NLOBS={}".format(p_DSName.name, p_DSName.shape[0])
    Status = "update"        
    
    ##Logging about the table
    logupdate(p_ModelName, p_JobName, p_EventName, p_Message, p_JobRunDate, Status, conn_db, ProdName, USERID)


def logupdate(p_ModelName, p_JobName, p_EventName, p_Message, p_JobRunDate, Status, conn_db, ProdName, USERID):
    
    #Connection to Oracle
    cur = conn_db.cursor()
    
    if Status.lower() == 'update'.lower(): #LOG_MESSAGE
        cur.callproc(str(ProdName) + '.PKG_CDR_LOGGING.LOG_MESSAGE', (p_JobName, p_ModelName, p_EventName, p_Message, USERID, p_JobRunDate))
    
    elif Status.lower() == 'warning'.lower(): #LOG_WARNING
        cur.callproc(str(ProdName) + '.PKG_CDR_LOGGING.LOG_WARNING', (p_JobName, p_ModelName, p_EventName, p_Message, USERID, p_JobRunDate))
    
    elif Status.lower() == 'error'.lower(): #LOG_EXCEPTION
        cur.callproc(str(ProdName) + '.PKG_CDR_LOGGING.LOG_EXCEPTION', (p_JobName, p_ModelName, p_EventName, p_Message, USERID, p_JobRunDate))
        
    cur.close()


def cdr_cntr_job(p_JobName, p_JobRunDate, Status, conn_db, ProdName, p_Comment = None, p_DSName = None, p_Step_NUM = None, p_CMNT_Err = None, p_ModelName = None):
    
    output = None
    
    cur = conn_db.cursor()
    
    p_JobRunDate_INT = int(str(p_JobRunDate.strftime('%Y%m%d')))
    
    if Status.lower() == "Start".lower():
        output = cur.callfunc(str(ProdName) + '.PKG_CDR_CONTROL_JOB_V2.FN_START_JOB', int, (p_JobName, p_JobRunDate_INT, p_ModelName))
    
    elif Status.lower() == "End".lower():
        output = cur.callfunc(str(ProdName) + '.PKG_CDR_CONTROL_JOB_V2.FN_END_JOB', int, (p_JobName, p_JobRunDate_INT, p_ModelName))
    
    elif Status.lower() == "No_Execution".lower():
        output = cur.callfunc(str(ProdName) + '.PKG_CDR_CONTROL_JOB_V2.FN_NOEXEC_JOB', int, (p_JobName, p_JobRunDate_INT, p_ModelName))
    
    elif Status.lower() == "Comments".lower():
        if len(p_Comment) > 0:
            pass
            
        else:
            cur.execute("select * from " + str(ProdName) + "." + str(p_DSName))
            data = cur.fetchall()

            col_list = []
            for i in range(len(cur.description)):
                col_list.append(cur.description[i][0])

            python_DF = pd.DataFrame(data, columns = col_list)
            python_DF.name = p_DSName
            
            p_Comment = "" + "STEP" + str(p_Step_NUM) + ":" + str(python_DF.shape[0])
            
        output = cur.callfunc(str(ProdName) + '.PKG_CDR_CONTROL_JOB_V2.FN_ADD_COMMENTS', int, (p_JobName, p_JobRunDate_INT, p_Comment))            
    
    elif Status.lower() == "Status".lower():
        output = cur.callfunc(str(ProdName) + '.PKG_CDR_CONTROL_JOB_V2.FN_GET_JOB_STATUS', str, (p_JobName, p_JobRunDate_INT))
            
    elif Status.lower() == "Failed".lower():
        print("failed", p_JobName, p_JobRunDate_INT)
        output = cur.callfunc(str(ProdName) + '.PKG_CDR_CONTROL_JOB_V2.FN_FAILED_JOB', int, (p_JobName, p_JobRunDate_INT, p_ModelName))
    
    
    cur.close()
    
    return output


def send_email(email_from, email_to,subject, body,sandbx_conn):
    ''' This function would be used to send emails.'''    
    cur = sandbx_conn.cursor()    
    for each_user in email_to:
        cur.callproc('CDW_SHR.SP_SEND_EMAIL', (email_from, each_user, subject, body))
    cur.close()
    return 'Success'

def send_model_status(ModelName, JobRunDate, Status, email_from, email_to, sandbx_conn):
    body = '''{:1}

        Please do not reply to this email.This mailbox is not monitored and you will not receive a response.
        Incase of any question or for more information, Contact Claims Production: @DS Production Support Team
        '''
    if Status.upper() == 'Started'.upper():
        subject = '''PROCESS STARTED: {} Process has started for {}'''.format(ModelName, JobRunDate)
        line = '{} Process has started for {}.'.format(ModelName, JobRunDate)        
        
    elif Status.upper() == 'Completed'.upper():
        subject = '''PROCESS COMPLETED: {} Process completed for {}'''.format(ModelName, JobRunDate)
        line = '{} Process has been completed for {}.'.format(ModelName, JobRunDate)        
        
    elif Status.upper() == 'Already Running'.upper():
        subject = 'ALREADY RUNNING: {} Process is already running for {}'.format(ModelName, JobRunDate)
        line = '{} Process is already running for {}. Cannot run the job if it is already running.'.format(ModelName, JobRunDate)     
        
    elif Status.upper() == 'Already Completed'.upper():
        subject = 'ALREADY COMPLETED: {} Process already completed for {}'.format(ModelName, JobRunDate)
        line = '{} Process is already completed for {}. Cannot run the job if it is already completed.'.format(ModelName, JobRunDate)     
        
    elif Status.upper() == 'Referrals'.upper():
        subject = '{} Referrals Upload for {} has COMPLETED'.format(ModelName[:-3], JobRunDate)
        line = '{} Referrals Upload for {} has COMPLETED successfully.'.format(ModelName, JobRunDate)
    
    elif Status.upper() == 'Error'.upper():
        subject = 'ERROR: {} Process has encountered an error for {}'.format(ModelName, JobRunDate)
        line = '{} Process has encountered an error for {}.'.format(ModelName, JobRunDate)
    
    elif Status.upper() == 'Referrals'.upper():
        subject = '{} Referrals Upload for {} has COMPLETED'.format(ModelName[:-3], JobRunDate)
        line = '{} Referrals Upload for {} has COMPLETED successfully.'.format(ModelName, JobRunDate)
        
    elif Status.upper() == 'No Execution'.upper():
        subject = 'NO EXECUTION: {} on {}'.format(ModelName, JobRunDate)
        line = 'Please be informed that we will not able to execute {} for {} due to data unavailability.'.format(ModelName, JobRunDate)
    
    body = body.format(line)
    return send_email(email_from, email_to, subject, body, sandbx_conn)

def log(msg):
    '''This function prints and puts the message in log file.'''
    print('--> ', msg, ' <--')
    print("")
    return msg 

def error_log(msg):
    print ("Error Encountered! : {}".format(msg))
    return msg

def log_mail(file):
    subject= "Log File for {} process dated {}".format(model_name, jobrundate.strftime('%Y-%m-%d'))
    body = '''
    Hello,
    Please find attached the log file for {model} for {dt}.

    Thanks
    '''.format(model=model_name,dt=jobrundate.strftime('%Y-%m-%d'))
    send_email(email_from, email_to, subject, body, sandbx_conn)
    return 'Log report has been sent'

def log_print(msg):
    '''This function prints and puts the message in log file.'''
    print('--> ', msg, ' <--')
    print("")
    return msg

#Added as part of Production support 2.0
def get_failed_job(ModelName, JobRunDate, conn_db):
    '''This function will retrieve the failed job name and failed stage'''
    import pandas as pd
    query = """SELECT CDR_CTRL_JOB_NM
	      FROM CDW_SHR.CDR_CONTROL_JOB_V2
	     WHERE MODEL_NM = '{}'
	           AND BATCH_DT = {}
	           AND CDR_CTRL_JOB_ID = (SELECT MAX(CDR_CTRL_JOB_ID)
	           FROM CDW_SHR.CDR_CONTROL_JOB_V2
	           WHERE MODEL_NM = '{}'
	           AND BATCH_DT = {})
	           AND CDR_CTRL_JOB_NM != 'MAIN_CODE'
           AND CDR_CTRL_JOB_STATUS != 'COMPLETED'""".format(ModelName, int(str(JobRunDate.strftime('%Y%m%d'))),ModelName, int(str(JobRunDate.strftime('%Y%m%d'))))
    conn_CDW_IMDSCVP = conn_db
    job_name = pd.read_sql_query(query, conn_CDW_IMDSCVP)['CDR_CTRL_JOB_NM']
    
    if len(job_name)>0:
        query1 = """select sub_process_name 
                from CDW_SHR.PROCESS_RUN_MASTER_LIST  
                where job_name='{}'""".format(job_name[0])
        
        failed_job_stage = pd.read_sql_query(query1, conn_CDW_IMDSCVP)['SUB_PROCESS_NAME']
        if len(failed_job_stage)>0:
            failed_job_stage = failed_job_stage[0]
        else:
            failed_job_stage = job_name[0]
            
        failed_job_name = job_name[0]
        
    else:
        failed_job_stage = 'MAIN_CODE'
        failed_job_name = 'MAIN_CODE'

    return (failed_job_stage, failed_job_name)

def send_error_msg(ModelName, JobRunDate, Status, email_from, email_to,  error):
    ''' This function will send notification whenever there is a model failure.''' 
        
    import sys
    sys.path.insert(0,'/data/lake/clmds/models_python/cr_library/')
    from Mailer import sendmail
    if Status.upper() == 'Error'.upper():
        file_name = '{}_ERROR.txt'.format(ModelName)
        with open(file_name,'w') as file:
            for line in error[2]:
                file.write("!! " + line + "\r\n")
        file.close()
        subject = 'ERROR: {} Process has encountered an error for {}'.format(ModelName, JobRunDate)
        line = '''Hi All,
            
{} Process has encountered an error for {}.
The error occured during {} stage. Please find attached the error message.
        
Please do not reply to this email.This mailbox is not monitored and you will not receive a response.
Incase of any question or for more information, Contact Claims Production: @DS Production Support Team
     
     
Thanks & Regards
Ds_Productionsupport'''.format(ModelName, JobRunDate, error[0].upper())
    sendmail(subject,line,email_to,file_name,email_from)
    return
