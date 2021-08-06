import logging
import smtplib
import socket
from os import path
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email import encoders

logger = logging.getLogger(__name__)

def sendmail(subject,
             body,
             to_addr,
             attachments=[],
             from_addr='ace'):
    
    '''This function send email with attachements
    Attributes:
        subject= Subject of email.String input
        body= Body of email.String input
        to_addr= List of email recipients
        attachments= Path of attachment file. String input
        from_addr= Sender email address. String input'''
    
    # Handle recepient(s) as a list every time. Might come in as a string or list.
    if not isinstance(to_addr, list):
        to_addr = [x.strip() for x in to_addr.split(',')]

    # Attachment(s) might come in as a string (single file) or list of files.
    if not isinstance(attachments, list):
        attachments = [attachments]

    msg = MIMEMultipart()
    msg['From'] = from_addr
    msg['To'] = ', '.join(to_addr)
    msg['Subject'] = subject

    if body:
        msg.attach(MIMEText(body))

    for attachment in attachments:
        with open(attachment, 'rb') as f:
            part = MIMEBase('application', 'octet-stream')
            part.set_payload(f.read())
            encoders.encode_base64(part)
            fname = path.basename(attachment)
            part.add_header(
                'Content-Disposition',
                'attachment; filename= {}'.format(fname)
            )
            msg.attach(part)

    s = smtplib.SMTP('higmx.thehartford.com')
    s.sendmail(msg['From'], to_addr, msg.as_string())
    s.quit()
    logger.debug('Sent email!')