import utils
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import os
import logging

logger = logging.getLogger(__name__)

def send_ticket_report(corpus):

    try:
        message = MIMEMultipart()
        message["From"] = os.environ.get("sender_email_address")

        message['To'] = os.environ.get("receiver_email_address")
        message['Subject'] = "NoReply: Daily Report on Lidal Data Injection"
        message.attach(MIMEText(corpus, 'plain'))
        

        with smtplib.SMTP(os.environ.get("smtp_server"), int(os.environ.get("port"))) as server:
            server.starttls()
            server.login(os.environ.get("sender_email_address"), os.environ.get("password"))
            server.send_message(message)
            
        return True
    except Exception as e:
        return False
