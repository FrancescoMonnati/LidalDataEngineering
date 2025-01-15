import utils
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import os


def send_ticket_report(processed_files, errors):

    try:
        message = MIMEMultipart()
        message["From"] = os.environ.get("sender_email_address")

        message['To'] = os.environ.get("receiver_email_address")
        message['Subject'] = "NoReply: Daily Report on Lidal Data Injection"
        
        status_file = ""
        if processed_files:
            
            for file_info in processed_files:
                status_file += 'success' if file_info.get('success', False) else 'warning'

        else:
            status_file += "No files were processed"
        status_error = ""
        if errors:      
            for error in errors:
                status_error += f"{error}"
        else:
            status_error += "No errors encountered during processing"
        body = status_file + "/n" + status_error
        message.attach(MIMEText(body, 'plain'))
        

        with smtplib.SMTP(os.environ.get("smtp_server"), int(os.environ.get("port"))) as server:
            server.starttls()
            server.login(os.environ.get("sender_email_address"), os.environ.get("password"))
            server.send_message(message)
            
        return True
    except Exception as e:
        print(f"Error sending email: {str(e)}")
        return False
