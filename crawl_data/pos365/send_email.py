from datetime import date

from tkmail import Email
from shop_config import email_config


def send_email(value, file_name):
    email_to = email_config['to_email']
    email_cc = email_config['cc_email']
    email_bcc = email_config['bcc_email']

    email_text = """
        <p>Dear phòng vận hành.</p>
        <p>Team Fraud gửi danh sách cảnh báo DVCNTT nghi ngờ gian lận.</p>
    """
    subject = "[{}] [Fraud] Báo cáo nghi ngờ".format(str(date.today()))
    print(value.getvalue(), file_name)
    email = (
        Email(
            username=email_config['username'],
            password=email_config['password'],
            subject=subject,
            sender='FRAUD_TEAM',
            to=email_to,
            cc=email_cc,
            bcc=email_bcc,
        )
        .html(email_text)
        .attachment(value.getvalue(), name=file_name)
    )
    email.send().retry(times=5)

