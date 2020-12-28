from datetime import datetime

from tkmail import Email
from shop_config import email_config


def send_email(dispose, sale_order):
    email_to = email_config['to_email']
    email_cc = email_config['cc_email']
    email_bcc = email_config['bcc_email']
    email_text = "<p>Danh sách nghi ngờ sai phạm</p>"

    if len(dispose) > 0:
        email_text += f" <p> danh sách hủy hàng </p> "
        for i in dispose.to_dict(orient='records'):
            email_text += f"<p> {i} </p>"
    if len(sale_order) == 0:
        email_text += f" <p> danh sách thanh toán trái phép: </p> "

        for i in sale_order.to_dict(orient='records'):
            email_text += f"<p> {i} </p>"

    subject = "[{}] [Fraud] Báo cáo nghi ngờ".format(str(datetime.now()))

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
    )
    email.send().retry(times=5)

