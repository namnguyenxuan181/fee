import pandas as pd
from tkmail import Email

from io import BytesIO
from common.email.constants import Sender
from common.helper.config import config
from common.spark.helper import init_spark
from common.email import connection
from publisher.common.path import PublishVlan2Path


RUN_DATE = config.run_date


def rename_df(df):
    df = df.toPandas()
    return df.rename(
        columns={
            'merchant_code': 'merchant code',
            'merchant_name': 'tên merchant',
            'tran_date': 'ngày giao dịch',
            'tran_at': 'thời điểm giao dịch',
            'list_tran': 'danh sách giao dịch',
            'debit_day': 'giá trị giao dịch 1 ngày',
            'mcc_level3': 'ngành hàng',
            'avg_debit_month': 'giá trị giao dịch trung bình tháng',
            'avg_debit_mcc': 'giá trị giao dịch trung bình của ngành hàng trong 1 tháng',
            'threshold': 'ngưỡng cảnh báo',
            'reason': 'lý do',
            'qr_trace': 'mã giao dịch',
            'discount_amount': 'giá trị khuyễn mãi',
            'debit_amount': 'giá trị giao dịch',
            'avg_discount_month': 'khuyễn mãi trung bình ngày trong tháng trước',
            'total_tran': 'số lượng giao dịch',
            'total_account': 'số lượng tài khoản',
            'total_debit': 'tổng giá trị giao dịch',
            'pre_tran_day': 'thời gian dao dịch trước',
            'pre_qr_trace': 'mã giao dịch trước',
            'total_debit_one_day': 'giá trị giao dịch 1 ngày',
            'avg_debit_last_week': 'giá trị giao dịch trung bình tuần trước',
            'transform_promotion_code': 'mã khuyễn mãi',
            'max_debit_day': 'giá trị giao dịch lớn nhất trong 1 ngày',
            'ratio': 'hệ số nhân',
            'employee_name': 'tên nhân viên',
        }
    )


def send_email(value, file_name):

    email_to = config.email_to
    email_cc = config.email_cc
    email_bcc = config.email_bcc

    email_text = """
        <p>Dear phòng vận hành.</p>
        <p>Team Fraud gửi danh sách cảnh báo DVCNTT nghi ngờ gian lận.</p>
    """
    subject = "[{}] [DWH] [Fraud] Báo cáo DVCNTT nghi ngờ".format(str(RUN_DATE.to_date()))

    email = (
        Email(
            **connection.get_connection(),
            subject=subject,
            sender=Sender.VNPAY_REPORT_SYSTEM,
            to=email_to,
            cc=email_cc,
            bcc=email_bcc,
        )
        .html(email_text)
        .attachment(value.getvalue(), name=file_name)
    )
    email.send().retry(times=5)


def load_report(spark, rule_name):
    return spark.read.parquet(
        PublishVlan2Path.FRAUD_DETECTION_22_LANG_HA.generate(partition=f"{rule_name}/date={RUN_DATE}")
    )


def main():
    spark = init_spark(app_name='fraud_detection_22_lang_ha', master='local')
    rule1 = load_report(spark, "rule1")
    rule2 = load_report(spark, "rule2")
    rule3 = load_report(spark, "rule3")
    rule4 = load_report(spark, "rule4")
    rule5 = load_report(spark, "rule5")
    rule6 = load_report(spark, "rule6")
    rule7_1 = load_report(spark, "rule7_1")
    rule7_2 = load_report(spark, "rule7_2")
    rule8 = load_report(spark, "rule8")
    rule9 = load_report(spark, "rule9")

    report_list = {
        'rule1': rename_df(rule1),
        'rule2': rename_df(rule2),
        'rule3': rename_df(rule3),
        'rule4': rename_df(rule4),
        'rule5': rename_df(rule5),
        'rule6': rename_df(rule6),
        'rule7_1': rename_df(rule7_1),
        'rule7_2': rename_df(rule7_2),
        'rule8': rename_df(rule8),
        'rule9': rename_df(rule9),
    }

    file_name = f'report_vhtt_{RUN_DATE}.xlsx'
    excel_output = BytesIO()
    with pd.ExcelWriter(excel_output) as writer:
        for name, df in report_list.items():
            if len(df) != 0:
                df.to_excel(writer, sheet_name=name)

    send_email(excel_output, file_name)


if __name__ == '__main__':
    main()
