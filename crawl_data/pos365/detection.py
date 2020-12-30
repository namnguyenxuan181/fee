from datetime import datetime, date

import pandas as pd
import xlrd
from pyspark.sql import SparkSession

from run_date import RunDate
from send_email import send_email

INPUT_COLS = ['_0', 'order_code', '_1', 'tran_at', 'employee', 'table', 'customer_number', '_2', '_3', 'discount', 'amount', '_4', 'debit']
DISPOSE_MAPPING = {
    'Người tạo': 'employee',
    'Tên Phòng / Bàn': 'table',
    'Ngày tạo': 'tran_at',
    'Lý do': 'reason',
    'Tên hàng hóa': 'product',
    'Số lượng': 'quantity',
    'Giá bán': 'prices',
    'Thao tác sau tạm tính?': 'new_amount',
}


def clean(row: str):
    row = str(row)
    if len(row) > 16:
        return row
    return None


def convert_time(row):
    return datetime(*xlrd.xldate_as_tuple(row, 0))


def detect_order_fraud(df: pd.DataFrame) -> pd.DataFrame:
    thungan_list = ['thungan', 'Admin']
    select_cols = ['order_code', 'tran_at', 'employee', 'discount', 'amount']
    return df[~df['employee'].isin(thungan_list)][select_cols]


def detect_dispose_fraud(df: pd.DataFrame) -> pd.DataFrame:
    def detect(employee: str, product: str):
        cashier = 'cashier'
        admin = 'admin'
        if (employee != product) or (cashier not in employee) or (admin not in employee.lower()):
            return True
        return False
    select_cols = ['tran_at', 'employee', 'product', 'reason']
    print(df)
    df['is_fraud'] = df.apply(lambda x: detect(x['employee'], x['product']), axis=1)
    return df[df.is_fraud == True][select_cols]


def detect_fraud(spark: SparkSession):
    run_date = RunDate(date.today())
    print('detection fraud')
    raw_sale_order = pd.read_excel(f'sale_order/{run_date}.xlsx', names=INPUT_COLS)
    raw_sale_order['tran_at'] = raw_sale_order['tran_at'].apply(clean)
    cleaned_sale_order = raw_sale_order[(raw_sale_order.tran_at > '0') & (raw_sale_order.employee > '0')].astype({'employee': 'str'})

    raw_dispose = pd.read_excel(f'dispose/{run_date}.xlsx').rename(columns=DISPOSE_MAPPING).astype({'employee': 'str'})
    existing_fraud_dispose = spark.createDataFrame(pd.read_excel('report/existing_fraud_dispose.xlsx'))
    existing_fraud_sale_order = spark.createDataFrame(pd.read_excel('report/existing_fraud_sale_order.xlsx'))
    fraud_dispose = detect_dispose_fraud(raw_dispose)
    fraud_dispose['tran_at'] = fraud_dispose['tran_at'].apply(convert_time)
    fraud_dispose = spark.createDataFrame(fraud_dispose)
    fraud_sale_order = spark.createDataFrame(detect_order_fraud(cleaned_sale_order))

    new_fraud_dispose = fraud_dispose.subtract(existing_fraud_dispose).toPandas()

    new_fraud_sale_order = fraud_sale_order.subtract(existing_fraud_sale_order).toPandas()

    print(new_fraud_dispose)
    print(new_fraud_sale_order)
    if len(new_fraud_dispose) > 0 or len(new_fraud_sale_order) > 0:
        send_email(
            new_fraud_dispose,
            new_fraud_sale_order
        )
        fraud_dispose.toPandas().to_excel('existing_fraud_dispose.xlsx', index=False)
        fraud_sale_order.toPandas().to_excel('existing_fraud_sale_order.xlsx', index=False)
    else:
        print("don't have any fraud data")


if __name__ == '__main__':
    _spark = SparkSession.builder.getOrCreate()
    detect_fraud(_spark)
