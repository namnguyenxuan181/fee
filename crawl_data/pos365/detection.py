from io import BytesIO

import pandas as pd
from pyspark.sql import SparkSession
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


def detect_order_fraud(df: pd.DataFrame) -> pd.DataFrame:
    thungan_list = ['thungan', 'Admin']
    select_cols = ['order_code', 'tran_at', 'employee', 'discount', 'amount']
    return df[~df['employee'].isin(thungan_list)][select_cols]


def detect_dispose_fraud(df: pd.DataFrame) -> pd.DataFrame:
    def detect(employee, product):
        booking_sub = 'booking'
        if employee not in product and booking_sub in product:
            return True
        return False
    select_cols = ['tran_at', 'employee', 'product', 'reason']
    df['is_fraud'] = df.apply(lambda x: detect(x['employee'], x['product']), axis=1)
    return df[df.is_fraud == True][select_cols]


def detect_fraud(spark: SparkSession):
    raw_sale_order = pd.read_excel('raw_sale_order.xlsx', names=INPUT_COLS)
    raw_sale_order['tran_at'] = raw_sale_order['tran_at'].apply(clean)
    cleaned_sale_order = raw_sale_order[(raw_sale_order.tran_at > '0') & (raw_sale_order.employee > '0')]
    raw_dispose = pd.read_excel('raw_dispose.xlsx').rename(columns=DISPOSE_MAPPING)

    existing_fraud_dispose = spark.createDataFrame(pd.read_excel('existing_fraud_dispose.xlsx'))
    existing_fraud_sale_order = spark.createDataFrame(pd.read_excel('existing_fraud_sale_order.xlsx'))
    fraud_dispose = spark.createDataFrame(detect_dispose_fraud(raw_dispose))
    fraud_sale_order = spark.createDataFrame(detect_order_fraud(cleaned_sale_order))

    new_fraud_dispose = fraud_dispose.subtract(existing_fraud_dispose).toPandas()

    new_fraud_sale_order = fraud_sale_order.subtract(existing_fraud_sale_order).toPandas()

    excel_output = BytesIO()
    print(new_fraud_dispose)
    with pd.ExcelWriter(excel_output) as writer:
        if len(new_fraud_dispose) > 0 or len(new_fraud_sale_order) > 0:
            new_fraud_dispose.to_excel(writer, sheet_name='dispose')
            new_fraud_sale_order.to_excel(writer, sheet_name='sale_order')
    send_email(excel_output, 'fraud_report.xlsx')

    fraud_dispose.toPandas().to_excel('existing_fraud_dispose.xlsx', index=False)
    fraud_sale_order.toPandas().to_excel('existing_fraud_sale_order.xlsx', index=False)


if __name__ == '__main__':
    _spark = SparkSession.builder.getOrCreate()
    detect_fraud(_spark)
