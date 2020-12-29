import time

from pyspark.sql.session import SparkSession

from detection import detect_fraud
from dispose import download_dispose_report
from sale_order import SaleOrderCrawler
from shop_config import shop_config


def main(spark: SparkSession):
    while True:
        download_dispose_report()
        SaleOrderCrawler().run()
        detect_fraud(spark)
        time.sleep(shop_config['report_duration'])


if __name__ == '__main__':
    _spark = SparkSession.builder.getOrCreate()
    main(_spark)

