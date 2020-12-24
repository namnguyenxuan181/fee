class Report:
    ORDER_DETAIL_REPORT = 'ProductSaleDetailReport'
    ORDER_REPORT = 'RevenueDetailForRestaurantReport'


class TimeRange:
    TODAY = 'today'
    TIME_RANGE_7DAYS = '7days'


config = {
    'shop_name': 'yldemo',
    'RID': 'gnmxWtLa8OM=',
    'BID': 'Jx2xfLllIZM=',
    'CurrentUserId': 176193,
    'report': Report.ORDER_REPORT,
    'time_range': TimeRange.TIME_RANGE_7DAYS,
    'report_duration': 60*15,
}

email_config = {
    'username': 'firedragon1801a@gmail.com',
    'password': '18011997a',
    'to_email': 'nam.nguyenxuan181@gmail.com',
    'cc_email': '',
    'bcc_email': '',

}
