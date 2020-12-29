class Report:
    ORDER_DETAIL_REPORT = 'ProductSaleDetailReport'
    ORDER_REPORT = 'RevenueDetailForRestaurantReport'


class TimeRange:
    TODAY = 'today'
    TIME_RANGE_7DAYS = '7days'


shop_config = {
    'shop_name': 'sayahdemo',
    'RID': 'nxINeMdBwtw=',
    'BID': 'X3m7fcjuAyo=',
    'CurrentUserId': 179024,
    'report': Report.ORDER_REPORT,
    'time_range': TimeRange.TIME_RANGE_7DAYS,
    'report_duration': 60*5,
}

shop_cookie = {
    'ss-id': 'ojFoLFEqZhiWYkydtho7',
    'ss-pid': 'evOYpYMW4B0GeqvJJg49',
    'ss-opt': 'perm',
}

email_config = {
    'username': 'firedragon1801a@gmail.com',
    'password': '18011997a',
    'to_email': 'nam.nguyenxuan181@gmail.com,tuanluu89@gmail.com',
    'cc_email': '',
    'bcc_email': '',

}
