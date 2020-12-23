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
    'time_range': TimeRange.TODAY,
    'report_duration': 1000*60*15,
}
