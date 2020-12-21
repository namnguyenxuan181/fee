import requests

origin = 'https://shopmen01.pos365.vn'
referer = 'https://shopmen01.pos365.vn/'
Content_Type = 'application/json; charset=UTF-8'
accept = 'application/json, text/javascript, */*; q=0.01'

body = '{"format":"HTML5Interactive","deviceInfo":{"enableSearch":true,"ContentOnly":true,"UseSVG":true,"BasePath":"https://reporting.pos365.vn/api/reports"},"useCache":false}'
client = requests.get('https://reporting.pos365.vn/api/reports/clients').value()

body = '{"report":"RevenueDetailForRestaurantReport","parameterValues":{"filter":"{\"TimeRange\":\"7days\",\"Report\":\"RevenueDetailForRestaurantReport\",\"RID\":\"Y2rL0/liws4=\",\"BID\":\"lmB6dGQ+VUA=\",\"CurrentUserId\":172756}"}}'
instances = requests.post(f'https://reporting.pos365.vn/api/reports/clients/{client}/instances')

body='{"format":"HTML5Interactive","deviceInfo":{"enableSearch":true,"ContentOnly":true,"UseSVG":true,"BasePath":"https://reporting.pos365.vn/api/reports"},"useCache":false}'
document = request.post(f'https://reporting.pos365.vn/api/reports/clients/{client}/instances/{instances}/documents')

doc = request.get(f'https://reporting.pos365.vn/api/reports/clients/{client}/instances/{instances}/documents/{document}/info')