import requests
import json

origin = 'https://shopmen01.pos365.vn'
referer = 'https://shopmen01.pos365.vn/'
Content_Type = 'application/json; charset=UTF-8'
accept = 'application/json, text/javascript, */*; q=0.01'

body = '{"format":"HTML5Interactive","deviceInfo":{"enableSearch":true,"ContentOnly":true,"UseSVG":true,"BasePath":"https://reporting.pos365.vn/api/reports"},"useCache":false}'
client = requests.post(
    'https://reporting.pos365.vn/api/reports/clients',
    headers={
        'origin': origin,
        'referer': referer,
        'accept': accept,
        'Content-Type': Content_Type,
    },
    # json={"timeStamp": 1608570470626},
).json()['clientId']
print(client)

data01 = {"report": "RevenueDetailForRestaurantReport",
          "parameterValues":
              {
                  "filter": "{\"TimeRange\":\"7days\",\"Report\":\"RevenueDetailForRestaurantReport\",\"RID\":\"Y2rL0/liws4=\",\"BID\":\"lmB6dGQ+VUA=\",\"CurrentUserId\":172756}",
                      # {
              #         "TimeRange": "7days",
              #         "Report": "RevenueDetailForRestaurantReport",
              #         "RID": "Y2rL0/liws4=",
              #         "BID": "lmB6dGQ+VUA=",
              #         "CurrentUserId": 172756,
                  }
              # }
          }
param = requests.post(
    f'https://reporting.pos365.vn/api/reports/clients/1d8a56c6dd9/parameters',
    headers={
        'origin': origin,
        'referer': referer,
        'accept': accept,
        'Content-Type': Content_Type,
    },
    json=data01,

)
print(param)
print(param.text)
# body = '{"report":"RevenueDetailForRestaurantReport","parameterValues":{"filter":"{\"TimeRange\":\"7days\",\"Report\":\"RevenueDetailForRestaurantReport\",\"RID\":\"Y2rL0/liws4=\",\"BID\":\"lmB6dGQ+VUA=\",\"CurrentUserId\":172756}"}}'
#
data = {
    "report": 'RevenueDetailForRestaurantReport',
    'CurrentUserId': '172756',
    'parameterValues': {
        'filter': "{\"TimeRange\":\"7days\",\"Report\":\"RevenueDetailForRestaurantReport\",\"RID\":\"Y2rL0/liws4=\",\"BID\":\"lmB6dGQ+VUA=\",\"CurrentUserId\":172756}"
    }
}
instances = requests.post(
    f'https://reporting.pos365.vn/api/reports/clients/{client}/instances',
    headers={
        'origin': 'https://shopmen01.pos365.vn',
        'referer': 'https://shopmen01.pos365.vn/',
        'Content-Type': 'application/json; charset=UTF-8',
        'accept': 'application/json, text/javascript, */*; q=0.01',
        'Accept-Encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive',
    },
    json=data
).json()['instanceId']
print(instances)

#
# body='{"format":"HTML5Interactive","deviceInfo":{"enableSearch":true,"ContentOnly":true,"UseSVG":true,"BasePath":"https://reporting.pos365.vn/api/reports"},"useCache":false}'
data = {
    'format': 'HTML5Interactive',
    'deviceInfo': {
        'enableSearch': True,
        'ContentOnly': True,
        'UseSVG': True,
        'BasePath': 'https://reporting.pos365.vn/api/reports',

    },
    'useCache': False
}
document = requests.post(
    f'https://reporting.pos365.vn/api/reports/clients/{client}/instances/{instances}/documents',

    headers={
        'origin': 'https://shopmen01.pos365.vn',
        'referer': 'https://shopmen01.pos365.vn/',
        'Content-Type': 'application/json; charset=UTF-8',
        'accept': 'application/json, text/javascript, */*; q=0.01',
        'Accept-Encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive',
    },
    json=data
).json()['documentId']
print(document)
import time

print(requests.get(
    f'https://reporting.pos365.vn/api/reports/clients/{client}/instances/{instances}/documents/{document}/info').json()[
          'documentReady'])
while (requests.get(
        f'https://reporting.pos365.vn/api/reports/clients/{client}/instances/{instances}/documents/{document}/info').json()[
           'documentReady'] is not True):
    print(requests.get(
        f'https://reporting.pos365.vn/api/reports/clients/{client}/instances/{instances}/documents/{document}/info').json()[
              'documentReady'])
    time.sleep(500)
data2 = {
    "format": "XLS",
    "deviceInfo": {
        "enableSearch": True,
        "BasePath": "https://reporting.pos365.vn/api/reports"
    },
    "useCache": True,
    "baseDocumentID": document,
}
print(data2)

final_doc_id = requests.post(
    f'https://reporting.pos365.vn/api/reports/clients/{client}/instances/{instances}/documents',

    headers={
        'origin': 'https://shopmen01.pos365.vn',
        'referer': 'https://shopmen01.pos365.vn/',
        'Content-Type': 'application/json; charset=UTF-8',
        'accept': 'application/json, text/javascript, */*; q=0.01',
    },
    json=data2
).json()['documentId']
page = requests.get(
    f'https://reporting.pos365.vn//api/reports/clients/{client}/instances/{instances}/documents/{document}/pages/1',
    headers={
        'origin': 'https://shopmen01.pos365.vn',
        'referer': 'https://shopmen01.pos365.vn/',
        'Content-Type': 'application/json; charset=UTF-8',
        'accept': 'application/json, text/javascript, */*; q=0.01',
    },
)
print(f'https://reporting.pos365.vn//api/reports/clients/{client}/instances/{instances}/documents/{document}/pages/1')
print(page.text)
option = requests.options(
    f'https://reporting.pos365.vn//api/reports/clients/{client}/instances/{instances}/documents/{document}',
    headers={
        'origin': 'https://shopmen01.pos365.vn',
        'referer': 'https://shopmen01.pos365.vn/',
        'Content-Type': 'application/json; charset=UTF-8',
        'accept': 'application/json, text/javascript, */*; q=0.01',
    },
)
print(option.text)
while (requests.get(
        f'https://reporting.pos365.vn/api/reports/clients/{client}/instances/{instances}/documents/{final_doc_id}/info').json()[
           'documentReady'] is not True):
    print(requests.get(
        f'https://reporting.pos365.vn/api/reports/clients/{client}/instances/{instances}/documents/{final_doc_id}/info').json()[
              'documentReady'])
    time.sleep(5000)
print(document)
print(final_doc_id)
print(
    f'https://reporting.pos365.vn/api/reports/clients/{client}/instances/{instances}/documents/{final_doc_id}?response-content-disposition=attachment')
# url = 'https://reporting.pos365.vn/api/reports/clients/2ff4e99d3e6/instances/8a277080128/documents/988275f78980fc9f0a6f86?response-content-disposition=attachment'
doc = requests.get(
    # 'https://reporting.pos365.vn/api/reports/clients/cb2b75430fe/instances/8a277080128/documents/90063346248557ed0de18a?response-content-disposition=attachment',
    f'https://reporting.pos365.vn/api/reports/clients/{client}/instances/{instances}/documents/{final_doc_id}?response-content-disposition=attachment',
    headers={
        'origin': 'https://shopmen01.pos365.vn',
        'referer': 'https://shopmen01.pos365.vn/',
        'Content-Type': 'application/json; charset=UTF-8',
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
    },
)
print(doc)
print(doc.text)

with open("my_file.xlsx", 'wb') as f:
    f.write(doc.content)
