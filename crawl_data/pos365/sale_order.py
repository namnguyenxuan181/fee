import requests
import time
from shop_config import config


class SaleOrderCrawler:
    def __init__(self):
        self.base_url = 'https://reporting.pos365.vn/api/reports'
        self.headers = {
            'origin': f'https://{config["shop_name"]}.pos365.vn',
            'referer': f'https://{config["shop_name"]}.pos365.vn/',
            'accept': 'application/json, text/javascript, */*; q=0.01',
            'Content-Type': 'application/json; charset=UTF-8',
        }

    def get_client(self) -> str:
        return requests.post(
            'https://reporting.pos365.vn/api/reports/clients',
            headers=self.headers
        ).json()['clientId']

    def get_instances(self, client: str) -> str:
        data = {
            "report": config['report'],
            'parameterValues': {
                'filter': "{\"TimeRange\":\"%s\",\"Report\":\"%s\",\"SoldBy\":0,\"RID\":\"%s\",\"BID\":\"%s\",\"CurrentUserId\":%s}" \
                          % (config['time_range'], config['report'], config['RID'], config['BID'], config['CurrentUserId'])
            }
        }
        print(data)

        return requests.post(
            f'{self.base_url}/clients/{client}/instances',
            headers=self.headers,
            json=data
        ).json()['instanceId']

    def set_param(self, client: str):

        data = {
            "report": config['report'],
            'parameterValues': {
                'filter': "{\"TimeRange\":\"%s\",\"Report\":\"%s\",\"SoldBy\":0,\"RID\":\"%s\",\"BID\":\"%s\",\"CurrentUserId\":%s}" \
                          % (config['time_range'], config['report'], config['RID'], config['BID'], config['CurrentUserId'])
            }
        }
        param = requests.post(
            f'{self.base_url}/clients/{client}/parameters',
            headers=self.headers,
            json=data,

        )
        print('param', param.json())

    def get_base_document(self, client: str, instance: str) -> str:
        data = {
            'format': 'HTML5Interactive',
            'deviceInfo': {
                'enableSearch': True,
                'ContentOnly': True,
                'UseSVG': True,
                'BasePath': self.base_url,
            },
            'useCache': False
        }

        return requests.post(
            f'{self.base_url}/clients/{client}/instances/{instance}/documents',
            headers=self.headers,
            json=data,
        ).json()['documentId']

    def confirm_document_ready(self, client: str, instance: str, document: str):
        while (
            requests.get(
                f'{self.base_url}/clients/{client}/instances/{instance}/documents/{document}/info'
            ).json()['documentReady'] is not True
        ):
            print(requests.get(
                f'{self.base_url}/clients/{client}/instances/{instance}/documents/{document}/info').json()['documentReady'])
            time.sleep(100)

    def get_document(self,  client: str, instance: str, base_document: str) -> str:
        data = {
            "format": "XLS",
            "deviceInfo": {
                "enableSearch": True,
                "BasePath": self.base_url
            },
            "useCache": True,
            "baseDocumentID": base_document,
        }

        doc_id = requests.post(
            f'{self.base_url}/clients/{client}/instances/{instance}/documents',
            headers=self.headers,
            json=data
        ).json()['documentId']

        return doc_id

    def download_data(self, client: str, instance: str, document: str):
        report = requests.get(
            f'{self.base_url}/clients/{client}/instances/{instance}/documents/{document}?response-content-disposition=attachment',
            headers={
                'origin': f'https://{config["shop_name"]}.pos365.vn',
                'referer': f'https://{config["shop_name"]}.pos365.vn/',
                'Content-Type': 'application/json; charset=UTF-8',
                'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
            },
        )
        with open(f"raw_sale_order.xlsx", 'wb') as f:
            f.write(report.content)

    def run(self):
        print(self.headers)
        print(config)
        client = self.get_client()
        print(f'client: {client}')
        self.set_param(client)
        instance = self.get_instances(client=client)
        print(f'instance: {instance}')
        base_document = self.get_base_document(client=client, instance=instance)
        print(f'base_document: {base_document}')
        self.confirm_document_ready(client, instance, base_document)
        document = self.get_document(client, instance, base_document)
        print(f'document: {document}')
        self.confirm_document_ready(client, instance, document)
        self.download_data(client=client, instance=instance, document=document)


if __name__ == '__main__':
    SaleOrderCrawler().run()
