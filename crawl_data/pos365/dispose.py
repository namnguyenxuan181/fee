import requests
from shop_config import shop_config, shop_cookie

def download_dispose_report():
    report = requests.get(
        f'https://{shop_config["shop_name"]}.pos365.vn/Export/RoomHistory?time=7days',
        headers={
            'origin': f'https://{shop_config["shop_name"]}.pos365.vn',
            'referer': f'https://{shop_config["shop_name"]}.pos365.vn/',
            'Content-Type': 'application/json; charset=UTF-8',
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
        },
        cookies=shop_cookie
    )

    # requests.api
    print(report.headers)
    with open(f"raw_dispose.xlsx", 'wb') as f:
        f.write(report.content)


if __name__ == '__main__':
    download_dispose_report()

