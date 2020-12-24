import requests
from shop_config import config

def login():
    data = {
        'Username': '0982906045',
        'Password': '123',
        'loginsButton': 'Dashboard',
        '__RequestVerificationToken': 'CfDJ8IqAspSO0RNCtrJVYJZXIm3UfWQm4ziemO4zTysD6bRQqStlLNvs5q2G1zvOGZ1-Ac044R0cqKC1mHgMm89ePurSWycQvP0L9vL_8Wwz2qQjDp_elX3qQuXTBq9EEDFVXY1uxyvdE5Xq_5xqo4ngelQ'
    }
    login_status = requests.post(
        'https://yldemo.pos365.vn/Signin',
        headers={
            'origin': f'https://yldemo.pos365.vn',
            'referer': f'https://yldemo.pos365.vn/Signin',
        },
        json=data
    )
    print(login_status)
    print(login_status.cookies.get_dict())
    return login_status


def download_dispose_report():
    report = requests.get(
        'https://yldemo.pos365.vn/Export/RoomHistory?time=anytime',
        headers={
            'origin': f'https://{config["shop_name"]}.pos365.vn',
            'referer': f'https://{config["shop_name"]}.pos365.vn/',
            'Content-Type': 'application/json; charset=UTF-8',
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
        },
        cookies={
            'ss-id': 'TPBPmW9LxbhrB8yFo1QK',
            'ss-pid': 'TR3RhgmkDwFDShvwpc4W',
            'ss-opt': 'perm',
        }
    )

    # requests.api
    print(report.headers)
    with open(f"raw_dispose.xlsx", 'wb') as f:
        f.write(report.content)


if __name__ == '__main__':
    login_status = login()
    download_dispose_report()

