import bs4
import pandas as pd
import requests
import datetime
from formulas.utils import RunDate


def get_page_content(url):
    page = requests.get(url, headers={"Accept-Language": "en-US"})
    return bs4.BeautifulSoup(page.text, "html.parser")


def interest_cleaner(row: str):
    if row == "-":
        return None
    row = row.replace("%", "").replace(",", ".")
    return round(float(row) / 100, 4)


def get_interest_from_table(url):
    page = get_page_content(url)
    interest_table = page.findAll("tr")
    interest = []
    for i in interest_table:
        interest.append(list(map(lambda x: x.text.strip(), i.findAll("td"))))
    return interest


def tenor_cleaner(row: str) -> (int, str):
    if row.lower() == "không kỳ hạn":
        return 0, None
    tenors = row.split()
    unit = tenors[1].lower()
    if unit == "ngày":
        unit = "day"
    elif unit == "tháng":
        unit = "month"
    elif unit == "tuần":
        unit = "week"
    return int(tenors[0]), unit


def get_vcb_interest():
    vcb_url = "https://portal.vietcombank.com.vn/UserControls/TVPortal.TyGia/pListLaiSuat.aspx?CusstomType=1&BacrhID=1&InrateType=&isEn=False&numAfter=2"
    vcb_interest = get_interest_from_table(vcb_url)
    vcb_tiet_kiem = [i[:2] for i in vcb_interest[2:13]]
    vcb_interest_df = pd.DataFrame(vcb_tiet_kiem, columns=["tenor", "interest"])
    vcb_interest_df['tenor'], vcb_interest_df['unit'] = zip(*vcb_interest_df["tenor"].apply(tenor_cleaner))
    vcb_interest_df["bank_issue"], vcb_interest_df["type"] = "vcb", "offline"
    vcb_interest_df["interest"] = vcb_interest_df["interest"].apply(interest_cleaner)
    return vcb_interest_df


# PVCOM BANK
def get_pvc_interest():
    pv_url = "https://www.pvcombank.com.vn/bieu-lai-suat.html"
    pv_page = get_page_content(pv_url)
    pv_interest_table = pv_page.findAll("div", id="frontend-content")
    interest_line = pv_interest_table[0].findAll("tr")
    pv_interest = []
    for i in interest_line[1:]:
        tmp = i.text.split("\n")
        pv_interest.append([tmp[1], tmp[2]])

    pv_interest_df = pd.DataFrame(pv_interest, columns=["tenor", "interest"])
    pv_interest_df['tenor'], pv_interest_df['unit'] = zip(*pv_interest_df["tenor"].apply(tenor_cleaner))

    pv_interest_df["bank_issue"], pv_interest_df["type"] = "pvc", 'offline'
    pv_interest_df["interest"] = pv_interest_df["interest"].apply(interest_cleaner)
    return pv_interest_df


# SCB
def get_scb_interest():
    scb_offline_url = (
        "https://www.scb.com.vn/vie/detaillist/qa_interestrate_detail/QA_INTERESTRATE_SAVING_PHATLOCTAI/0"
    )
    scb_online_url = (
        "https://www.scb.com.vn/vie/detaillist/qa_interestrate_detail/QA_INTERESTRATE_ONLINE_DEPOSIT/0"  # online
    )

    scb_offline_interest = get_interest_from_table(scb_offline_url)
    scb_online_interest = get_interest_from_table(scb_online_url)
    scb_online_interest_df = pd.DataFrame(
        scb_online_interest[4:], columns=["tenor", "_1", "_2", "interest"]
    )[['tenor', 'interest']]
    scb_online_interest_df["type"], scb_online_interest_df["bank_issue"]= "online", "scb"
    scb_online_interest_df["interest"] = scb_online_interest_df["interest"].apply(interest_cleaner)
    scb_online_interest_df['tenor'], scb_online_interest_df['unit'] = zip(
        *scb_online_interest_df["tenor"].apply(tenor_cleaner)
    )

    scb_offline_interest_df = pd.DataFrame(
        scb_offline_interest[2:], columns=["tenor", "_1", "interest"]
    )[['tenor', 'interest']]

    scb_offline_interest_df["type"], scb_offline_interest_df['unit'], scb_offline_interest_df["bank_issue"] = "offline", "month", "scb"
    scb_offline_interest_df["interest"] = scb_offline_interest_df["interest"].apply(interest_cleaner)
    scb_offline_interest_df["tenor"] = scb_offline_interest_df['tenor'].astype("int")
    scb_interest_df = scb_online_interest_df.append(scb_offline_interest_df, ignore_index=True)
    return scb_interest_df


# BIDV
def get_bid_interest():
    bidv_url = "https://www.bidv.com.vn//ServicesBIDV/InterestDetailServlet"
    bidv_res = requests.post(bidv_url)
    bidv_interest = pd.DataFrame(bidv_res.json()["hanoi"]["data"])

    mapping_cols = {"title_vi": "tenor", "VND": "interest"}

    bidv_interest = bidv_interest[["title_vi", "VND"]].rename(columns=mapping_cols)

    bidv_interest['tenor'], bidv_interest['unit'] = zip(*bidv_interest["tenor"].apply(tenor_cleaner))
    bidv_interest["interest"] = bidv_interest["interest"].apply(interest_cleaner)
    bidv_interest["type"], bidv_interest["bank_issue"] = "offline", "bidv"
    return bidv_interest


# dong a
def get_donga_interest():
    donga_interest_table = get_interest_from_table(
        "https://kinhdoanh.dongabank.com.vn/widget/temp/-/DTSCDongaBankIView_WAR_DTSCDongaBankIERateportlet?type=tktt-vnd"
    )
    donga_interest_df = pd.DataFrame(donga_interest_table[3:], columns=["tenor", "_1", "_2", "interest"])[
        ["tenor", "interest"]
    ]
    donga_interest_df['tenor'], donga_interest_df['unit'] = zip(*donga_interest_df["tenor"].apply(tenor_cleaner))
    donga_interest_df["interest"] = donga_interest_df["interest"].apply(interest_cleaner)
    donga_interest_df["bank_issue"], donga_interest_df["type"] = "donga", "offline"
    return donga_interest_df


# nam a
def get_nama_interest():
    nama_interest_table = get_interest_from_table("https://www.namabank.com.vn//lai-suat-tien-gui-vnd-nam")
    nama_interest_df = pd.DataFrame(nama_interest_table[3:], columns=["tenor", "interest", "_1", "_2", "_3", "_4"])[
        ["tenor", "interest"]
    ]
    nama_interest_df['tenor'], nama_interest_df['unit'] = zip(*nama_interest_df["tenor"].apply(tenor_cleaner))
    nama_interest_df["interest"] = nama_interest_df["interest"].apply(interest_cleaner)
    nama_interest_df["bank_issue"], nama_interest_df["type"] = "nama", "offline"
    return nama_interest_df


if __name__ == "__main__":
    print("start")
    run_date = RunDate(datetime.datetime.today())
    interest_df = (
        get_vcb_interest().append(get_pvc_interest(), ignore_index=True)
        .append(get_bid_interest(), ignore_index=True)
        .append(get_scb_interest(), ignore_index=True)
        .append(get_donga_interest(), ignore_index=True)
        .append(get_nama_interest(), ignore_index=True)
    )[['bank_issue', 'tenor', 'unit', 'type', 'interest']]
    interest_df.to_csv(f"interest/date={run_date}.csv", index=False)
    print("done")
