import bs4
from pyspark.sql import SparkSession, Window, functions as F
import pandas as pd
import requests
import datetime
from formulas.utils import RunDate


def get_page_content(url):
    page = requests.get(url, headers={"Accept-Language": "en-US"})
    return bs4.BeautifulSoup(page.text, "html.parser")


def interest_cleaner(row: str):
    row = str(row)
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
    map_unit = {
        "D": "day",
        "M": "month",
        "Y": "year",
    }
    for k, v in map_unit.items():
        if k in row:
            return int(row.replace(k, "")), v
    tenors = row.split()
    unit = tenors[1].lower()
    if unit == "ngày":
        unit = "day"
    elif unit == "tháng":
        unit = "month"
    elif unit == "tuần":
        unit = "week"
    return int(tenors[0]), unit


def add_from_amount_to_amount():
    return 0, 10e18-1


def get_vcb_interest():
    print('get vcb interest')
    vcb_url = "https://portal.vietcombank.com.vn/UserControls/TVPortal.TyGia/pListLaiSuat.aspx?CusstomType=1&BacrhID=1&InrateType=&isEn=False&numAfter=2"
    vcb_interest = get_interest_from_table(vcb_url)
    vcb_tiet_kiem = [i[:2] for i in vcb_interest[2:13]]
    vcb_interest_df = pd.DataFrame(vcb_tiet_kiem, columns=["tenor", "interest"])
    vcb_interest_df["tenor"], vcb_interest_df["unit"] = zip(*vcb_interest_df["tenor"].apply(tenor_cleaner))
    vcb_interest_df["bank_issue"], vcb_interest_df["type"] = "vcb", "offline"
    vcb_interest_df["interest"] = vcb_interest_df["interest"].apply(interest_cleaner)
    vcb_interest_df['from_amount'], vcb_interest_df['to_amount'] = add_from_amount_to_amount()
    vcb_interest_df['is_default'] = True
    return vcb_interest_df


# PVCOM BANK
def get_pvc_interest():
    print('get pvc interest')
    res = requests.get("https://www.pvcombank.com.vn//api/interest-rate")
    pvc_interest_table = pd.DataFrame(res.json())
    pvc_interest_table = pvc_interest_table.fillna(0).astype({"INTERESTE": "int64"})
    pvc_interest_table = pvc_interest_table.rename(
        columns={"INTERESTE": "interest", "TERM": "tenor", "CHANNEL": "type"}
    )
    pvc_interest_table["tenor"], pvc_interest_table["unit"] = zip(*pvc_interest_table["tenor"].apply(tenor_cleaner))
    pvc_interest_table["interest"] = pvc_interest_table["interest"].apply(interest_cleaner)
    select_cols = ["tenor", "unit", "interest", "type"]
    product_name = "Tiết kiệm Đại chúng trả lãi cuối kỳ"
    offline = "TELLER"
    online = "EBANK"
    pvc_interest_df = pvc_interest_table[
        (pvc_interest_table["CURRENCY"] == "VND") & (pvc_interest_table["PRODUCTNAME"] == product_name)
        & ((pvc_interest_table["type"] == offline) | (pvc_interest_table["type"] == online))
        ][select_cols]

    pvc_interest_df["bank_issue"] = "pvc"
    pvc_interest_df = pvc_interest_df.replace({online: "online", offline: "offline"})
    pvc_interest_df['from_amount'], pvc_interest_df['to_amount'] = add_from_amount_to_amount()
    pvc_interest_df['is_default'] = True
    return pvc_interest_df


# SCB
def get_scb_interest():
    print('get scb interest')
    scb_offline_url = "https://www.scb.com.vn/vie/detaillist/qa_interestrate_detail/QA_INTERESTRATE_SAVING_PHATLOCTAI/0"
    scb_online_url = (
        "https://www.scb.com.vn/vie/detaillist/qa_interestrate_detail/QA_INTERESTRATE_ONLINE_DEPOSIT/0"  # online
    )

    scb_offline_interest = get_interest_from_table(scb_offline_url)
    scb_online_interest = get_interest_from_table(scb_online_url)
    scb_online_interest_df = pd.DataFrame(scb_online_interest[4:], columns=["tenor", "_1", "_2", "interest"])[
        ["tenor", "interest"]
    ]
    scb_online_interest_df["type"], scb_online_interest_df["bank_issue"] = "online", "scb"
    scb_online_interest_df["interest"] = scb_online_interest_df["interest"].apply(interest_cleaner)
    scb_online_interest_df["tenor"], scb_online_interest_df["unit"] = zip(
        *scb_online_interest_df["tenor"].apply(tenor_cleaner)
    )

    scb_offline_interest_df = pd.DataFrame(scb_offline_interest[2:], columns=["tenor", "_1", "interest"])[
        ["tenor", "interest"]
    ]

    scb_offline_interest_df["type"], scb_offline_interest_df["unit"], scb_offline_interest_df["bank_issue"] = (
        "offline",
        "month",
        "scb",
    )
    scb_offline_interest_df["interest"] = scb_offline_interest_df["interest"].apply(interest_cleaner)
    scb_offline_interest_df["tenor"] = scb_offline_interest_df["tenor"].astype("int")
    scb_interest_df = scb_online_interest_df.append(scb_offline_interest_df, ignore_index=True)
    scb_interest_df['from_amount'], scb_interest_df['to_amount'] = add_from_amount_to_amount()
    scb_interest_df['is_default'] = True

    return scb_interest_df


# BIDV
def get_bid_interest():
    print('get bidv interest')
    bidv_url = "https://www.bidv.com.vn//ServicesBIDV/InterestDetailServlet"
    bidv_res = requests.post(bidv_url)
    bidv_interest = pd.DataFrame(bidv_res.json()["hanoi"]["data"])

    mapping_cols = {"title_vi": "tenor", "VND": "interest"}

    bidv_interest = bidv_interest[["title_vi", "VND"]].rename(columns=mapping_cols)

    bidv_interest["tenor"], bidv_interest["unit"] = zip(*bidv_interest["tenor"].apply(tenor_cleaner))
    bidv_interest["interest"] = bidv_interest["interest"].apply(interest_cleaner)
    bidv_interest["type"], bidv_interest["bank_issue"] = "offline", "bidv"
    bidv_interest['from_amount'], bidv_interest['to_amount'] = add_from_amount_to_amount()
    bidv_interest['is_default'] = True

    return bidv_interest


# dong a
def get_donga_interest():
    print('get dong a interest')
    donga_interest_table = get_interest_from_table(
        "https://kinhdoanh.dongabank.com.vn/widget/temp/-/DTSCDongaBankIView_WAR_DTSCDongaBankIERateportlet?type=tktt-vnd"
    )
    donga_interest_df = pd.DataFrame(donga_interest_table[3:], columns=["tenor", "_1", "_2", "interest"])[
        ["tenor", "interest"]
    ]
    donga_interest_df["tenor"], donga_interest_df["unit"] = zip(*donga_interest_df["tenor"].apply(tenor_cleaner))
    donga_interest_df["interest"] = donga_interest_df["interest"].apply(interest_cleaner)
    donga_interest_df["bank_issue"], donga_interest_df["type"] = "donga", "offline"
    donga_interest_df['from_amount'], donga_interest_df['to_amount'] = add_from_amount_to_amount()
    donga_interest_df['is_default'] = True

    return donga_interest_df


# nam a
def get_nama_interest():
    print('get nam a interest')
    nama_interest_table = get_interest_from_table("https://www.namabank.com.vn//lai-suat-tien-gui-vnd-nam")
    nama_interest_df = pd.DataFrame(nama_interest_table[3:], columns=["tenor", "interest", "_1", "_2", "_3", "_4"])[
        ["tenor", "interest"]
    ]
    nama_interest_df["tenor"], nama_interest_df["unit"] = zip(*nama_interest_df["tenor"].apply(tenor_cleaner))
    nama_interest_df["interest"] = nama_interest_df["interest"].apply(interest_cleaner)
    nama_interest_df["bank_issue"], nama_interest_df["type"] = "nama", "offline"
    nama_interest_df['from_amount'], nama_interest_df['to_amount'] = add_from_amount_to_amount()
    nama_interest_df['is_default'] = True

    return nama_interest_df


def get_vib_interest():
    print('get vib interest')
    spark = SparkSession.builder.getOrCreate()
    res = requests.post("https://www.vib.com.vn//VIBOpenCA/html/jsp/info/new_getSavingRateFromCore.jsp")
    vib_interest_table = pd.DataFrame(res.json())
    vib_interest_table = vib_interest_table.fillna(0).astype({"balance": "int64"})
    vib_interest_table = vib_interest_table.rename(
        columns={"balance": "to_amount", "actualRate": "interest", "term": "tenor", "productType": "type"}
    )
    vib_interest_table = vib_interest_table[
        (vib_interest_table["ccy"] == "VND")
        & ((vib_interest_table["type"] == "TDE") | (vib_interest_table["type"] == "524"))]
    vib_interest_table["tenor"], vib_interest_table["unit"] = zip(*vib_interest_table["tenor"].apply(tenor_cleaner))
    vib_interest_table["interest"] = vib_interest_table["interest"].apply(interest_cleaner)
    select_cols = ["tenor", "unit", "interest", "to_amount", "type"]
    w = Window.partitionBy("tenor").orderBy("to_amount")
    vib_interest_df = spark.createDataFrame(vib_interest_table[select_cols])
    vib_interest_df = (
        vib_interest_df.withColumn("from_amount", F.lag("to_amount").over(w))
        .withColumn("from_amount", F.col("from_amount") + 1).fillna(0, subset=['from_amount'])
        .withColumn(
            "type", F.when(F.col("type") == "TDE", "offline").otherwise("online")
        )
        .withColumn("bank_issue", F.lit("VIB"))
        .withColumn(
            "is_default", F.when(F.col("from_amount") == 0, True).otherwise(False)
        )
        .toPandas()
    )
    return vib_interest_df


if __name__ == "__main__":
    print("start")
    run_date = RunDate(datetime.datetime.today())
    interest_df = (
        get_vcb_interest()
        .append(get_pvc_interest(), ignore_index=True)
        .append(get_bid_interest(), ignore_index=True)
        .append(get_scb_interest(), ignore_index=True)
        # .append(get_donga_interest(), ignore_index=True)
        .append(get_nama_interest(), ignore_index=True)
        .append(get_vib_interest(), ignore_index=True)
    )[["bank_issue", "tenor", "unit", "type", "from_amount", "to_amount", "interest", 'is_default']]
    interest_df.to_csv(f"interest/date={run_date}.csv", index=False)
    print("done")
