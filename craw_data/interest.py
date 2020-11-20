import bs4
import pandas as pd
import requests


def get_page_content(url):
    page = requests.get(url, headers={"Accept-Language": "en-US"})
    return bs4.BeautifulSoup(page.text, "html.parser")


def interest_cleaner(row: str):
    row = row.replace("%", "").replace(",", ".")
    return float(row) / 100


# VCB

vcb_url = "https://portal.vietcombank.com.vn/UserControls/TVPortal.TyGia/pListLaiSuat.aspx?CusstomType=1&BacrhID=1&InrateType=&isEn=False&numAfter=2"
vcb_res = get_page_content(vcb_url)

contexts = vcb_res.findAll("tr")
vcb_interest = []
vcb_interest.append(list(map(lambda x: x.text.strip(), contexts[0].findAll("th"))))
for i in contexts:
    vcb_interest.append(list(map(lambda x: x.text.strip(), i.findAll("td"))))
vcb_tiet_kiem = [i[:2] for i in vcb_interest[3:14]]
vcb_tiet_kiem_df = pd.DataFrame(vcb_tiet_kiem, columns=["ky_han", "lai_suat"])
vcb_tiet_kiem_df["loai_hinh"] = "tiet_kiem"

vcb_tien_gui_co_ky_han = [i[:2] for i in vcb_interest[15:]]
vcb_tien_gui_co_ky_han_df = pd.DataFrame(vcb_tien_gui_co_ky_han, columns=["ky_han", "lai_suat"])
vcb_tien_gui_co_ky_han_df["loai_hinh"] = "tien_gui_co_ky_han"
vcb_interest_df = vcb_tiet_kiem_df.append(vcb_tien_gui_co_ky_han_df, ignore_index=True)

vcb_interest_df["bank_issue"] = "vcb"
vcb_interest_df["lai_suat"] = vcb_interest_df["lai_suat"].apply(interest_cleaner)

# PVCOM BANK
pv_url = "https://www.pvcombank.com.vn/bieu-lai-suat.html"
pv_page = get_page_content(pv_url)
pv_interest_table = pv_page.findAll("div", id="frontend-content")
interest_line = pv_interest_table[0].findAll("tr")
pv_interest = []
for i in interest_line[1:]:
    tmp = i.text.split("\n")
    pv_interest.append([tmp[1], tmp[2]])

pv_interest_df = pd.DataFrame(pv_interest, columns=["ky_han", "lai_suat"])

pv_interest_df["bank_issue"] = "pvc"
pv_interest_df["lai_suat"] = pv_interest_df["lai_suat"].apply(interest_cleaner)
pv_interest_df["loai_hinh"] = None


# SCB
def get_scb_interest(url):
    scb_page = get_page_content(url)
    scb_interest_table = scb_page.findAll("tr")
    scb_interest = []
    for i in scb_interest_table:
        scb_interest.append(list(map(lambda x: x.text.strip(), i.findAll("td"))))
    return scb_interest


scb_ptl_url = (
    "https://www.scb.com.vn/vie/detaillist/qa_interestrate_detail/QA_INTERESTRATE_SAVING_PHATLOCTAI/0"  # phat tai loc
)
scb_tkt_url = "https://www.scb.com.vn/vie/detaillist/qa_interestrate_detail/QA_INTERESTRATE_NORMAL_SAVING_DEPOSIT/0"  # tiet kiem thuong
scb_online_url = (
    "https://www.scb.com.vn/vie/detaillist/qa_interestrate_detail/QA_INTERESTRATE_ONLINE_DEPOSIT/0"  # online
)
scb_tksh_url = "https://www.scb.com.vn/vie/detaillist/qa_interestrate_detail/QA_EFFICIENT_SAVINGS_AND_INSURANCE/0"  # tiet kiem song hanh - bao
scb_tkckh_rul = "https://www.scb.com.vn/vie/detaillist/qa_interestrate_detail/QA_INTERESTRATE_DEPOSIT_PERIOD_FOREIGNERS/0"  # tiet kiem co ky han co khach hang ca nhan

scb_ptl_interest = get_scb_interest(scb_ptl_url)
scb_tkt_interest = get_scb_interest(scb_tkt_url)
scb_tksh_interest = get_scb_interest(scb_tksh_url)
scb_online_interest = get_scb_interest(scb_online_url)
scb_tkckh_interest = get_scb_interest(scb_tkckh_rul)

scb_online_interest_df = pd.DataFrame(
    scb_online_interest[3:], columns=["ky_han", "linh_lai_truoc", "linh_hang_thang", "linh_cuoi_ky"]
)
scb_online_interest_df = scb_online_interest_df[["ky_han", "linh_cuoi_ky"]].rename(columns={"linh_cuoi_ky": "lai_suat"})
scb_online_interest_df["loai_hinh"] = "online"
scb_online_interest_df["bank_issue"] = "scb"
scb_online_interest_df["lai_suat"] = scb_online_interest_df["lai_suat"].apply(interest_cleaner)


def scb_ptl_term_cleaner(row: str):
    return str(int(row)) + " tháng"


scb_ptl_interest_df = pd.DataFrame(scb_ptl_interest[2:], columns=["ky_han", "linh_hang_thang", "linh_cuoi_ky"])

scb_ptl_interest_df = scb_ptl_interest_df[["ky_han", "linh_cuoi_ky"]].rename(columns={"linh_cuoi_ky": "lai_suat"})
scb_ptl_interest_df["loai_hinh"] = "phat_loc_tai"
scb_ptl_interest_df["bank_issue"] = "scb"
scb_ptl_interest_df["lai_suat"] = scb_ptl_interest_df["lai_suat"].apply(interest_cleaner)

scb_ptl_interest_df["ky_han"] = scb_ptl_interest_df["ky_han"].apply(scb_ptl_term_cleaner)
scb_interest_df = scb_online_interest_df.append(scb_ptl_interest_df, ignore_index=True)

# BIDV
bidv_url = "https://www.bidv.com.vn//ServicesBIDV/InterestDetailServlet"
bidv_res = requests.post(bidv_url)
bidv_hn_interest = pd.DataFrame(bidv_res.json()["hanoi"]["data"])

mapping_cols = {"title_vi": "ky_han", "VND": "lai_suat"}

bidv_hn_interest = bidv_hn_interest[["title_vi", "VND"]].rename(columns=mapping_cols)

bidv_hn_interest["loai_hinh"] = None
bidv_hn_interest["bank_issue"] = "bidv"
bidv_hn_interest["lai_suat"] = bidv_hn_interest["lai_suat"].apply(interest_cleaner)

# dong a
donga_interest_table = get_scb_interest(
    "https://kinhdoanh.dongabank.com.vn/widget/temp/-/DTSCDongaBankIView_WAR_DTSCDongaBankIERateportlet?type=tktt-vnd"
)
donga_interest_df = pd.DataFrame(donga_interest_table[3:], columns=["ky_han", "a", "b", "lai_suat"])[
    ["ky_han", "lai_suat"]
]
donga_interest_df["bank_issue"] = "donga"
donga_interest_df["loai_hinh"] = None

# nam a
nama_interest_table = get_scb_interest("https://www.namabank.com.vn//lai-suat-tien-gui-vnd-nam")
nama_interest_df = pd.DataFrame(nama_interest_table[3:], columns=["ky_han", "lai_suat", "a", "b", "c", "d"])[
    ["ky_han", "lai_suat"]
]
nama_interest_df["bank_issue"] = "nama"
nama_interest_df["loai_hinh"] = None

# final result
interest_df = (
    vcb_interest_df.append(pv_interest_df, ignore_index=True)
    .append(scb_interest_df, ignore_index=True)
    .append(bidv_hn_interest, ignore_index=True)
    .append(donga_interest_df, ignore_index=True)
    .append(nama_interest_df, ignore_index=True)
)
