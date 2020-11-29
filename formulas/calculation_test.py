from datetime import date
import pandas as pd
from .calculation import Calculation, month_add


BANK_NAME = "VCB"
default_interest_rate_info = pd.DataFrame(
    data=[
        ("VCB", date(2020, 12, 30), 12, 0.068),
        ("VCB", date(2020, 12, 31), 12, 0.068),
        ("VCB", date(2021, 1, 1), 12, 0.068),
        ("VCB", date(2021, 1, 2), 12, 0.068),
        ("VCB", date(2021, 1, 3), 12, 0.068),
        ("VCB", date(2021, 1, 4), 12, 0.068),
        ("VCB", date(2021, 1, 5), 12, 0.068),
    ],
    columns=["bank_name", "date", "term", "interest_rate"],
)


def test_month_add():
    test_cases = [
        {"name": "is not holiday", "day": date(2019, 12, 30), "months": 12, "expected": date(2020, 12, 30),},
        {"name": "is holiday", "day": date(2020, 1, 1), "months": 12, "expected": date(2021, 1, 4),},
    ]
    for test in test_cases:
        actual = month_add(test["day"], test["months"])
        assert actual == test["expected"], test["name"]


def test_compute_info_for_each_term():
    test_cases = [
        {
            "name": "happy case",
            "created_date": date(2019, 12, 30),
            "term": 12,
            "maturity": date(2020, 12, 30),
            "current_date": date(2020, 10, 1),
            "origin_amount": 100000000,
            "interest_rates": {0: 0.072},  # 7.2%
            "interest_type": 2,
            "default_interest_rate_info": default_interest_rate_info,
            "expected": {
                "number_pass_term": 0,
                "maturity_dates": {0: date(2020, 12, 30)},
                "begin_of_terms": {0: date(2019, 12, 30)},
                "days_of_year": {0: 366},
                "interest_rates": {0: 0.072},
            },
        },
        {
            "name": "pass a term",
            "created_date": date(2019, 12, 31),
            "term": 12,
            "maturity": date(2020, 12, 31),
            "current_date": date(2021, 9, 1),
            "origin_amount": 100000000,
            "interest_rates": {0: 0.072},  # 7.2%
            "interest_type": 2,
            "default_interest_rate_info": default_interest_rate_info,
            "expected": {
                "number_pass_term": 1,
                "maturity_dates": {0: date(2020, 12, 31), 1: date(2022, 1, 4)},
                "begin_of_terms": {0: date(2019, 12, 31), 1: date(2020, 12, 31)},
                "days_of_year": {0: 366, 1: 365},
                "interest_rates": {0: 0.072, 1: 0.068},
            },
        },
    ]

    for test in test_cases:
        calculation = Calculation(
            bank_name=BANK_NAME,
            created_date=test["created_date"],
            maturity=test["maturity"],
            interest_rates=test["interest_rates"],
            term=test["term"],
            origin_amount=test["origin_amount"],
            current_date=test["current_date"],
            interest_type=test["interest_type"],
            default_interest_rate_info=test["default_interest_rate_info"],
        )
        assert calculation.number_pass_term == test["expected"]["number_pass_term"], (
            test["name"] + " in number_pass_term"
        )
        assert calculation.maturity_dates == test["expected"]["maturity_dates"], test["name"] + " in maturity_dates"
        assert calculation.begin_of_terms == test["expected"]["begin_of_terms"], test["name"] + " in begin_of_terms"
        assert calculation.days_of_year == test["expected"]["days_of_year"], test["name"] + " in days_of_year"
        assert calculation.interest_rates == test["expected"]["interest_rates"], test["name"] + " in interest_rates"


def test_days_since_begin_of_last_term():
    test_cases = [
        {
            "name": "happy case",
            "created_date": date(2019, 12, 31),
            "term": 12,
            "maturity": date(2020, 12, 31),
            "current_date": date(2020, 10, 1),
            "origin_amount": 100000000,
            "interest_rates": {0: 0.072},  # 7.2%
            "interest_type": 2,
            "default_interest_rate_info": default_interest_rate_info,
            "expected": 275,
        },
        {
            "name": "pass a term",
            "created_date": date(2019, 12, 31),
            "term": 12,
            "maturity": date(2020, 12, 31),
            "current_date": date(2021, 9, 1),
            "origin_amount": 100000000,
            "interest_rates": {0: 0.072},  # 7.2%
            "interest_type": 2,
            "default_interest_rate_info": default_interest_rate_info,
            "expected": 244,
        },
    ]
    for test in test_cases:
        calculation = Calculation(
            bank_name=BANK_NAME,
            created_date=test["created_date"],
            maturity=test["maturity"],
            interest_rates=test["interest_rates"],
            term=test["term"],
            origin_amount=test["origin_amount"],
            current_date=test["current_date"],
            interest_type=test["interest_type"],
            default_interest_rate_info=test["default_interest_rate_info"],
        )
        assert calculation.days_since_begin_of_last_term == test["expected"], test["name"]


def test_days_to_next_maturity():
    test_cases = [
        {
            "name": "happy case",
            "created_date": date(2019, 12, 31),
            "term": 12,
            "maturity": date(2020, 12, 31),
            "current_date": date(2020, 10, 1),
            "origin_amount": 100000000,
            "interest_rates": {0: 0.072},  # 7.2%
            "interest_type": 2,
            "default_interest_rate_info": default_interest_rate_info,
            "expected": 91,
        },
        {
            "name": "pass a term",
            "created_date": date(2019, 12, 31),
            "term": 12,
            "maturity": date(2020, 12, 31),
            "current_date": date(2021, 9, 1),
            "origin_amount": 100000000,
            "interest_rates": {0: 0.072},  # 7.2%
            "interest_type": 2,
            "default_interest_rate_info": default_interest_rate_info,
            "expected": 125,
        },
    ]
    for test in test_cases:
        calculation = Calculation(
            bank_name=BANK_NAME,
            created_date=test["created_date"],
            maturity=test["maturity"],
            interest_rates=test["interest_rates"],
            term=test["term"],
            origin_amount=test["origin_amount"],
            current_date=test["current_date"],
            interest_type=test["interest_type"],
            default_interest_rate_info=test["default_interest_rate_info"],
        )
        assert calculation.days_to_next_maturity == test["expected"], test["name"]


def test_current_origin_amount():
    test_cases = [
        {
            "name": "happy case",
            "created_date": date(2019, 12, 31),
            "term": 12,
            "maturity": date(2020, 12, 31),
            "current_date": date(2020, 10, 1),
            "origin_amount": 100000000,
            "interest_rates": {0: 0.072},  # 7.2%
            "interest_type": 2,
            "default_interest_rate_info": default_interest_rate_info,
            "expected": 100000000,
        },
        {
            "name": "pass a term",
            "created_date": date(2019, 12, 30),
            "term": 12,
            "maturity": date(2020, 12, 30),
            "current_date": date(2021, 9, 1),
            "origin_amount": 100000000,
            "interest_rates": {0: 0.072},  # 7.2%
            "interest_type": 2,
            "default_interest_rate_info": default_interest_rate_info,
            "expected": 107200000,
        },
        {
            "name": "pass a term, but interest_type=1",
            "created_date": date(2019, 12, 31),
            "term": 12,
            "maturity": date(2020, 12, 31),
            "current_date": date(2021, 9, 1),
            "origin_amount": 100000000,
            "interest_rates": {0: 0.072},  # 7.2%
            "interest_type": 1,
            "default_interest_rate_info": default_interest_rate_info,
            "expected": 100000000,
        },
    ]
    for test in test_cases:
        calculation = Calculation(
            bank_name=BANK_NAME,
            created_date=test["created_date"],
            maturity=test["maturity"],
            interest_rates=test["interest_rates"],
            term=test["term"],
            origin_amount=test["origin_amount"],
            current_date=test["current_date"],
            interest_type=test["interest_type"],
            default_interest_rate_info=test["default_interest_rate_info"],
        )
        assert calculation.current_origin_amount == test["expected"], test["name"]


def test_maturity_amount():
    test_cases = [
        {
            "name": "happy case",
            "created_date": date(2019, 12, 31),
            "term": 12,
            "maturity": date(2020, 12, 31),
            "current_date": date(2020, 10, 1),
            "origin_amount": 100000000,
            "interest_rates": {0: 0.072},  # 7.2%
            "interest_type": 2,
            "default_interest_rate_info": default_interest_rate_info,
            "expected": 107200000,
        },
        {
            "name": "pass a term",
            "created_date": date(2019, 12, 31),
            "term": 12,
            "maturity": date(2020, 12, 31),
            "current_date": date(2021, 9, 1),
            "origin_amount": 100000000,
            "interest_rates": {0: 0.072},  # 7.2%
            "interest_type": 2,
            "default_interest_rate_info": default_interest_rate_info,
            "expected": 114569486,
        },
        {
            "name": "pass a term, but interest_type=1",
            "created_date": date(2019, 12, 30),
            "term": 12,
            "maturity": date(2020, 12, 30),
            "current_date": date(2021, 8, 31),
            "origin_amount": 100000000,
            "interest_rates": {0: 0.072},  # 7.2%
            "interest_type": 1,
            "default_interest_rate_info": default_interest_rate_info,
            "expected": 106800000,
        },
    ]
    for test in test_cases:
        calculation = Calculation(
            bank_name=BANK_NAME,
            created_date=test["created_date"],
            maturity=test["maturity"],
            interest_rates=test["interest_rates"],
            term=test["term"],
            origin_amount=test["origin_amount"],
            current_date=test["current_date"],
            interest_type=test["interest_type"],
            default_interest_rate_info=test["default_interest_rate_info"],
        )
        assert round(calculation.maturity_amount) == test["expected"], test["name"]


def test_accrued_interest():
    test_cases = [
        {
            "name": "happy case",
            "created_date": date(2019, 12, 31),
            "term": 12,
            "maturity": date(2020, 12, 31),
            "current_date": date(2020, 10, 1),
            "origin_amount": 100000000,
            "interest_rates": {0: 0.072},  # 7.2%
            "interest_type": 2,
            "default_interest_rate_info": default_interest_rate_info,
            "expected": 5409836,
        },
        {
            "name": "pass a term",
            "created_date": date(2019, 12, 30),
            "term": 12,
            "maturity": date(2020, 12, 30),
            "current_date": date(2021, 8, 31),
            "origin_amount": 100000000,
            "interest_rates": {0: 0.072},  # 7.2%
            "interest_type": 2,
            "default_interest_rate_info": default_interest_rate_info,
            "expected": 4873048,
        },
        {
            "name": "pass a term, but interest_type=1",
            "created_date": date(2019, 12, 30),
            "term": 12,
            "maturity": date(2020, 12, 30),
            "current_date": date(2021, 8, 31),
            "origin_amount": 100000000,
            "interest_rates": {0: 0.072},  # 7.2%
            "interest_type": 1,
            "default_interest_rate_info": default_interest_rate_info,
            "expected": 4545753,
        },
    ]
    for test in test_cases:
        calculation = Calculation(
            bank_name=BANK_NAME,
            created_date=test["created_date"],
            maturity=test["maturity"],
            interest_rates=test["interest_rates"],
            term=test["term"],
            origin_amount=test["origin_amount"],
            current_date=test["current_date"],
            interest_type=test["interest_type"],
            default_interest_rate_info=test["default_interest_rate_info"],
        )
        assert round(calculation.accrued_interest) == test["expected"], test["name"]


def test_seller_received_amount():
    test_cases = [
        {
            "name": "happy case",
            "created_date": date(2019, 12, 31),
            "term": 12,
            "maturity": date(2020, 12, 31),
            "current_date": date(2020, 10, 1),
            "origin_amount": 100000000,
            "interest_rates": {0: 0.072},  # 7.2%
            "interest_type": 2,
            "seller_interest_rate": 0.06,
            "default_interest_rate_info": default_interest_rate_info,
            "expected": 104508197,
        },
        {
            "name": "pass a term",
            "created_date": date(2019, 12, 31),
            "term": 12,
            "maturity": date(2020, 12, 31),
            "current_date": date(2021, 9, 1),
            "origin_amount": 100000000,
            "interest_rates": {0: 0.072},  # 7.2%
            "interest_type": 2,
            "seller_interest_rate": 0.06,
            "default_interest_rate_info": default_interest_rate_info,
            "expected": 111499748,
        },
    ]
    for test in test_cases:
        calculation = Calculation(
            bank_name=BANK_NAME,
            created_date=test["created_date"],
            maturity=test["maturity"],
            interest_rates=test["interest_rates"],
            term=test["term"],
            origin_amount=test["origin_amount"],
            current_date=test["current_date"],
            interest_type=test["interest_type"],
            default_interest_rate_info=test["default_interest_rate_info"],
        )
        calculation.seller_interest_rate = test["seller_interest_rate"]
        assert round(calculation.seller_received_amount) == test["expected"], test["name"]


def test_buyer_payment_amount():
    test_cases = [
        {
            "name": "happy case",
            "created_date": date(2019, 12, 31),
            "term": 12,
            "maturity": date(2020, 12, 31),
            "current_date": date(2020, 10, 1),
            "origin_amount": 100000000,
            "interest_rates": {0: 0.072},  # 7.2%
            "interest_type": 2,
            "buyer_interest_rate": 0.06,
            "default_interest_rate_info": default_interest_rate_info,
            "expected": 105624293,
        },
        {
            "name": "pass a term",
            "created_date": date(2019, 12, 30),
            "term": 12,
            "maturity": date(2020, 12, 30),
            "current_date": date(2021, 8, 31),
            "origin_amount": 100000000,
            "interest_rates": {0: 0.072},  # 7.2%
            "interest_type": 2,
            "buyer_interest_rate": 0.06,
            "default_interest_rate_info": default_interest_rate_info,
            "expected": 112256767,
        },
    ]
    for test in test_cases:
        calculation = Calculation(
            bank_name=BANK_NAME,
            created_date=test["created_date"],
            maturity=test["maturity"],
            interest_rates=test["interest_rates"],
            term=test["term"],
            origin_amount=test["origin_amount"],
            current_date=test["current_date"],
            interest_type=test["interest_type"],
            default_interest_rate_info=test["default_interest_rate_info"],
        )
        calculation.buyer_interest_rate = test["buyer_interest_rate"]
        assert round(calculation.buyer_payment_amount) == test["expected"], test["name"]


def test_compute_sell_and_buy_interest_info():
    test_cases = [
        {
            "name": "happy case",
            "created_date": date(2019, 12, 31),
            "term": 12,
            "maturity": date(2020, 12, 31),
            "current_date": date(2020, 10, 1),
            "origin_amount": 100000000,
            "bank_transaction_fee": 50000,
            "interest_rates": {0: 0.072},  # 7.2%
            "interest_type": 2,
            "difference_interest_rate": 0.0077,
            "buyer_interest_rate": None,
            "seller_interest_rate": None,
            "default_interest_rate_info": default_interest_rate_info,
            "expected": {"buyer_interest_rate": 0.0629, "seller_interest_rate": 0.0629,},
        },
        {
            "name": "pass a term",
            "created_date": date(2019, 12, 31),
            "term": 12,
            "maturity": date(2020, 12, 31),
            "current_date": date(2021, 9, 1),
            "origin_amount": 100000000,
            "bank_transaction_fee": 50000,
            "interest_rates": {0: 0.072},  # 7.2%
            "interest_type": 2,
            "difference_interest_rate": 0.0077,
            "buyer_interest_rate": None,
            "seller_interest_rate": None,
            "default_interest_rate_info": default_interest_rate_info,
            "expected": {"buyer_interest_rate": 0.059, "seller_interest_rate": 0.059,},
        },
        {
            "name": "know seller_interest_rate",
            "created_date": date(2019, 12, 31),
            "term": 12,
            "maturity": date(2020, 12, 31),
            "current_date": date(2020, 10, 1),
            "origin_amount": 100000000,
            "bank_transaction_fee": 50000,
            "interest_rates": {0: 0.072},  # 7.2%
            "interest_type": 2,
            "difference_interest_rate": 0.0077,
            "buyer_interest_rate": None,
            "seller_interest_rate": 0.064,
            "default_interest_rate_info": default_interest_rate_info,
            "expected": {"buyer_interest_rate": 0.0598, "seller_interest_rate": 0.064,},
        },
        {
            "name": "know buyer_interest_rate",
            "created_date": date(2019, 12, 31),
            "term": 12,
            "maturity": date(2020, 12, 31),
            "current_date": date(2020, 10, 1),
            "origin_amount": 100000000,
            "bank_transaction_fee": 50000,
            "interest_rates": {0: 0.072},  # 7.2%
            "interest_type": 2,
            "difference_interest_rate": 0.0077,
            "buyer_interest_rate": 0.072,
            "seller_interest_rate": None,
            "default_interest_rate_info": default_interest_rate_info,
            "expected": {"buyer_interest_rate": 0.072, "seller_interest_rate": 0.0598,},
        },
    ]
    for test in test_cases:
        calculation = Calculation(
            bank_name=BANK_NAME,
            created_date=test["created_date"],
            maturity=test["maturity"],
            interest_rates=test["interest_rates"],
            term=test["term"],
            origin_amount=test["origin_amount"],
            current_date=test["current_date"],
            interest_type=test["interest_type"],
            default_interest_rate_info=test["default_interest_rate_info"],
        )
        calculation.compute_sell_and_buy_interest_info(
            test["difference_interest_rate"],
            test["bank_transaction_fee"],
            test["seller_interest_rate"],
            test["buyer_interest_rate"],
        )
        print(calculation.difference_amount)
        assert round(calculation.buyer_interest_rate, 4) == test["expected"]["buyer_interest_rate"], test["name"]
        assert round(calculation.seller_interest_rate, 4) == test["expected"]["seller_interest_rate"], test["name"]
