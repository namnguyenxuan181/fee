from datetime import date
from typing import Dict
from dateutil.relativedelta import relativedelta
import pandas as pd
from .utils import RunDate
import os

base_path = os.path.dirname(os.path.realpath(__file__))
dim_date_path = os.path.join(base_path, "dim_date.csv")

default_dim_date = pd.read_csv(dim_date_path)


def month_add(origin_day: date, months: int, dim_date: pd.DataFrame = default_dim_date):
    origin_day = RunDate(origin_day)
    end_date = origin_day + relativedelta(months=months)
    while (
        dim_date[dim_date["date"] == int(end_date)]["is_holiday"].values[0]
        or dim_date[dim_date["date"] == int(end_date)]["is_weekend"].values[0]
    ):
        end_date += relativedelta(days=1)
    return end_date.to_date()


class Calculation:
    def __init__(
        self,
        bank_name: str,
        created_date: date,
        maturity: date,
        interest_rates: Dict[int, float],
        term: int,
        origin_amount: int,
        current_date: date,
        interest_type: int,
        default_interest_rate_info: pd.DataFrame,
    ):
        self.bank_name = bank_name
        self.created_date = created_date
        self.maturity = maturity
        self.interest_rates = interest_rates
        self.term = term
        self.origin_amount = origin_amount
        self.current_date = current_date
        self.interest_type = interest_type
        self.number_pass_term = 0
        self.maturity_dates = {0: self.maturity}
        self.begin_of_terms = {0: self.created_date}
        self.days_of_year = {0: (self.created_date + relativedelta(months=12) - self.created_date).days}
        self.seller_interest_rate = None
        self.buyer_interest_rate = None
        self.compute_info_for_each_term()
        self.fill_in_interest_rates(default_interest_rate_info)

    def compute_info_for_each_term(self):
        _end_of_term = self.maturity
        while _end_of_term < self.current_date:
            self.number_pass_term += 1
            _end_of_term = month_add(_end_of_term, months=self.term)
            _begin_of_term = self.maturity_dates[self.number_pass_term - 1]
            self.maturity_dates[self.number_pass_term] = _end_of_term
            self.begin_of_terms[self.number_pass_term] = _begin_of_term
            self.days_of_year[self.number_pass_term] = (_begin_of_term + relativedelta(months=12) - _begin_of_term).days

    def fill_in_interest_rates(self, default_interest_rate_info: pd.DataFrame):
        for i in range(0, self.number_pass_term + 1):
            if i not in self.interest_rates:
                self.interest_rates[i] = default_interest_rate_info[
                    (default_interest_rate_info["bank_name"] == self.bank_name)
                    & (default_interest_rate_info["date"] == self.begin_of_terms[i])
                    & (default_interest_rate_info["term"] == self.term)
                ]["interest_rate"].values[0]

    @property
    def days_of_last_year(self):
        return self.days_of_year[self.number_pass_term]

    @property
    def days_since_begin_of_last_term(self):
        return (self.current_date - self.begin_of_terms[self.number_pass_term]).days

    @property
    def days_to_next_maturity(self):
        return (self.maturity_dates[self.number_pass_term] - self.current_date).days

    @property
    def current_origin_amount(self):
        if self.interest_type == 2:
            current_origin_amount = self.origin_amount
            for i in range(0, self.number_pass_term):
                current_origin_amount = current_origin_amount * (
                    1
                    + self.interest_rates[i]
                    * (self.maturity_dates[i] - self.begin_of_terms[i]).days
                    / self.days_of_year[i]
                )
            return current_origin_amount
        else:
            return self.origin_amount

    @property
    def maturity_amount(self):
        return self.current_origin_amount * (
            1
            + self.interest_rates[self.number_pass_term]
            * (self.maturity_dates[self.number_pass_term] - self.begin_of_terms[self.number_pass_term]).days
            / self.days_of_last_year
        )

    @property
    def accrued_interest(self):
        return self.current_origin_amount * (
            self.interest_rates[self.number_pass_term] * self.days_since_begin_of_last_term / self.days_of_last_year
        )

    @property
    def seller_received_amount(self):
        return self.current_origin_amount * (
            1 + self.seller_interest_rate * self.days_since_begin_of_last_term / self.days_of_last_year
        )

    @property
    def buyer_payment_amount(self):
        return self.maturity_amount / (
            1 + self.buyer_interest_rate * self.days_to_next_maturity / self.days_of_last_year
        )

    @property
    def difference_amount(self):
        return self.buyer_payment_amount - self.seller_received_amount

    @property
    def difference_interest_rate(self):
        return self.difference_amount / self.current_origin_amount

    def compute_sell_and_buy_interest_info(
        self,
        difference_interest_rate: float,
        bank_transaction_fee: int,
        seller_interest_rate: float = None,
        buyer_interest_rate: float = None,
    ):
        d = difference_interest_rate + bank_transaction_fee / self.current_origin_amount
        n_days = self.days_of_year[self.number_pass_term]
        t = (self.maturity_dates[self.number_pass_term] - self.begin_of_terms[self.number_pass_term]).days
        l = self.interest_rates[self.number_pass_term]
        n = self.days_since_begin_of_last_term
        if not seller_interest_rate and not buyer_interest_rate:
            a = n * (t - n)
            b = n_days * t + n_days * d * t - n_days * d * n
            c = n_days ** 2 * d - n_days * l * t
            delta = b ** 2 - 4 * a * c
            y = (-b + delta ** 0.5) / (2 * a)

            self.seller_interest_rate = y
            self.buyer_interest_rate = y
        elif not seller_interest_rate and buyer_interest_rate:
            y = buyer_interest_rate
            b = n_days * l * t - n_days * y * (t - n) - n_days ** 2 * d - n_days * d * y * (t - n)
            a = n_days * n + y * n * (t - n)
            x = b / a
            self.seller_interest_rate = x
            self.buyer_interest_rate = y
        elif not buyer_interest_rate and seller_interest_rate:
            x = seller_interest_rate
            b = n_days * l * t - n_days * x * n - n_days ** 2 * d
            a = (t - n) * (n_days * d + n_days + x * n)
            y = b / a
            self.seller_interest_rate = x
            self.buyer_interest_rate = y
