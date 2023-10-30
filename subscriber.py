
import csv
import datetime
from thinkific_data import Thinkific



class Subscriber:
    def __init__(self, email, date_joined, lead_source=None):
        self.email = email
        self.date_joined = date_joined
        self.days_on_list = 1
        self.ltv = 0
        self.daily_values = {}
        self.subscribed = True
        self.date_unsubscribed = False
        self.days_until_first_purchase = None
        self.lead_source = lead_source

    def update(self, date, thinkific_data, mailchimp_unsubscribes):
        self.update_days_on_list(date)
        self.update_ltv(date, thinkific_data)
        self.update_daily_values(date)
        self.update_subscription_status(mailchimp_unsubscribes)
        self.update_customer_status()

    def update_days_on_list(self, date): #accepts string date in YYYY-MM-DD
        today = datetime.datetime.strptime(date, '%Y-%m-%d').date()
        join_date = datetime.datetime.strptime(self.date_joined, '%Y-%m-%d').date()
        self.days_on_list = (today - join_date).days

    def update_ltv(self, date, thinkific_data): #requires an updated thinkific_customers dict with thinkific export data from desired date
        if self.email in thinkific_data[date].customer_value:
            self.ltv = thinkific_data[date].customer_value[self.email]

    def update_daily_values(self, date):
        self.daily_values[self.days_on_list] = self.ltv

    def update_subscription_status(self, mailchimp_unsubscribes): #requires 2 dicts with email key and date value
        if self.email in mailchimp_unsubscribes:
            self.subscribed = False
            self.date_unsubscribed = mailchimp_unsubscribes[self.email]

    def update_customer_status(self):
        if self.days_until_first_purchase == 0 and self.ltv > 0:
            for i in self.daily_values:
                if self.daily_values[i] > 0 and self.days_until_first_purchase == 0:
                    self.days_until_first_purchase = i
