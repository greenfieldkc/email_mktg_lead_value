import csv
import datetime
from transactions_db import TransactionsDatabase

class TransactionsFileParser:

    def __init__(self, transactions_csv):
        self.path = transactions_csv
        self.transactions = [] #holds list of transactions in form [email, price, date, order_id]
        self.get_transactions_from_file()

#reads the transactions file and populates the transactions instance variable
#transactions dict holds key=email, val=[date,price]
    def get_transactions_from_file(self):
        with open(self.path) as file:
            reader = csv.reader(file, delimiter=",")
            count = 0
            for row in reader:
                if count == 0:
                    count += 1
                else:
                    email = row[9]
                    date = row[0]
                    price = float(row[2])
                    order_id = row[1]
                    self.transactions.append([email, price, date, order_id])
                    count += 1

    def add_transactions_to_db(self, db):
        for t in self.transactions:
            db.create_transaction(t[0], t[1], t[2], t[3])






###----------> Testing <-----------------------------
if __name__ == '__main__':
    db = TransactionsDatabase('TRANSACTIONS DB PATH')
    test = TransactionsFileParser('TRANSACTIONS CSV FILE PATH')
    test.add_transactions_to_db(db)
    #db.print_transactions_table()
    #print(db.execute_read_query("SELECT COUNT (*) FROM transactions"))
