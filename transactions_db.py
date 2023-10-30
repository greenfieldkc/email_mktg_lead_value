import sqlite3
from sqlite3 import Error


class TransactionsDatabase:
    def __init__(self, path):
        self.path = path
        try:
            self.connection = sqlite3.connect(path)
            print("Transactions database connection successful")
            self.create_transactions_table()
        except Error as e:
            print(f"The error {e} occurred.")

    def execute_query(self, query):
        cursor = self.connection.cursor()
        try:
            cursor.execute(query)
            self.connection.commit()
            print("Query executed successfully.")
        except Error as e:
            print(f"The error {e} occurred.")

    def execute_read_query(self, query):
        cursor = self.connection.cursor()
        result = None
        try:
            cursor.execute(query)
            result = cursor.fetchall()
            return result
        except Error as e:
            print(f"The error '{e}' occurred.")

    def create_transactions_table(self):
        query = """
        CREATE TABLE IF NOT EXISTS transactions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            email TEXT NOT NULL,
            order_id TEXT NOT NULL,
            price FLOAT NOT NULL,
            date TEXT NOT NULL
            );
        """
        self.execute_query(query)

    def create_transaction(self, email, price, date, order_id):
        query = f"""
        INSERT INTO transactions
            (email, price, date, order_id)
        VALUES
            ('{email}', '{price}', '{date}', '{order_id}');
        """
        if not self.check_transaction_exists(order_id, date):
            self.execute_query(query)

    def check_transaction_exists(self, order_id, date):
        query = f"""
            SELECT EXISTS (SELECT
                1
            FROM
                transactions
            WHERE
                order_id = '{order_id}' AND date = '{date}'
            )
        """
        if self.execute_read_query(query)[0][0] > 0:
            return True
        else:
            return False

    def print_transactions_table(self, max_count=20):
        print("printing transactions table...")
        count = 0
        results = self.execute_read_query("SELECT * FROM transactions")
        for t in results:
            print(f"Email: {t[1]}, Order ID: {t[2]}, Price: {t[3]}, Date: {t[4]}")
            count += 1
            if count >= max_count : break

    def get_user_transactions(self, email):
        query = f"""
            SELECT * FROM transactions WHERE email = '{email}'
        """
        return self.execute_read_query(query)

    def get_customer_list(self):
        query = """
            SELECT DISTINCT email FROM transactions"""
        return self.execute_read_query(query)
