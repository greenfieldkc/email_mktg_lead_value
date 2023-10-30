import sqlite3
from sqlite3 import Error

class EmailDatabase:
    def __init__(self, path):
        self.path = path
        try:
            self.connection = sqlite3.connect(path)
            print("Email database connection successful.")
            self.create_users_table()
        except Error as e:
            print(f"The error {e} occurred.")



    def execute_query(self, query):
        cursor = self.connection.cursor()
        try:
            cursor.execute(query)
            self.connection.commit()
            #print("Query executed successfully.")
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

    def create_users_table(self):
        query = """
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            email TEXT NOT NULL UNIQUE,
            date_joined TEXT NOT NULL,
            lead_source TEXT,
            date_unsubscribed TEXT,
            lifetime_value FLOAT,
            daily_values TEXT
            );
        """
        self.execute_query(query)

    def create_user(self, email, date_joined, lead_source='None'):
        query = f"""
        INSERT INTO users
            (email, date_joined, lead_source)
        VALUES
            ('{email}', '{date_joined}', '{lead_source}');
        """
        self.execute_query(query)

    def update_user_date_unsubscribed(self, email, date_unsubscribed):
        query = f"""
            UPDATE users
            SET date_unsubscribed = '{date_unsubscribed}'
            WHERE email = '{email}';
        """
        self.execute_query(query)

    def update_lifetime_value(self, email, ltv):
        query = f"""
            UPDATE users
            SET lifetime_value = '{ltv}'
            WHERE email = '{email}';
        """
        self.execute_query(query)

    def update_daily_values(self, email, daily_values):
        query = f"""
            UPDATE users
            SET daily_values = '{daily_values}'
            WHERE email = '{email}';
        """
        self.execute_query(query)

    def get_daily_values_as_list(self, email):
        query = f"""
        SELECT
            daily_values
        FROM
            users
        WHERE
            email = '{email}'
        """
        result = self.execute_read_query(query)
        print(result)
        values = eval(result[0][0])
        return values

    def get_user_join_date(self, email):
        query = f"""
        SELECT
            date_joined
        FROM
            users
        WHERE
            email = '{email}'
        """
        return self.execute_read_query(query)[0][0]

    def get_user_lead_source(self, email):
        query = f"""
        SELECT
            lead_source
        FROM
            users
        WHERE
            email = '{email}'
        """
        return self.execute_read_query(query)[0][0]

    def get_user_data(self, email):
        query = f"""
        SELECT
            *
        FROM
            users
        WHERE
            email = '{email}'
        """
        return self.execute_read_query(query)

    def check_user_exists(self, email):
        query = f"""
        SELECT EXISTS (SELECT
            1
        FROM
            users
        WHERE
            email = '{email}')
        """
        try:
            if self.execute_read_query(query)[0][0] > 0:
                return True
            else:
                return False
        except:
            return False

    def print_users_table(self, max_count=20):
        count = 0
        results = self.execute_read_query("SELECT * FROM users")
        for user in results:
            print(f"User Id: {user[0]}, Email {user[1]}, Date Joined: {user[2]}, Date Unsubscribed: {user[4]}, Lead Source: {user[3]}, Lifetime Value: {user[5]}, Daily #Values: {user[6]}")
            count += 1
            if count >= max_count : break
