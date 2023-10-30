import pandas as pd
import matplotlib.pyplot as plt
from email_db import EmailDatabase
from mailchimp_file_parser import MailchimpFileParser
from mailchimp_directory_manager import MailchimpDirectoryManager
from transactions_file_parser import TransactionsFileParser
from transactions_db import TransactionsDatabase
from datetime import date
from datetime import datetime



#---> Only need to run once to populate database
def run_db_update(email_db, transactions_db):
    mgr = MailchimpDirectoryManager(email_db,
        "ENTER DIRECTORY PATH FOR unprocessed_subscriber_files",
        "ENTER DIRECTORY PATH FOR processed_subscriber_files",
        "ENTER DIRECTORY PATH FOR unprocessed_unsubscribe_files",
        "ENTER DIRECTORY PATH FOR processed_unsubscribe_files"
        )
    mgr.process_subscriber_directory()
    mgr.process_unsubscribed_directory()
    add_customers_to_email_db(email_db, transactions_db)

def add_customers_to_email_db(email_db, transactions_db):
    customers = transactions_db.get_customer_list()
    for customer in customers:
        email = customer[0]
        if not email_db.check_user_exists(email):
            date_joined = transactions_db.get_user_transactions(email)[0][4]
            email_db.create_user(email, date_joined)

#----------------


#Import from database to DataFrame for analysis/plotting

def create_user_dataframe(db):
    sql_query = pd.read_sql_query("""SELECT * FROM users """,   db.connection)
    df = pd.DataFrame(sql_query)
    print(df.head)
    df["date_unsubscribed"] = pd.to_datetime(df['date_unsubscribed'])
    df["date_joined"] = pd.to_datetime(df['date_joined'])
    if not (df["date_unsubscribed"].empty):
        df["days_until_unsub"] = (df["date_unsubscribed"] - df["date_joined"]).dt.days
    df["days_since_signup"] = (pd.Timestamp.today().normalize() -   df["date_joined"]).dt.days
    return df

def create_transactions_dataframe(db):
    sql_query = pd.read_sql_query("""SELECT * FROM transactions """,   db.connection)
    df = pd.DataFrame(sql_query)
    return df






#returns a new dataframe with the email list churn data by day, calculating cumulative unsubscribes from the list and retention rate
def get_churn_data(df, num_days, lead_source=False):
    total_users = []            #the number of users where days since sign up >= that day
    cumulative_unsub = []       #the total number of unsubscribes on or befor that day
    if lead_source:
        for i in range(num_days):
            user_count = (df["days_since_signup"] >= i) & (df["lead_source"] == lead_source)
            total_users.append(len(df[user_count]))
            unsub_count = (df["days_until_unsub"] <= i) & (df["days_since_signup"] >= i) & (df["lead_source"] == lead_source)
            cumulative_unsub.append(len(df[unsub_count]))
    else:
        for i in range(num_days):
            user_count = df["days_since_signup"] >= i
            total_users.append(len(df[user_count]))
            unsub_count = (df["days_until_unsub"] <= i) & (df["days_since_signup"] >= i)
            cumulative_unsub.append(len(df[unsub_count]))
    churn_data = pd.DataFrame()
    churn_data["total_users"] = total_users
    churn_data["cumulative_unsub"] = cumulative_unsub
    churn_data["user_retention"] = (1 - churn_data["cumulative_unsub"]/churn_data["total_users"])*100
    return churn_data




#-----------> comparing data for plotting <--------------------
#returns a dataframe to compare list churn for all_users vs facebook_ads leads
def compare_churn_by_lead_source(users_df, num_days):
    pass
    compare = pd.DataFrame()
    compare["all_users"] = get_churn_data(users_df, num_days)["user_retention"]
    compare["facebook_ads"] = get_churn_data(users_df, num_days, "Facebook")["user_retention"]
    return compare

#opens a plot of email list retention using the compare_churn_by_lead_source function
def plot_user_retention_by_lead_source(users_df, num_days):
    temp = compare_churn_by_lead_source(users_df, num_days)
    all_users = plt.plot(temp["all_users"], label="All Users", color="green")
    facebook_ads = plt.plot(temp["facebook_ads"], label="Facebook Ads", color="blue")
    leg = plt.legend(loc="upper center")
    plt.xlabel("Days on List")
    plt.ylabel("Retention (%)")
    plt.show()

def plot_user_value_by_lead_source(email_db, transactions_db, users_df, num_days):
    temp = calculate_average_daily_values(email_db, transactions_db, users_df, num_days)
    temp2 = calculate_average_daily_values(email_db, transactions_db, users_df, num_days, lead_source="Facebook")
    all_users = plt.plot(temp["average_value"], label="All Users", color="green")
    facebook_ads = plt.plot(temp2["average_value"], label="Facebook Ads", color="blue")
    leg = plt.legend(loc="upper center")
    plt.xlabel("Days on List")
    plt.ylabel("Average Lead Value ($)")
    plt.show()

###---------------------------------------------------




###---------------------------------------------------
### Calculate User daily values and ltv from Transactions and update db
#takes the users dataframe, transactions dataframe, and user email
def calculate_user_daily_values(users, transactions_db, email):
    daily_values = [0]
    join_date = users.loc[users['email'] == email].iloc[0]['date_joined']
    print(join_date)
    days_since_signup = users.loc[users['email'] == email].iloc[0]['days_since_signup']
    user_transactions = pd.DataFrame(transactions_db.get_user_transactions(email))
    user_transactions.columns = ["transaction_id", "email", "order_id", "price", "date"]
    user_transactions['date'] = pd.to_datetime(user_transactions['date'])
    user_transactions.sort_values('date')
    user_transactions = user_transactions.groupby(['date']).sum().reset_index()
    index = 0
    for i in user_transactions.index:
        transaction_date = user_transactions.iloc[i]["date"]
        transaction_price = user_transactions.iloc[i]["price"]
        if (transaction_date - join_date).days == 0:
            daily_values[0] = transaction_price
            index += 1
        else:
            while (transaction_date - join_date).days > index:
                if index != 0 : daily_values.append(daily_values[index - 1])
                index += 1
            if (transaction_date - join_date).days == index:
                daily_values.append(daily_values[index - 1] + transaction_price)
                index += 1
    while days_since_signup >= index:
        if index == 0:
            daily_values.append(daily_values[index])
        else:
            daily_values.append(daily_values[index-1])
        index += 1
    return daily_values



#update the user email database with daily_values and lifetime_value
def update_user_daily_values(users_df, email_db, transactions_db):
    for customer in transactions_db.get_customer_list():
        email = customer[0]
        if email_db.check_user_exists(email):
            daily_values = calculate_user_daily_values(users_df, transactions_db, email)
            email_db.update_daily_values(email, daily_values)
            email_db.update_lifetime_value(email, daily_values[-1])

def create_daily_values_df(email_db, transactions_db, lead_source=False):
    update_user_daily_values(users_df, email_db, transactions_db)
    df = pd.DataFrame()
    count = 0
    customers = transactions_db.get_customer_list()
    if lead_source:
        for customer in customers:
            source = email_db.get_user_lead_source(customer[0])
            if source != lead_source : customers.remove(customer)
    for customer in customers:
        values = pd.DataFrame(email_db.get_daily_values_as_list(customer[0]))
        df = df.add(values, fill_value=0)
    return df


def calculate_average_daily_values(email_db, transactions_db, users_df, num_days, lead_source=False):
    daily_values = pd.DataFrame()
    values_df = create_daily_values_df(email_db, transactions_db, lead_source).head(num_days)
    print(daily_values.head)
    daily_values['total_value'] = values_df
    total_users = []
    if lead_source:
        for i in range(num_days):
            user_count = (users_df["days_since_signup"] >= i) & (users_df["lead_source"] == lead_source)
            total_users.append(len(users_df[user_count]))
    else:
        for i in range(num_days):
            user_count = users_df["days_since_signup"] >= i
            total_users.append(len(users_df[user_count]))
    daily_values['total_users'] = total_users
    daily_values['average_value'] = daily_values['total_value'] / daily_values['total_users']
    return daily_values




###----------> Main Execution Thread <------------------
email_db = EmailDatabase('EMAIL DB PATH')
transactions_db = TransactionsDatabase('TRANSACTIONS DB PATH')
run_db_update(email_db, transactions_db)
users_df = create_user_dataframe(email_db)
transactions_df = create_transactions_dataframe(transactions_db)
#update_user_daily_values(users_df, email_db, transactions_db)
print ("----")
print ("----")
print ("----")

#plot_user_retention_by_lead_source(users_df, 1200)
plot_user_value_by_lead_source(email_db, transactions_db, users_df, 1200)
