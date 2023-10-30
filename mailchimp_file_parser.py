import csv
import datetime


class MailchimpFileParser:

    def __init__(self, path):
        self.path = path

    def get_subscribers(self):
    #returns a dictionary of subscribers from file, with key=email, values=[join_date, lead_source]
        subscribers = {}
        with open(self.path) as file:
            reader = csv.reader(file, delimiter=",")
            count = 0
            for row in reader:
                if count == 0:
                    count += 1
                else:
                    email = row[0]
                    join_date = row[10][0:10]
                    lead_source = "None"
                    if ("FB" in row[25]):
                        lead_source = "Facebook"
                    subscribers[email] = [join_date, lead_source]
                    count +=1
        return subscribers

    def get_unsubscribes(self):
        unsubscribes = {}
        with open(self.path) as file:
            reader = csv.reader(file, delimiter=",")
            count = 0
            for row in reader:
                if count == 0:
                    count += 1
                else:
                    email = row[0]
                    join_date = row[10][0:10]
                    unsub_date = row[21][0:10]
                    lead_source = "None"
                    if 'cleaned' in self.path:
                        if ("FB" in row[27]):
                            lead_source = "Facebook"
                    if 'archived' in self.path:
                        join_date = row[11][0:10]
                        unsub_date = "2023-10-18"
                        lead_source = "None"
                        if ("FB" in row[26]):
                            lead_source = "Facebook"
                    try:
                        if ("FB" in row[29]):
                            lead_source = "Facebook"
                    except:
                            pass
                    unsubscribes[email] = [unsub_date, join_date, lead_source]
        return unsubscribes

    def get_archived_emails(self):
        archived_users = {}
        with open(self.path) as file:
            reader = csv.reader(file, delimiter=",")
            count = 0
            for row in reader:
                if count == 0:
                    count += 1
                else:
                    email = row[0]
                    join_date = row[11][0:10]
                    print(join_date)
                    unsub_date = "2023-10-18"
                    lead_source = "None"
                    if ("FB" in row[26]):
                        lead_source = "Facebook"
                    archived_users[email] = [unsub_date, join_date, lead_source]
        return archived_users
