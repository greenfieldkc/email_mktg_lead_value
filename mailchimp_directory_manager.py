import os
import shutil
from email_db import EmailDatabase
from mailchimp_file_parser import MailchimpFileParser


class MailchimpDirectoryManager:
    def __init__(self, db, unprocessed_subscriber_directory, processed_subscriber_directory, unprocessed_unsubscribed_directory, processed_unsubscribed_directory):
        self.db = db
        self.unprocessed_subscriber_directory = unprocessed_subscriber_directory
        self.unprocessed_unsubscribed_directory = unprocessed_unsubscribed_directory
        self.processed_subscriber_directory = processed_subscriber_directory
        self.processed_unsubscribed_directory = processed_unsubscribed_directory


    def process_subscriber_directory(self):
        for file in os.listdir(self.unprocessed_subscriber_directory):
            filepath = self.unprocessed_subscriber_directory + "/" + file
            parser = MailchimpFileParser(filepath)
            self.add_subscribers_to_db(parser.get_subscribers(), self.db)
            new_filepath = self.processed_subscriber_directory + "/" + file
            shutil.move(filepath, new_filepath)

    def process_unsubscribed_directory(self):
        for file  in os.listdir(self.unprocessed_unsubscribed_directory):
            filepath = self.unprocessed_unsubscribed_directory + "/" + file
            parser = MailchimpFileParser(filepath)
            self.add_update_unsubscribes_to_db(parser.get_unsubscribes(), self.db)
            new_filepath = self.processed_unsubscribed_directory + "/" + file
            shutil.move(filepath, new_filepath)

    def add_subscribers_to_db(self, subscribers, db): #receives a dictionary of subscribers from file, with key=email, values=[join_date, lead_source] and database connection object
        for user in subscribers:
            db.create_user(user, subscribers[user][0], subscribers[user][1])


    def add_update_unsubscribes_to_db(self, unsubscribes, db):
        for user in unsubscribes:
            if db.check_user_exists(user):
                db.update_user_date_unsubscribed(user, unsubscribes[user][0])
            else:
                db.create_user(user, unsubscribes[user][1], unsubscribes[user][2])
                db.update_user_date_unsubscribed(user, unsubscribes[user][0])
