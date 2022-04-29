"""
Python Script to Extract Post URL from Sitemap & Submit to an API
"""
import os
import csv
import json
import requests

from google.oauth2 import service_account
from googleapiclient.discovery import build


def save_urls_to_csv():
    """Save all URLs to be submitted to a CSV file."""
    try:
        filename = "PostURLs.csv"
        r = requests.get("https://squirrelarena.com/export.php")
        r.raise_for_status()
        urls = r.text.strip().split()
        with open(filename, mode="w", encoding="utf-8", newline="") as f:
            csv_writer = csv.writer(f)
            [csv_writer.writerow([url]) for url in urls]
    except requests.HTTPError as e:
        print(e)
    except Exception as e:
        print("ERROR: ", e)
    return None


def read_credentials(filename):
    """Read credential keys from CSV and return as dictionary."""
    keys = []
    full_path = os.path.join(os.getcwd(), filename)
    if os.path.exists(full_path):
        with open(full_path) as f:
            csv_reader = csv.reader(f)
            for row in csv_reader:
                key = json.loads(row[0].strip())
                keys.append(key)
        return keys
    else:
        return None


def read_csv(filename):
    """Read URLs from CSV and return as dictionary for fast access."""
    urls = {}
    full_path = os.path.join(os.getcwd(), filename)
    if os.path.exists(full_path):
        with open(full_path) as f:
            csv_reader = csv.reader(f)
            for row in csv_reader:
                urls[row[0]] = None
    return urls


def save_submitted_urls(filename, urls):
    """Save submitted URLs to CSV to avoiding duplicate submission."""
    with open(filename, "a", encoding="utf-8", newline="") as f:
        csv_writer = csv.writer(f)
        [csv_writer.writerow([url]) for url in urls]


def prepare_urls_for_submission(urls_to_be_submitted, submitted_urls):
    """Preare batch of 100 URLs to be submittted."""
    all_batches = []
    one_batch = []  # a list 100 URLs to be submitted using one key
    for url in urls_to_be_submitted:
        if url not in submitted_urls:
            one_batch.append(url)

        if len(one_batch) == 100:
            all_batches.append(one_batch)
            one_batch = []

    all_batches.append(one_batch)  # if there are URLs less than hundred at the end
    return all_batches


def insert_event(id, response, exception):
    if exception is not None:
        # print(exception)
        print("Failed.")
    else:
        # print(response)
        print("Success.")


def submit_urls_in_batch(urls, key):
    """Submite list of URLs to Goggle Index API in batch."""

    credentials = service_account.Credentials.from_service_account_info(key)
    service = build("indexing", "v3", credentials=credentials)
    batch = service.new_batch_http_request(callback=insert_event)

    for url in urls:
        batch.add(
            service.urlNotifications().publish(
                body={"url": url, "type": "URL_UPDATED"},
            )
        )
    batch.execute()

    # save submitted URLs
    filename = "SubmittedURLs.csv"
    save_submitted_urls(filename, urls)


save_urls_to_csv()

# Read URLs to be submitted from Posturls.csv
urls_to_be_submitted = read_csv("PostURLs.csv")

# Read submitted URLs from SubmittedURLs.csv
submitted_urls = read_csv("SubmittedURLs.csv")

# list of 100 credential keys
keys = read_credentials("API_KEYS.csv")

# list of URL batches for submission
urls = prepare_urls_for_submission(urls_to_be_submitted, submitted_urls)

i = 1
for batch, key in zip(urls, keys):
    print(f"Submitting URLs using key {i}...")
    submit_urls_in_batch(batch, key)
    i += 1
