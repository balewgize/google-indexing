"""
Python Script to Extract Post URL from Sitemap & Submit to an API
"""
import os
import csv
import json
import time
import requests
from bs4 import BeautifulSoup

from google.oauth2 import service_account
from googleapiclient.discovery import build


def save_urls_to_csv(url):
    """Save all URLs to be submitted to a CSV file."""
    print("Fetching URLs from the website...")
    try:
        filename = "PostURLs.csv"
        r = requests.get(url)
        r.raise_for_status()

        soup = BeautifulSoup(r.content, "html.parser")
        site_maps = soup.find_all("loc")
        for s in site_maps:
            try:
                url = s.text.strip()
                r = requests.get(url)
                r.raise_for_status()

                soup = BeautifulSoup(r.content, "html.parser")
                urls = soup.find_all("loc")

                with open(filename, mode="a", encoding="utf-8", newline="") as f:
                    csv_writer = csv.writer(f)
                    [csv_writer.writerow([url.text.strip()]) for url in urls]

            except requests.HTTPError as e:
                print(e)
            except Exception as e:
                print(f"Error while scraping sitemap {url}: {e}")
    except requests.HTTPError as e:
        print(e)
    except Exception as e:
        print(f"Error while saving URLs to CSV {e}")
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

    if one_batch:
        all_batches.append(one_batch)  # if there are URLs less than hundred at the end
    return all_batches


def insert_event(id, response, exception):
    if exception is not None:
        print("FAILED: ", exception)
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


url = "https://nofly90.com/export.php"

while True:
    save_urls_to_csv(url)

    # Read URLs to be submitted from Posturls.csv
    urls_to_be_submitted = read_csv("PostURLs.csv")

    # Read submitted URLs from SubmittedURLs.csv
    submitted_urls = read_csv("SubmittedURLs.csv")

    # list of 100 credential keys
    keys = read_credentials("API_KEYS.csv")

    # list of URL batches for submission
    urls = prepare_urls_for_submission(urls_to_be_submitted, submitted_urls)

    i = 0
    counter = 1

    for key in keys:
        pair_batches = urls[i : i + 2]

        if pair_batches != []:
            if len(pair_batches) == 2:
                first_batch, second_batch = pair_batches[:]
            else:
                first_batch = pair_batches[0]
                second_batch = []

            # submit the first 100 URLs using the key
            if first_batch:
                print(f"\nSubmitting URLs using KEY {counter:02}...")
                submit_urls_in_batch(first_batch, key)
                print(f"{counter:02} Waiting for 7.5 minutes...")
                time.sleep(450)  # wait 7.5 minutes (7.5 * 60 = 450 seconds)

            # submit the second 100 URLs using the key
            if second_batch:
                submit_urls_in_batch(second_batch, key)
                print(f"KEY {counter:02} fully used.\n")
                print(f"{counter:02} Waiting for 7.5 minutes...")
                time.sleep(450)  # wait 7.5 minutes before moving to the next key

            # daily quota for one key (200 submission) is reached, move to the next key
            counter += 1
            i += 2
        else:
            # all urls are submitted, check the website again
            save_urls_to_csv(url)
            urls_to_be_submitted = read_csv("PostURLs.csv")
            submitted_urls = read_csv("SubmittedURLs.csv")
            urls = prepare_urls_for_submission(urls_to_be_submitted, submitted_urls)
            i = 0

    print("\nAll of 100 keys fully used. Starting again from KEY 1.\n")
