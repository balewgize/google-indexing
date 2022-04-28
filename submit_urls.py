"""
Python Script to Extract Post URL from Sitemap & Submit to an API
"""
import csv
import requests


def save_urls_to_csv():
    """Save all URLs to be submitted to a CSV file."""
    try:
        headers = {
            "authority": "squirrelarena.com",
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
            "accept-language": "en-US,en;q=0.9",
            "cache-control": "max-age=0",
            "sec-fetch-dest": "document",
            "sec-fetch-mode": "navigate",
            "sec-fetch-site": "none",
            "sec-fetch-user": "?1",
            "sec-gpc": "1",
            "upgrade-insecure-requests": "1",
            "user-agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.127 Safari/537.36",
        }

        r = requests.get("https://squirrelarena.com/export.php", headers=headers)
        r.raise_for_status()
        urls = r.text.strip().split()

        with open("PostURLs.csv", mode="w", encoding="utf-8", newline="") as f:
            csv_writer = csv.writer(f)
            csv_writer.writerow(["URL"])
            [csv_writer.writerow([url]) for url in urls]
    except requests.HTTPError as e:
        print(e)
    except Exception as e:
        print("ERROR: ", e)
    return None


save_urls_to_csv()
