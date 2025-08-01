import requests
import time
import logging
from google.cloud import storage
from google.cloud import bigquery
from datetime import datetime
import json
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from concurrent.futures import ThreadPoolExecutor

def get_steamspy_api_session():
    session = requests.Session()
    retries = Retry(total=5, backoff_factor=2, status_forcelist=[429, 500, 502, 503, 504])
    session.mount('https://', HTTPAdapter(max_retries=retries))
    return session

def fetch_app_list():
    logging.info("Fetching app list from Steam official API")
    session = get_steamspy_api_session()
    try:
        response = session.get("https://api.steampowered.com/ISteamApps/GetAppList/v2/", timeout=20)
        response.raise_for_status()
        apps = response.json()["applist"]["apps"]
        app_ids = [app["appid"] for app in apps if app.get("appid")]
        logging.info(f"Fetched {len(app_ids)} total app IDs")
        return app_ids
    except Exception as e:
        logging.error(f"Failed to fetch app list: {e}")
        return []

def fetch_app_details(appid, session):
    url = f"https://steamspy.com/api.php?request=appdetails&appid={appid}"
    try:
        response = session.get(url, timeout=10)
        response.raise_for_status()
        data = json.loads(response.text)
        if data and data.get("appid") != 999999:
            return data
        else:
            return None
    except Exception:
        return None

def validate_app_ids(app_ids, session, sample_size=100):
    logging.info(f"Validating sample of {sample_size} app IDs")
    valid_ids = []
    sample = app_ids[:sample_size]
    with ThreadPoolExecutor(max_workers=3) as executor:
        results = [executor.submit(fetch_app_details, appid, session) for appid in sample]
        for appid, future in zip(sample, results):
            if future.result():
                valid_ids.append(appid)
    success_rate = len(valid_ids) / len(sample) if sample else 0
    logging.info(f"Validation success rate: {success_rate:.2%}")
    return app_ids[:int(len(app_ids) * success_rate * 1.5)]

def extract_and_upload(**kwargs):
    execution_date = kwargs.get("ds")
    run_id = kwargs.get("run_id")
    conf = kwargs.get("dag_run").conf or {}
    is_initial_load = conf.get("is_initial_load", False)
    max_apps = conf.get("max_apps", None)

    storage_client = storage.Client()
    bucket = storage_client.bucket("us-central1-composer-dev-33ed7046-bucket")
    bq_client = bigquery.Client()
    dataset_id = "steam_data"
    table_id = "cleaned_steam_games"

    session = get_steamspy_api_session()
    app_ids = fetch_app_list()

    app_ids = validate_app_ids(app_ids, session)

    if not is_initial_load:
        query = f"SELECT DISTINCT appid FROM `{bq_client.project}.{dataset_id}.{table_id}`"
        processed_ids = {row["appid"] for row in bq_client.query(query).result()}
        sample_fraction = 0.01
        sampled_ids = list(processed_ids & set(app_ids))[:int(len(processed_ids) * sample_fraction)]
        new_ids = list(set(app_ids) - processed_ids)
        app_ids = new_ids + sampled_ids
    if max_apps:
        app_ids = app_ids[:max_apps]

    def fetch_batch(app_ids_batch):
        results = []
        for appid in app_ids_batch:
            time.sleep(2.0)
            app_data = fetch_app_details(appid, session)
            if not app_data:
                continue
            tags_data = app_data.get("tags", {})
            tags = list(set(tag.lower() for tag in tags_data.keys())) if isinstance(tags_data, dict) else []
            owners_range = app_data.get("owners", "0 .. 0").replace(",", "").split(" .. ")
            min_owners = int(owners_range[0]) if owners_range else 0
            max_owners = int(owners_range[1]) if len(owners_range) > 1 else min_owners
            genre_raw = app_data.get("genre")
            genres = [g.strip() for g in genre_raw.split(",")] if genre_raw else []
            # genres = [g.strip() for g in app_data.get("genre", "").split(",") if g.strip()]
            languages_raw = app_data.get("languages", "") or ""
            languages = [l.strip() for l in languages_raw.split(",") if l.strip()]
            price = float(app_data.get("price", 0)) / 100 if app_data.get("price") not in [None, "Free to Play"] else 0
            initialprice = float(app_data.get("initialprice", 0)) / 100 if app_data.get("initialprice") not in [None, "Free to Play"] else 0
            discount = float(app_data.get("discount", 0)) if app_data.get("discount") else 0
            result = {
                "raw": {
                    "appid": appid,
                    "name": app_data.get("name"),
                    "data": app_data,
                    "load_date": execution_date
                },
                "cleaned": {
                    "appid": appid,
                    "name": app_data.get("name"),
                    "genre": genres,
                    "tags": tags,
                    "positive": app_data.get("positive", 0),
                    "negative": app_data.get("negative", 0),
                    "developer": app_data.get("developer") or "Unknown",
                    "publisher": app_data.get("publisher") or "Unknown",
                    "score_rank": app_data.get("score_rank") or "N/A",
                    "owners": app_data.get("owners"),
                    "min_owners": min_owners,
                    "max_owners": max_owners,
                    "average_forever": app_data.get("average_forever", 0),
                    "average_2weeks": app_data.get("average_2weeks", 0),
                    "median_forever": app_data.get("median_forever", 0),
                    "median_2weeks": app_data.get("median_2weeks", 0),
                    "ccu": app_data.get("ccu", 0),
                    "price": price,
                    "initialprice": initialprice,
                    "discount": discount,
                    "languages": languages,
                    "load_date": execution_date
                }
            }
            results.append(result)
        return results

    raw_data = []
    cleaned_data = []
    batch_size = 200
    with ThreadPoolExecutor(max_workers=3) as executor:
        batches = [app_ids[i:i+batch_size] for i in range(0, len(app_ids), batch_size)]
        for i, batch_results in enumerate(executor.map(fetch_batch, batches)):
            for result in batch_results:
                raw_data.append(result["raw"])
                cleaned_data.append(result["cleaned"])
            if not raw_data:
                break
            raw_blob = bucket.blob(f"raw/steam_games_{execution_date}_{i+1}.json")
            raw_blob.upload_from_string("\n".join(json.dumps(d) for d in raw_data))
            cleaned_blob = bucket.blob(f"processed/steam_games_cleaned_{execution_date}_{i+1}.json")
            cleaned_blob.upload_from_string("\n".join(json.dumps(d) for d in cleaned_data))
            raw_data = []
            cleaned_data = []