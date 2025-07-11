import pandas as pd
import numpy as np
import strsimpy
import requests
from time import time, sleep
from config import config
from utils import bq_utils
from fuzzywuzzy import process
import re
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

def similarity(str1, str2, n):
    return strsimpy.jaccard.Jaccard(n).similarity(str1, str2)

def clean_str(text):
    if pd.isna(text):
        return
    cleaned_text = text.replace("ñ", "n").replace("ã±", "n")

    return cleaned_text.lower()

def bq_data_to_df(): # load data from bq
    df = bq_utils.sql_query_bq(f"SELECT * FROM {config.GCLOUD_PROJECT_ID}.locations.address_location_psgc")
    df["address_cleaned"] = df["address"].map(clean_str)
    return df
df_bq = bq_data_to_df() # cache
df_bq_munprov = df_bq[(df_bq["geo_level"] == "municity") | (df_bq["geo_level"] == "provdist")] # cache

def geocode_osm(address):
    base = "https://nominatim.openstreetmap.org/search"
    params = {
        "q": address,
        "format": "json",    # ask for JSON output
        "limit": 1,          # max results
    }
    headers = {"User-Agent": "my_geocoder_app"}  # required
    resp = requests.get(base, params=params, headers=headers)
    resp.raise_for_status()  # will raise HTTPError on >400
    data = resp.json()
    if data:
        return float(data[0]["lat"]), float(data[0]["lon"])
    else:
        return None

def geocode_photon(address):
    resp = requests.get(
        "https://photon.komoot.io/api/",
        params={"q": address, "limit": 1},
    )
    resp.raise_for_status()
    data = resp.json().get("features")
    if data:
        coords = data[0]["geometry"]["coordinates"]
        return coords[1], coords[0]
    return None

def geocode(address):
    global time_osm
    if not address:
        return None

    address_cleaned = clean_str(address)
    n = max(5, round(np.sqrt(len(address_cleaned)/2)))
    
    df_munprov = df_bq_munprov.copy()
    df_munprov["score"] = df_munprov["address_cleaned"].map(
        lambda addr_row: similarity(address_cleaned, addr_row, n)
    )
    df_munprov = df_munprov[df_munprov["score"] != 0]

    conds = pd.Series(False, index=df_bq.index)
    for row in df_munprov.itertuples():
        if row.geo_level == "municity":
            conds |= df_bq["municity_code"] == row.municity_code
        elif row.geo_level == "provdist":
            conds |= df_bq["provdist_code"] == row.provdist_code
        else:
            raise ValueError(f"Unexpected geo_level: {row.geo_level}")

    df_bq_brgy = df_bq[conds].copy()
    df_bq_brgy["score"] = df_bq_brgy["address_cleaned"].map(
        lambda addr_row: similarity(address_cleaned, addr_row, n)
    )
    df_bq_brgy.sort_values(by="score", ascending=False, inplace=True)

    if not df_bq_brgy.empty:
        local_match = df_bq_brgy[["address", "latitude", "longitude", "score"]].iloc[0].to_dict()
        local_match.update({
            "input_address": address,
            "source": "database"
        })

        if local_match["score"] >= 0.1:
            return local_match
    return fallback_geocode(address)

def fallback_geocode(address):
    global time_osm
    _address = address + ", Philippines"
    
    try:
        delay = 1.25 - (time() - time_osm)
        if delay > 0:
            sleep(delay)
    except Exception:
        pass

    osm_result = geocode_osm(_address)
    time_osm = time()
    if osm_result:
        lat, lng = osm_result
        return {
            "input_address": address,
            "address": _address,
            "latitude": lat,
            "longitude": lng,
            "score": 0,
            "source": "osm"
        }
    
    photon_result = geocode_photon(_address)
    if photon_result:
        lat, lng = photon_result
        return {
            "input_address": address,
            "address": _address,
            "latitude": lat,
            "longitude": lng,
            "score": 0,
            "source": "photon"
        }

    return None

def normalize_location(text: str):
    if not isinstance(text, str):
        return ""

    text = text.encode("latin1").decode("utf-8", "ignore")
    text = text.lower()
    text = re.sub(r'[^a-z\s]', '', text)
    text = text.replace("city of", "").replace("municipality of", "")
    text = text.replace("gen", "general")
    text = text.replace("sto", "santo")
    text = re.sub(r'\s+', ' ', text).strip()
    logging.info(f"text: {text}")
    return text

def viable(location, serviceable_list, threshold=90):
    normalized_loc = normalize_location(location)
    match = process.extractOne(normalized_loc, serviceable_list)

    logging.info(f"normalized_loc: {normalized_loc}")
    logging.info(f"serviceable_list: {serviceable_list}")
    if match and match[1] >= threshold:
        logging.info(f"match: {match}")
        return "Yes"
    else:
        logging.info(f"match: {match}")
        return "No"

def tag_viable(df: pd.DataFrame) -> pd.DataFrame:
    try:
        municipalities_df = pd.read_csv("config/mgo_serviceable.csv")
        municipalities_df['normalized'] = municipalities_df['municipality_name'].apply(normalize_location)
        normalized_serviceable = municipalities_df['normalized'].dropna().unique().tolist()

        df['viable'] = df['location'].apply(lambda loc: viable(loc, normalized_serviceable))
        return df
    except FileNotFoundError as e:
        logging.info(f"File not found: {e}")
    except Exception as e:
        logging.info(f"Exception occurred while tagging viable locations: {e}")