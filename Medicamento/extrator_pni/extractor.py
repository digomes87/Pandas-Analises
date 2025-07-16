import requests
from config import CKAN_URL, DATASET_ID

def listar_recursos():
    r = requests.get(f"{CKAN_URL}/package_show", params={"id": DATASET_ID})
    r.raise_for_status()
    return r.json()["result"]["resources"]

def filtrar_csvs_2021(resources):
    return [
        r for r in resources
        if r.get("format", "").lower() == "csv" and "2021" in r.get("name", "")
    ]

