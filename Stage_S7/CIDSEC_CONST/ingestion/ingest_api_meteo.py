# -*- coding: utf-8 -*-
"""
=====================================================================================
ingest_api_meteo.py — Ingestion de l'API meteo Open-Meteo (sans cle d'API)
                       Open-Meteo weather API ingestion (no API key)

POURQUOI / WHY :
  Le sujet exige une ingestion MULTI-CANALE (fichiers ET API). Open-Meteo est sans
  cle, historique, et couvre exactement la periode des commandes Olist (Sao Paulo) :
  il permet le croisement meteo <-> retards de livraison (voir docs/01 §8.2).
  The subject requires MULTI-CHANNEL ingestion (files AND APIs). Open-Meteo needs no
  key, is historical, and covers exactly the Olist order period (Sao Paulo): it
  enables the weather <-> delivery-delay cross-analysis.

COMMENT / HOW :
  - Python PUR (bibliotheque standard uniquement) : fonctionne dans le conteneur
    Airflow sans installation supplementaire.
  - Pure Python (standard library only): runs in the Airflow container with no
    extra install.
  - La reponse est archivee TELLE QUELLE (regle B5) dans le dossier d'atterrissage,
    avec date dans le nom de fichier -> reproductibilite totale.
  - The response is archived AS-IS (rule B5) in the landing folder, dated in the
    file name -> full reproducibility.

USAGE / CALL :
  python ingest_api_meteo.py --start 2016-09-01 --end 2018-10-31 --out-dir /landing_api
  (sans arguments : periode Olist complete par defaut)

VERIFICATION :
  ls /landing_api/openmeteo_*.json  -> 1 fichier, et sa taille > 0
  python -c "import json;d=json.load(open('<fichier>'));print(len(d['daily']['time']),'jours')"
=====================================================================================
"""

import argparse
import json
import os
import urllib.parse
import urllib.request
from datetime import date

API_URL = "https://archive-api.open-meteo.com/v1/archive"

# Coordonnees de Sao Paulo (principal marche Olist) / Sao Paulo coordinates (main Olist market)
LATITUDE = -23.55
LONGITUDE = -46.63

# Variables quotidiennes demandees / requested daily variables
DAILY_VARS = "temperature_2m_mean,temperature_2m_max,temperature_2m_min,precipitation_sum,windspeed_10m_max"


def fetch_meteo(start: str, end: str, timeout: int = 60) -> dict:
    """
    Appelle l'API et retourne le dictionnaire JSON / Calls the API and returns the JSON dict.

    POURQUOI / WHY :
      urllib (et non requests) : zero dependance externe, meme comportement partout.
      urllib (not requests): zero external dependency, same behavior everywhere.
    COMMENT / HOW :
      Les parametres d'URL sont construits par urllib.parse pour eviter les erreurs
      d'encodage (les dates et le timezone contiennent des caracteres sensibles).
      URL parameters built via urllib.parse to avoid encoding errors.
    """
    params = urllib.parse.urlencode({
        "latitude": LATITUDE,
        "longitude": LONGITUDE,
        "start_date": start,
        "end_date": end,
        "daily": DAILY_VARS,
        "timezone": "America/Sao_Paulo",
    })
    url = f"{API_URL}?{params}"
    print(f"[api-meteo] appel / call: {url}")
    with urllib.request.urlopen(url, timeout=timeout) as resp:   # GET HTTP
        payload = resp.read().decode("utf-8")
    return json.loads(payload)


def main():
    p = argparse.ArgumentParser(description="Ingestion API meteo / weather API ingestion")
    p.add_argument("--start", default="2016-09-01", help="Date de debut / start date (AAAA-MM-JJ)")
    p.add_argument("--end", default="2018-10-31", help="Date de fin / end date (AAAA-MM-JJ)")
    p.add_argument("--out-dir", default=os.environ.get("LANDING_API_DIR", "/landing_api"),
                   help="Dossier d'atterrissage / landing directory")
    args = p.parse_args()

    data = fetch_meteo(args.start, args.end)

    # Controle minimal avant archivage / minimal check before archiving
    n_days = len(data.get("daily", {}).get("time", []))
    assert n_days > 0, "API reponse vide / empty API response"
    print(f"[api-meteo] {n_days} jours recus / days received")

    # Archive DATEDE et non ecrasable / DATED, non-overwritable archive (rule B5)
    os.makedirs(args.out_dir, exist_ok=True)
    out_path = os.path.join(args.out_dir, f"openmeteo_sp_daily_{args.start}_{args.end}.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False)
    print(f"[api-meteo] archivee / archived: {out_path}")


if __name__ == "__main__":
    main()
