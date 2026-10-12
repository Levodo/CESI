# -*- coding: utf-8 -*-
"""
=====================================================================================
ingest_api_fx.py — Ingestion de l'API de taux de change Frankfurter (sans cle)
                    Frankfurter FX rates API ingestion (no API key)

POURQUOI / WHY :
  Source API n°2 du projet : elle couvre exactement la periode des trades Cymbal
  (mai-novembre 2020) et permet de valoriser les trades en EUR/BRL (coherence
  inter-sources, docs/01 §8.2).
  Project's 2nd API source: it covers exactly the Cymbal trade period (May-Nov
  2020) and allows valuing trades in EUR/BRL (cross-source coherence).

COMMENT / HOW :
  - Python PUR (bibliotheque standard) comme l'API meteo.
  - La reponse est archivee telle quelle avec sa periode dans le nom (regle B5).
  - The response is archived as-is with its period in the file name (rule B5).

USAGE / CALL :
  python ingest_api_fx.py --start 2020-05-18 --end 2020-11-23 --out-dir /landing_api
  (sans arguments : periode Cymbal complete par defaut)

VERIFICATION :
  python -c "import json;d=json.load(open('<fichier>'));print(len(d['rates']),'jours')"
=====================================================================================
"""

import argparse
import json
import os
import urllib.parse
import urllib.request

API_URL = "https://api.frankfurter.app"

# Devises suivies : real brasilien (marche Olist), euro, livre, yen (marches Cymbal)
# Tracked currencies: Brazilian real (Olist market), euro, sterling, yen (Cymbal markets)
DEVISES = "BRL,EUR,GBP,JPY"


def fetch_fx(start: str, end: str, timeout: int = 60) -> dict:
    """
    Appelle l'API Frankfurter sur une plage de dates / Calls the Frankfurter API over a date range.
    URL : https://api.frankfurter.app/<start>..<end>?base=USD&symbols=...
    """
    params = urllib.parse.urlencode({"base": "USD", "symbols": DEVISES})
    url = f"{API_URL}/{start}..{end}?{params}"
    print(f"[api-fx] appel / call: {url}")
    # POURQUOI un User-Agent explicite : l'API Frankfurter renvoie HTTP 403 aux
    # requetes portant le user-agent par defaut de urllib ("Python-urllib/3.x"),
    # bloque comme robot (constate le 07/10/2026 depuis le conteneur Airflow).
    # Un en-tete d'application explicite est aussi une politesse API standard.
    # WHY an explicit User-Agent: the Frankfurter API returns HTTP 403 to requests
    # carrying urllib's default user-agent ("Python-urllib/3.x"), blocked as a bot
    # (observed 2026-10-07 from the Airflow container). An explicit app header is
    # also standard API etiquette.
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": "cidsec-data-pipeline/1.0 (contact: data-team@cidsec.local)",
            "Accept": "application/json",
        },
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:   # GET HTTP
        payload = resp.read().decode("utf-8")
    return json.loads(payload)


def main():
    p = argparse.ArgumentParser(description="Ingestion API taux de change / FX API ingestion")
    p.add_argument("--start", default="2020-05-18", help="Date de debut / start date (AAAA-MM-JJ)")
    p.add_argument("--end", default="2020-11-23", help="Date de fin / end date (AAAA-MM-JJ)")
    p.add_argument("--out-dir", default=os.environ.get("LANDING_API_DIR", "/landing_api"),
                   help="Dossier d'atterrissage / landing directory")
    args = p.parse_args()

    data = fetch_fx(args.start, args.end)

    # Controle minimal : la map "rates" doit contenir des jours / minimal check
    rates = data.get("rates", {})
    assert len(rates) > 0, "API reponse vide / empty API response"
    print(f"[api-fx] {len(rates)} jours ouvres recus / business days received")

    os.makedirs(args.out_dir, exist_ok=True)
    out_path = os.path.join(args.out_dir, f"frankfurter_usd_{args.start}_{args.end}.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False)
    print(f"[api-fx] archivee / archived: {out_path}")


if __name__ == "__main__":
    main()
