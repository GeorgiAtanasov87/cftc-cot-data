"""Run python data_collection.py to download and aggregate all data to a single parquet file."""
import requests
from pathlib import Path
import os
import zipfile
import re
import pandas as pd
from tqdm import tqdm
from datetime import datetime


def _download_cot_zip(year: int, folder: Path, overwrite: bool = False) -> None:
    """Downloads COT Futures only report for a given year as a zip
    If overwrite == True it will replace any existing data files
    Always downloads data for the current year
    """
    zip_path = Path(folder, f"deacot{year}.zip")
    if (zip_path.is_file()) & (not overwrite) & (datetime.now().year != year):
        print(f"{zip_path} exists, skipping download")
        return None

    url = f"https://www.cftc.gov/files/dea/history/deacot{year}.zip"
    print(f"Downloading {url} to {zip_path}")
    r = requests.get(url)
    with open(zip_path, "wb") as outfile:
        outfile.write(r.content)


def collect_reports(overwrite: bool = False):
    for year in range(1986, datetime.now().year + 1):
        _download_cot_zip(year, data_location, overwrite)


def parse_annual_zip(file_path: Path) -> pd.DataFrame:
    """Parse report (annual deacotYYYY.zip) and export
    columns for Sherloc report
    """
    df = (
        pd.read_csv(
            file_path,
            engine="pyarrow",
            usecols=[
                "Market and Exchange Names",
                "As of Date in Form YYYY-MM-DD",
                "Noncommercial Positions-Long (All)",
                "Noncommercial Positions-Short (All)",
                "Commercial Positions-Long (All)",
                "Commercial Positions-Short (All)",
            ],
        )
        .rename(
            columns={
                "Market and Exchange Names": "product",
                "As of Date in Form YYYY-MM-DD": "report_date",
                "Noncommercial Positions-Long (All)": "noncommercial_long",
                "Noncommercial Positions-Short (All)": "noncommercial_short",
                "Commercial Positions-Long (All)": "commercial_long",
                "Commercial Positions-Short (All)": "commercial_short",
            }
        )
        .set_index(["product", "report_date"])
    )
    return df


def aggregate_reports(source_folder: Path, destination_file: Path) -> pd.DataFrame:
    """Agreggates zipped reports into a single ordered parquet"""
    print(f"Aggregating zips in {source_folder}")
    df = pd.DataFrame()
    for file_path in tqdm(source_folder.glob("*.zip")):
        df = pd.concat([df, parse_annual_zip(file_path)])
    df = df.sort_values(by=["product", "report_date"], ascending=True)
    print(f"Writing {destination_file}")
    df.to_parquet(destination_file)


if __name__ == "__main__":
    data_location = Path("data/zip")
    data_location.mkdir(parents=True, exist_ok=True)
        
    collect_reports(overwrite=False)
    aggregate_reports(data_location, Path("data/all_cot.parquet"))
