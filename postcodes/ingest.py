import os
from pathlib import Path

import polars as pl
import geopandas as gp
from dotenv import load_dotenv


load_dotenv()
path_data = Path(os.environ.get("DATA_PATH"))


def load_pp() -> pl.LazyFrame:
    path_pp_parquet = path_data / "parquet" / "price_paid.parquet"
    pp_schema = pl.Schema(
        {
            "tuid": pl.String(),
            "price": pl.Int64(),
            "transfer_date": pl.Datetime(),
            "postcode": pl.String(),
            # Presumably Terraced, Flat, O-something?, Semi-detached, Detached
            "type": pl.Enum(categories=["T", "F", "O", "S", "D"]),
            "is_new_build": pl.Enum(categories=["Y", "N"]),
            # Presumably Freehold, Leasehold, Unclassified
            "duration": pl.Enum(categories=["F", "L", "U"]),
            "street_number": pl.String(),
            "flat_number": pl.String(),
            "street": pl.String(),
            "locality": pl.String(),
            "town": pl.String(),
            "district": pl.String(),
            "county": pl.String(),
            "category_type": pl.String(),
            "record_status": pl.String()
        }
    )
    pp = pl.scan_parquet(path_pp_parquet, schema=pp_schema)
    return pp

def load_postcodes() -> pl.LazyFrame:
    path_postcodes_parquet = path_data / "parquet" / "postcodes.parquet"
    schema_postcodes = pl.Schema({
        "postcode": pl.String(),
        "live_or_terminated": pl.Categorical(),
        "size": pl.Categorical(),
        "easting": pl.Int64(),
        "northing": pl.Int64(),
        "tbd": pl.Int8(),
        "country": pl.Categorical(),
        "latitude": pl.Float64(),
        "longitude": pl.Float64(),
        "postcode_no_spaces": pl.String(),
        "postcode_unit": pl.String(),
        "postcode_2_spaces": pl.String(),
        "postcode_area": pl.String(),
        "postcode_district": pl.String(),
        "postcode_sector": pl.String(),
        "postcode_first_half": pl.String(),
        "postcode_second_half": pl.String(),
    })
    postcodes = pl.scan_parquet(path_postcodes_parquet, schema=schema_postcodes)
    return postcodes

def load_bham_wards() -> gp.GeoDataFrame:
    path_bham_wards = path_data / "geojson" / "boundaries-wards-birmingham.geojson"
    bham_wards = gp.read_file(path_bham_wards)
    return bham_wards