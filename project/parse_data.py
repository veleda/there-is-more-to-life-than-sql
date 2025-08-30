import polars as pl
pl.Config.set_fmt_str_lengths(150)

from maplib import Mapping


## 
# Welcome to the super-messy data-handling-parsing-thing for public data
# 

nve_tpl = "http://data.eksempel.no/tpl/Vannkraftverk"
ssb_tpl = "http://data.eksempel.no/tpl/Inntekt"

ns = "http://data.eksempel.no/"

## Parsing NVE data about Vannkraftverk
def parse_nve():
    nve = pl.read_csv(
        "data/Vannkraftverk.csv",
        skip_rows=2,
        separator=";", 
        encoding="latin1", # to support more than ascii
        truncate_ragged_lines=True, # skips extra values for rows, if any
        null_values=["xxx"] 
    )

    # Columns of data I want to keep from source
    nve = nve.select([
        "Løpenummer", 
        "Navn", 
        "Maks ytelse [MW]", 
        "ErIDrift", 
        "IDriftDato", 
        "Nedborsfeltnavn",
        "Kommune"
    ])

    # Renaming stuff
    nve = nve.rename({"Løpenummer": "id"}).rename({"Navn": "navn"}).rename({"Maks ytelse [MW]": "maksYtelse"})
    nve = nve.with_columns(
        pl.col("Nedborsfeltnavn").alias("feltnavn")
    )

    # Creating subject URI
    nve = nve.with_columns(
        (pl.lit(ns) + pl.col("id").cast(pl.Utf8)).alias("id")
    ) # alias, to prevent new weird column

    # Creating URIs -- clearning and collapse camel case
    nve = nve.with_columns(
        (
            pl.lit(ns) + 
            pl.col("Nedborsfeltnavn")
            .cast(pl.Utf8)
            .str.replace_all(r"\W+", "")  # Remove all non-word characters (anything except a-z, A-Z, 0-9, _)
        ).alias("Nedborsfeltnavn")
    )

    # Casting max capacity value to float
    nve = nve.with_columns(
        pl.col("maksYtelse")
        .str.replace(",", ".")
        .cast(pl.Float64)
        .alias("maksYtelse")
    )

    return nve

############

## Parsing SSB data about avg income and municipalities
def parse_ssb():
    ssb = pl.read_csv(
        "data/ssb.csv",
        separator=";",
        skip_rows=3,
        null_values=[".", ":"],  # Treat dots as null
        encoding="utf8"
    )

    # Because of messy headers, do some skipping, and clear up municipality name to create URIs
    first_col = ssb.columns[0]
    ssb = ssb.with_columns([
        pl.col(first_col).fill_null(strategy="forward").alias("kommune_raw")
    ])

    ssb = ssb.with_columns([
        pl.col("kommune_raw").str.extract(r"\d+\s+(.*)", 1).alias("kommune")
    ])

    ssb = ssb.with_columns(
        pl.col("kommune").alias("kommunenavn")
    )

    # Creating URIs
    ssb = ssb.with_columns(
        (
            pl.lit(ns) + 
            pl.col("kommune")
            .cast(pl.Utf8)
            .str.replace_all(r"[^a-zA-ZæøåÆØÅ0-9]+", "") # Remove all non-word characters (anything except a-z, A-Z, 0-9, _)
        ).alias("kommune")
    )

    # Casting values for avg income to int, and renaming columns as OTTR does not support numbers as variable names
    ssb = ssb.with_columns([
        pl.col("2022").cast(pl.Int32),
        pl.col("2023").cast(pl.Int32),
        pl.col("2024").cast(pl.Int32),
    ])

    ssb = ssb.rename({"2022": "a"})
    ssb = ssb.rename({"2023": "b"})
    ssb = ssb.rename({"2024": "c"})

    # Filtering on only "bor i regionen", not "arbeider i regionen"
    arbeidssted_col = ssb.columns[2] 
    ssb = ssb.filter(pl.col(arbeidssted_col) == "Bor i regionen")

    # Drop columns we don't need
    ssb = ssb.drop(['kommune_raw', '', '_duplicated_0', '_duplicated_1', '_duplicated_2', '_duplicated_3', '_duplicated_4'])
    return ssb