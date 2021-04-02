import os
import csv
import json
import re
import click
import requests
import pandas as pd
from pathlib import Path
from pprint import pprint


def get_columns(schema):
    colnames, colsplits = [], []
    type_map = {}
    start_index = 0
    with open(schema, "r") as f:
        reader = csv.reader(f)
        for row in reader:
            colnames.append(row[0])
            offset = int(row[1])
            colsplits.append([start_index, start_index + offset])
            if row[2] == "TEXT":
                type_map[row[0]] = "object"
            elif row[2] == "BOOLEAN":
                type_map[row[0]] = bool
            elif row[2] == "INTEGER":
                type_map[row[0]] = int
            else:
                raise TypeError(f"Got an invalid type for {row}")
            start_index += offset
    return colnames, colsplits, type_map


@click.command()
@click.option(
    "--for-real", default=False, help="Whether to POST data for real, or just dry run"
)
@click.option(
    "--endpoint",
    default="https://2swdepm0wa.execute-api.us-east-1.amazonaws.com/prod/XXX/measures",
    help="Where to send the data",
)
def send_data(for_real, endpoint):
    files = os.listdir("data")
    schemas = {f[:-4]: f for f in files if re.match(r".*\.txt", f)}
    datafiles = {f[:-4]: f for f in files if re.match(r".*\.csv", f)}

    if len(schemas) != len(datafiles):
        click.echo("We dont have matching schema and data files")
        raise click.Abort()

    for name, schema in schemas.items():
        schema_path = Path("data") / schema
        colnames, colsplits, coltypes = get_columns(schema_path)
        csv_path = Path("data") / datafiles[name]

        df = pd.read_fwf(
            csv_path, names=colnames, header=None, colspecs=colsplits
        ).astype(coltypes)

        records = json.loads(df.to_json(orient="records"))

        for record in records:
            if for_real:
                r = requests.post(endpoint, data=record)
                click.echo(r)
            else:
                click.echo(f"Dry run: send a POST to {endpoint} with {record} as data")


if __name__ == "__main__":
    send_data()
