import os
import errno
import pandas as pd
import psycopg2
import requests
from environs import Env
from more_ds.network.url import URL
from psycopg2 import sql
from resources.resources import RESOURCES
from sqlalchemy import create_engine
from typing import Optional
from sqlalchemy.types import Date, Integer, Numeric, Text

env_db_conn: Env = Env()
DB_CONN:str = env_db_conn("CONN_DATABASE_POSTGRES_DEFAULT")
db_engine = create_engine(DB_CONN)

env_duo_conn: Env = Env()
BASE_URL:str = env_duo_conn("CONN_ONDERWIJSDATA_DUO_BASE_URL")

SHARED_DIR:str = '/usr/local/data_processor/downloads/'
DATASET_NAME:str ='onderwijs'
TARGET_DB_SCHEMA:str = 'dataset_onderwijs'

# ----------------------------------------------------- #
# PROCES LOGIC                                          #
# ----------------------------------------------------- #


def check_for_path(
    directory: str
):
    if not os.path.exists(os.path.dirname(directory)):
        try:
            os.makedirs(os.path.dirname(directory))
        except OSError as exc: # Guard against race condition
            if exc.errno != errno.EEXIST:
                raise


def download_file(
    resource_name: str,
) -> dict:
    """Downloads data from source RIO as a csv dataset.

    Args:
        resource_name: the name of the resource to proces whitin the RIO source.

    Returns:
        A dictionary containing RIO source records.

    """
    check_for_path(SHARED_DIR)
    resource = RESOURCES['duo']['rio_datasets']['resources'].get(resource_name)
    data_endpoint = URL(BASE_URL) / resource

    try:
        request = requests.get(data_endpoint)
        request.raise_for_status()
    except requests.exceptions.HTTPError as exc:
        raise RuntimeError(f'Please check URL: {str(exc)}') from exc

    data = request.content
    with open(f"{SHARED_DIR}{resource_name}.csv", 'wb') as csv_file:
        csv_file.write(data)


def import_data (
    schema_name: str,
    resource_name: str,
) -> None:
    """Import data from data structure into database table

    Args:
        schema_name: Target schema to import data into
        resource_name: the name of the resource to proces whitin the RIO source.

    Executes:
        SQL insert statements and executes it into database

    """
    db_conn = psycopg2.connect(DB_CONN)

    with db_conn:
        db_conn.autocommit = True
        cur = db_conn.cursor()
        cur.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {schema_name}").format(
                        schema_name=sql.Identifier(TARGET_DB_SCHEMA))
                    )

    download_file(resource_name)

    data = pd.read_csv (fr"{SHARED_DIR}{resource_name}.csv")
    df = pd.DataFrame(data)
    df.columns = df.columns.str.lower()
    dtype = {
        "id": Integer(),
        "onderwijsbestuurid": Text(),
        "in_bedrijfdatum": Date(),
        "uit_bedrijfdatum": Date(),
        "begindatum_periode": Date(),
        "einddatum_periode": Date(),
        "internationale_naam": Text(),
        "kvk_nummer": Numeric(),
        "rsin": Numeric(),
    }
    df.to_sql(
        name=f"{DATASET_NAME}_{resource_name}_new",
        con=db_engine,
        schema=schema_name,
        if_exists="replace",
        index=True,
        index_label='id',
        dtype=dtype
    )

    with db_conn:
        db_conn.autocommit = True
        cur = db_conn.cursor()
        cur.execute(sql.SQL("ALTER TABLE {schema_name}.{table_name} ADD PRIMARY KEY (id)").format(
                        schema_name=sql.Identifier(TARGET_DB_SCHEMA), table_name=sql.Identifier(f"{DATASET_NAME}_{resource_name}_new")
                    ))


if __name__ == "__main__":
    import_data(TARGET_DB_SCHEMA, 'onderwijsbesturen')
    # import_data(TARGET_DB_SCHEMA, 'examenlicenties')


