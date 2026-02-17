import io
import pandas as pd
import requests
from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.postgres import Postgres
from os import path

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_taxi_zones(*args, **kwargs):
    url = "https://d37ci6vzurychx.cloudfront.net/misc/taxi+_zone_lookup.csv"
    
    schema_name = 'bronze'
    table_name = 'taxi_zones'
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'

    print("Descargando Taxi Zones...")
    response = requests.get(url)
    response.raise_for_status()
    df = pd.read_csv(io.BytesIO(response.content))
    
    df['ingest_ts'] = pd.Timestamp.now()
    
    df.columns = [c.lower().replace(' ', '_') for c in df.columns]

    with Postgres.with_config(ConfigFileLoader(config_path, config_profile)) as loader:
        loader.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name};")
        loader.export(
            df,
            schema_name=schema_name,
            table_name=table_name,
            index=False,
            if_exists='replace', 
        )
        
    return "Taxi Zones Cargadas"


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
