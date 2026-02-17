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
def load_data_from_api(*args, **kwargs):

    years = ['2024'] 
    months = ['01'] 
    services = ['yellow', 'green'] 
    
    schema_name = 'bronze'
    table_name = 'raw_trips'
    
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'

    temp_file = '/tmp/temp_taxi_data.parquet'

    print(f"Iniciando ingesta a {schema_name}.{table_name}...")

    with Postgres.with_config(ConfigFileLoader(config_path, config_profile)) as loader:
        
        for year in years:
            for month in months:
                for service in services:
                    
                    file_name = f"{service}_tripdata_{year}-{month}.parquet"
                    url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{file_name}"
                    
                    print(f"Procesando: {file_name}...")
                    
                    try:
                        response = requests.get(url)
                        response.raise_for_status()
                        
                        df = pd.read_parquet(io.BytesIO(response.content))
                        
                        df['ingest_ts'] = pd.Timestamp.now()
                        df['source_month'] = f"{year}-{month}"
                        df['service_type'] = service 
                        
                        df.columns = [c.lower() for c in df.columns]

                        check_query = f"""
                        SELECT 1 
                        FROM information_schema.tables 
                        WHERE table_schema = '{schema_name}' 
                        AND table_name = '{table_name}';
                        """

                        tabla_exist = len(loader.load(check_query)) > 0

                        if tabla_exist:
                            query_delete = f"""
                            DELETE FROM {schema_name}.{table_name}
                            WHERE source_month = '{year}-{month}' 
                            AND service_type = '{service}';
                            """
                            loader.execute(query_delete)
                            loader.commit()
                            print(f"Datos previos eliminados {years}-{months}")
                        else:
                            print("No existe una tabla, se creará una nueva")

                        loader.export(
                            df,
                            schema_name=schema_name,
                            table_name=table_name,
                            index=False,
                            if_exists='append',
                            chunksize=10000,
                            allow_reserved_words=True
                        )
                        print(f"Éxito: Insertadas {len(df)} filas.")
                        
                    except requests.exceptions.HTTPError:
                        print(f"Archivo no encontrado: {url} (Probablemente mes futuro)")
                    except Exception as e:
                        print(f"Error crítico en {file_name}: {e}")
                        raise e 
    return "Ingest Complete"


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
