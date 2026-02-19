import io
import pandas as pd
import requests
from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.postgres import Postgres
from os import path
from datetime import datetime

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader

@data_loader
def load_data_smart(*args, **kwargs):
    execution_date = kwargs.get('execution_date')
   
    target_months = [] 
    
    
    if execution_date and kwargs.get('interval_seconds') is not None:
        year = execution_date.strftime('%Y')
        month = execution_date.strftime('%m')
        target_months.append((year, month))
        print(f"MODO TRIGGER: Cargando solo {year}-{month}")
    else:
        print("MODO MANUAL: Cargando historia completa (2022-2025)")
        years_history = ['2022', '2023', '2024', '2025']
        for y in years_history:
            for m in range(1, 13):
                target_months.append((y, f"{m:02d}"))

    services = ['yellow', 'green']
    schema_name = 'bronze'
    table_name = 'raw_trips'
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'

    with Postgres.with_config(ConfigFileLoader(config_path, config_profile)) as loader:
        
        for (year, month) in target_months:
            for service in services:
                file_name = f"{service}_tripdata_{year}-{month}.parquet"
                url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{file_name}"
                
                print(f"Procesando: {file_name} ...")
                
                try:
                    response = requests.get(url, timeout=60)
                    if response.status_code == 404:
                        print(f"⚠️ Salteando {file_name} (No existe)")
                        continue
                    
                    df = pd.read_parquet(io.BytesIO(response.content))
                    
                    df['ingest_ts'] = pd.Timestamp.now()
                    df['source_month'] = f"{year}-{month}"
                    df['service_type'] = service 
                    df.columns = [c.lower() for c in df.columns]

                    loader.execute(f"DELETE FROM {schema_name}.{table_name} WHERE source_month = '{year}-{month}' AND service_type = '{service}';")
                    
                    loader.export(
                        df,
                        schema_name=schema_name,
                        table_name=table_name,
                        index=False,
                        if_exists='append',
                        chunksize=50000
                    )
                    print(f"Cargado: {service} {month}/{year}")
                    
                except Exception as e:
                    print(f"Error en {file_name}: {e}")
                    continue

    return "Carga Completada"