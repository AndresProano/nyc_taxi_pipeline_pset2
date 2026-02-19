import requests

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


@data_exporter
def export_data(data, *args, **kwargs):

    url = "http://localhost:6789/api/pipeline_schedules/2/pipeline_runs/445c84bb29dc4c95ba1b4d3849f9c033"
    payload = {"pipeline_run": {}}
    response = requests.post(url, json=payload)
    
    if response.status_code == 200:
        print("Trigger dbt_after_ingest disparado con éxito")
    else:
        print(f"Error al disparar trigger: {response.text}")


