from datetime import timedelta

from airflow import DAG

from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

import requests, os, random

default_args = {
    'owner': 'Raul',
    'depends_on_past': False,
    'email': ['raul.ochoa9077@alumnos.udg.mx'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': timedelta(minutes=1)
}

# Temas de las imagenes.
unsplashQueries = ["history", "life", "wisdom", "art", "office", "nature"]

# Obtener la clave de la API.
apiKey = '5y-QD2AN071LSFQ38VArLTUknHJUN2dmGQiNDDpipiw'

# Extraer
def obtenerDatos ():
    # Solicitamos los datos de la imagen a la API.
    tema = random.choice (unsplashQueries)
    url = f"https://api.unsplash.com/photos/random?query={tema}&client_id={apiKey}"

    peticion = requests.get (url)

    datos = peticion.json()

    return datos

# Transformar
def transformarDatos (ti=None):
    datos = ti.xcom_pull(task_ids="extract")
    # Obtenemos el url de la imagen.
    url = datos["urls"]["regular"]

    return url

# Almacenar
def almacenarDatos (ti=None):
    imagen = ti.xcom_pull(task_ids="transform")
    # Para evitar tener muchas imagenes descargadas, sobreescribimos el mismo archivo.
    nombreArchivo = "imagen.jpg"
    peticionImagen = requests.get (imagen, stream=True)
    
    if (peticionImagen.status_code != 200):
        raise Exception ("Ocurrio un error al solicitar la imagen a la API.")
    
    with open (nombreArchivo, "wb") as imagenDescarga:
        for chunk in peticionImagen:
            imagenDescarga.write (chunk)
        
    os.system ("start imagen.jpg")

with DAG (
    'airflow_dag',
    default_args=default_args,
    description='ETL con Apache Airflow',
    schedule_interval=timedelta(minutes=1),
    start_date=days_ago(2),
    catchup=False,
    tags=['airflow dag']
) as dag:
    extract_task = PythonOperator(task_id="extract", python_callable=obtenerDatos)
    transform_task = PythonOperator(task_id="transform", python_callable=transformarDatos)
    load_task = PythonOperator(task_id="load", python_callable=almacenarDatos)

    extract_task >> transform_task >> load_task
