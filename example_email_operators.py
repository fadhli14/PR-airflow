from airflow.utils.dates import days_ago
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.email_operator import EmailOperator

from pprint import pprint
import pandas as pd

args = {
    'owner': 'Airflow',
    'start_date': days_ago(2),
}

dag = DAG(
    dag_id='example_email_operators',
    default_args=args,
    schedule_interval=None,
    tags=['example']
)

def print_context(ds, **kwargs):
    pprint(kwargs)
    print(ds)
    return 'Whatever you return gets printed in the logs'

def orangterkaya_2020():
    url='https://id.wikipedia.org/wiki/Daftar_orang_terkaya_di_Indonesia'
    dataframe=pd.read_html(url)
    df=dataframe[7]
    df.to_csv('list_orang_terkaya_di_indonesia.csv')
orangterkaya_2020()


cetak_context = PythonOperator(
    task_id='print_the_context',
    provide_context=True,
    python_callable=print_context,
    dag=dag,
)

send_email = EmailOperator(
        task_id='send_email',
        to='fia.digitalskola@gmail.com',
        subject='Fadhli_DigitalSkola_Airflow',
        html_content=""" <h3>Email Test</h3> """,
        files=['list_orang_terkaya_di_indonesia.csv'],
        dag=dag
)

cetak_context >> send_email
