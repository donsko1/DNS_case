import datetime as dt
import pandas as pd
import gc
from airflow.models import DAG
from airflow.operators.python import PythonOperator

args = {
    'owner': 'airflow',
    'start_date': dt.datetime(2024, 7, 14),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=1),
    'depends_on_past': False,
}

def data_processing():
    df_dp = pd.read_csv("data/quality_status/dim_product.csv", usecols=lambda column : column != 'Unnamed: 0')
    df_s = pd.read_csv("data/quality_status/sales.csv", usecols=lambda column: column != 'Unnamed: 0')
    df_s['date'] = pd.to_datetime(df_s['date'])
    last_date = df_s['date'].max()
    one_year_ago = last_date - pd.DateOffset(years=1)
    df_s = df_s[df_s['date'] >= one_year_ago]
    df_grouped = df_s.groupby('fk_product').agg({'solds' : 'sum', 'defects_entry_period_sale': 'sum'}).reset_index()
    df_merged = df_grouped.merge(df_dp[['fk_product', 'product', 'type']], on='fk_product', how='left')
    df = df_merged[df_merged['type'] == 'Товар']
    del df['type']
    df = df[(df['defects_entry_period_sale'] <= df['solds']) & (df['solds'] != 0)]
    df['percent_defects'] = round(df['defects_entry_period_sale'] / df['solds'] * 100, 2)
    df.to_csv('data/df_with_percent.csv', index=False)

def assessment():
    df = pd.read_csv('data/df_with_percent.csv')
    filtered_df = df.loc[(df['solds'] > 100) & (df['percent_defects'] > 0)]
    mode_df = filtered_df.groupby('product')['percent_defects'].apply(lambda x: x.mode()[0]).reset_index()
    mode_df.columns = ['product', 'mode_percent_defects']
    grouped_df = filtered_df.groupby('product')['percent_defects'].agg(['median', 'mean']).reset_index()
    grouped_df = pd.merge(grouped_df, mode_df, on='product', how='left')
    result_df = pd.merge(df, grouped_df, on='product', how='left')
    result_df['grade'] = result_df.apply(lambda row: "не достаточно данных" if row['solds'] < 100 else ("плохое качество товара" if row['percent_defects'] > row['mean'] else "хорошее качество товара"), axis=1)
    res_df = result_df[['fk_product', 'percent_defects', 'grade']]
    res_df.to_csv('data/result.csv', index=False)

with DAG(
    dag_id = 'DNS_case',
    schedule_interval = "00 15 * * *",
    default_args = args
) as dag:
    data_processing_task = PythonOperator(
        task_id='data_processing',
        python_callable=data_processing,
        dag=dag
    )

    assessment_task = PythonOperator(
        task_id='assessment',
        python_callable=assessment,
        dag=dag
    )

    data_processing_task >> assessment_task