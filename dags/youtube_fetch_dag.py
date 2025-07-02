from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import http.client
from datetime import datetime , timedelta
import json
import requests

# YouTube API config
API_KEY = 'AIzaSyBmB22K_aBeXTvSM7dnJQjS8tGeWifZTo4'
CHANNEL_ID = 'UCNzsfB5c0Lo3gpT8PO35RPg'  # Change this if needed

# def fetch_youtube_data():
    # try:
    #     url = (
    #         f'https://www.googleapis.com/youtube/v3/channels'
    #         f'?part=snippet,statistics&id={CHANNEL_ID}&key={API_KEY}'
    #     )
    #     response = requests.get(url)
    #     print("✅ This task ran successfully.")
    #     data = response.json()
    #     print(data)

    #     if 'items' in data and len(data['items']) > 0:
    #         title = data['items'][0]['snippet']['title']
    #         subscribers = data['items'][0]['statistics']['subscriberCount']
    #         print(f"📊 Channel: {title}")
    #         print(f"👥 Subscribers: {subscribers}")
    #     else:
    #         print("❌ No data found.")
    #         print("🔍 Response:", data)
    # except Exception as e:
    #     print("⚠️ Error during fetch:", str(e))
def fetch_youtube_data():
    try:
        conn = http.client.HTTPSConnection("www.googleapis.com")
        endpoint = f"/youtube/v3/channels?part=snippet,statistics&id=UCNzsfB5c0Lo3gpT8PO35RPg&key=AIzaSyBmB22K_aBeXTvSM7dnJQjS8tGeWifZTo4"
        conn.request("GET", endpoint)
        res = conn.getresponse()
        data = res.read()

        # Just print the size to avoid memory crash
        print("✅ Fetched data size:", len(data))

        # Optional: parse JSON
        parsed = json.loads(data)
        print("📦 Parsed title:", parsed["items"][0]["snippet"]["title"])

        if 'items' in data and len(data['items']) > 0:
            title = data['items'][0]['snippet']['title']
            subscribers = data['items'][0]['statistics']['subscriberCount']
            print(f"📊 Channel: {title}")
            print(f"👥 Subscribers: {subscribers}")
        else:
            print("❌ No data found.")
            print("🔍 Response:", data)
    except Exception as e:
        print("❌ Error:", e)

# Define DAG
with DAG(
    dag_id='youtube_fetch_dag',
    start_date=datetime(2024, 1, 1),
    schedule='@daily',
    catchup=False,
    tags=['youtube', 'api'],
) as dag:
    fetch_task = PythonOperator(
        task_id='fetch_youtube_data_task',
        python_callable=fetch_youtube_data,
    )