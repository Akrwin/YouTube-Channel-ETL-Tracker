from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from googleapiclient.discovery import build
from googleapiclient.discovery import HttpError
from datetime import timezone , datetime , timedelta
import pandas as pd 
import logging
import json
import duckdb

# YouTube API config
API_KEY = 'AIzaSyBmB22K_aBeXTvSM7dnJQjS8tGeWifZTo4'
CHANNEL_ID = 'UCoiEtD4v1qMAqHV5MDI5Qpg'  # Change this if needed
OUTPUT_PATH = '/tmp/youtube_data.json'
CSV_OUTPUT_PATH = '/tmp/youtube_data.csv'

def get_youtube_service():
    """Initialize Youtube API service"""
    try:
        youtube = build('youtube','v3',developerKey= API_KEY)
        logging.info("Youtube API service initialized successfully")
        return youtube
    except Exception as e:
        logging.error(f"Failed to initialize Youtube API service: {str(e)}")
        raise

def extract_channel_info(**context):
    """Extract basic channel information"""
    youtube = get_youtube_service()

    try:
        # Get channel statistics
        channel_request = youtube.channels().list(
            part='snippet,statistics,contentDetails',
            id=CHANNEL_ID
        )
        channel_response = channel_request.execute()

        if not channel_response['items']:
            raise ValueError(f"Channel with ID {CHANNEL_ID} not found")
        
        channel_data = channel_response['items'][0]
        
        # Extract relevant information
        channel_info = {
            'channel_id': channel_data['id'],
            'channel_name': channel_data['snippet']['title'],
            'description': channel_data['snippet']['description'],
            'subscriber_count': channel_data['statistics'].get('subscriberCount',0),
            'video_count': channel_data['statistics'].get('videoCount',0),
            'view_count': channel_data['statistics'].get('viewCount',0),
            'published_at': channel_data['snippet']['publishedAt'],
            'extraction_date': datetime.now().isoformat()
        }

        logging.info(f"channel info extracted: {channel_info['channel_name']}")

        # Push data to XCom for downstream tasks
        context['task_instance'].xcom_push(key='channel_info',value=channel_info)

        return channel_info
    
    except HttpError as e:
        logging.error(f"HTTP error occured: {e}")
        raise
    except Exception as e:
        logging.error(f"An error occured while extracting channel info: {e}")
        raise

def extract_recent_videos(**context):
    """Extract recent videos from the channel with pagination"""
    youtube = get_youtube_service()
    
    # Configuration
    MAX_RESULTS_PER_REQUEST = 50  # YouTube API limit
    TOTAL_VIDEOS_DESIRED = 1000

    try:
        # Get channel's uploads playlist ID
        channel_request = youtube.channels().list(
            part='contentDetails',
            id=CHANNEL_ID
        )
        channel_response = channel_request.execute()

        uploads_playlist_id = channel_response['items'][0]['contentDetails']['relatedPlaylists']['uploads']

        videos_data = []
        next_page_token = None
        
        while len(videos_data) < TOTAL_VIDEOS_DESIRED:
            # Calculate how many results to request this time
            remaining_videos = TOTAL_VIDEOS_DESIRED - len(videos_data)
            max_results_this_request = min(MAX_RESULTS_PER_REQUEST, remaining_videos)
            
            # Get videos from uploads playlist
            playlist_request = youtube.playlistItems().list(
                part='snippet,contentDetails',
                playlistId=uploads_playlist_id,
                maxResults=max_results_this_request,
                pageToken=next_page_token
            )
            playlist_response = playlist_request.execute()

            # Process videos in this batch
            for item in playlist_response['items']:
                video_id = item['contentDetails']['videoId']

                # Get detailed video statistics
                video_request = youtube.videos().list(
                    part='snippet,statistics,contentDetails',
                    id=video_id
                )
                video_response = video_request.execute()

                if video_response['items']:
                    video_details = video_response['items'][0]

                    video_info = {
                        'video_id': video_id,
                        'title': video_details['snippet']['title'],
                        'description': video_details['snippet']['description'][:500],
                        'published_at': video_details['snippet']['publishedAt'],
                        'duration': video_details['contentDetails']['duration'],
                        'view_count': video_details['statistics'].get('viewCount', 0),
                        'like_count': video_details['statistics'].get('likeCount', 0),
                        'comment_count': video_details['statistics'].get('commentCount', 0),
                        'tags': video_details['snippet'].get('tags', []),
                        'category_id': video_details['snippet'].get('categoryId'),
                        'extraction_date': datetime.now().isoformat()
                    }

                    videos_data.append(video_info)

            # Check if there are more pages
            next_page_token = playlist_response.get('nextPageToken')
            if not next_page_token:
                # No more pages available
                break
                
            logging.info(f"Retrieved {len(videos_data)} videos so far...")

        logging.info(f"Extracted {len(videos_data)} videos total")

        # Push data to XCom
        context['task_instance'].xcom_push(key='videos_data', value=videos_data)

        return videos_data

    except HttpError as e:
        logging.error(f"HTTP error occurred: {e}")
        raise
    except Exception as e:
        logging.error(f"An error occurred while extracting videos: {e}")
        raise

def save_data_to_json(**context):
    """Save extracted data to JSON file"""
    try:
        # Get data form XCom
        channel_info = context['task_instance'].xcom_pull(task_ids='extract_channel_info',key='channel_info')
        videos_data = context['task_instance'].xcom_pull(task_ids='extract_recent_videos',key='videos_data')

        # Combine all data
        combined_data = {
            'channel_info': channel_info,
            'videos':videos_data,
            'extraction_metadata':{
                'total_videos': len(videos_data),
                'extraction_timestamp': datetime.now().isoformat()
            }
        }

        # Save to JSON file
        with open(OUTPUT_PATH,'w',encoding='utf-8') as f:
            json.dump(combined_data,f,indent=2,ensure_ascii=False)

        logging.info(f"Data saved to {OUTPUT_PATH}")

    except Exception as e:
        logging.error(f"Error saving data to JSON {e}")
        raise

def convert_to_csv(**context):
    """Convert video data to CSV format"""
    try:
        # Get videos data from XCom
        videos_data = context['task_instance'].xcom_pull(task_ids='extract_recent_videos', key='videos_data')
        
        # Convert to DataFrame
        df = pd.DataFrame(videos_data)
        
        # Clean and process data
        if not df.empty:
            # Convert numeric columns
            numeric_columns = ['view_count', 'like_count', 'comment_count']
            for col in numeric_columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
            
            # Convert published_at to datetime
            df['published_at'] = pd.to_datetime(df['published_at'])
            
            # Handle tags column (convert list to string)
            df['tags'] = df['tags'].apply(lambda x: ', '.join(x) if isinstance(x, list) else '')
            
            # Save to CSV
            df.to_csv(CSV_OUTPUT_PATH, index=False, encoding='utf-8')
            
            logging.info(f"CSV file saved to {CSV_OUTPUT_PATH} with {len(df)} records")
        
        return CSV_OUTPUT_PATH
        
    except Exception as e:
        logging.error(f"Error converting to CSV: {e}")
        raise

def data_quality_check(**context):
    """Perform basic data quality checks"""
    try:
        videos_data = context['task_instance'].xcom_pull(task_ids='extract_recent_videos', key='videos_data')
        
        if not videos_data:
            raise ValueError("No video data found")
        
        # Basic quality checks
        checks = {
            'total_videos': len(videos_data),
            'videos_with_views': sum(1 for video in videos_data if int(video.get('view_count', 0)) > 0),
            'videos_with_likes': sum(1 for video in videos_data if int(video.get('like_count', 0)) > 0),
            'recent_videos': sum(1 for video in videos_data 
                               if datetime.fromisoformat(video['published_at'].replace('Z', '+00:00')) > datetime.now(timezone.utc) - timedelta(days=30)
                               )
        }
        
        logging.info(f"Data quality check results: {checks}")
        
        # Set minimum thresholds
        if checks['total_videos'] < 5:
            logging.warning("Low number of videos extracted")
        
        if checks['videos_with_views'] == 0:
            raise ValueError("No videos with view data found")
        
        return checks
        
    except Exception as e:
        logging.error(f"Data quality check failed: {e}")
        raise

def load_data_into_duckdb(**context):
    """load a CSV into DuckDB using either SQL syntax """
    csv_path = '/tmp/youtube_data.csv'
    db_path = 'data/youtube_data.duckdb'

    con = duckdb.connect(db_path)

    # ✅ 1. Create the table if it doesn't exist
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS youtube_data AS 
        SELECT * FROM read_csv_auto('{csv_path}') LIMIT 0
    """)

    # ✅ 2. Truncate the table (delete all rows)
    con.execute("DELETE FROM youtube_data")

    # ✅ 3. Insert fresh data
    con.execute(f"""
        INSERT INTO youtube_data 
        SELECT * FROM read_csv_auto('{csv_path}')
    """)

    print("✅ Table truncated and new data inserted successfully.")


# Define DAG
with DAG(
    dag_id='youtube_fetch_dag',
    start_date=datetime(2024, 1, 1),
    schedule='@daily',
    catchup=False,
    tags=['youtube', 'api'],
) as dag:
    
    extract_channel_task = PythonOperator(
        task_id='extract_channel_info',
        python_callable=extract_channel_info
    )

    extract_videos_task = PythonOperator(
        task_id='extract_recent_videos',
        python_callable=extract_recent_videos
    )

    save_json_task = PythonOperator(
        task_id='save_data_to_json',
        python_callable=save_data_to_json    
    )

    convert_csv_task = PythonOperator(
        task_id='convert_to_csv',
        python_callable=convert_to_csv    
    )

    quality_check_task = PythonOperator(
        task_id='data_quality_check',
        python_callable=data_quality_check
    )

    load_data_into_duckdb_task = PythonOperator(
        task_id='load_data_into_duckdb',
        python_callable=load_data_into_duckdb
    )

    # Optional: Clean up temporary files
    cleanup_task = BashOperator(
        task_id='cleanup_temp_files',
        bash_command='echo "Pipeline completed successfully" && ls -la /tmp/youtube_data.*'
    )

# Define task dependencies
extract_channel_task >> extract_videos_task >> [save_json_task, convert_csv_task, quality_check_task] >> load_data_into_duckdb_task
load_data_into_duckdb_task >> cleanup_task