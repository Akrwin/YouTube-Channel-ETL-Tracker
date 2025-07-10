import requests

API_KEY = 'AIzaSyBmB22K_aBeXTvSM7dnJQjS8tGeWifZTo4'
CHANNEL_ID = 'UCNzsfB5c0Lo3gpT8PO35RPg'  # Example: Google Developers

def get_channel_stats():
    url = (
        f'https://www.googleapis.com/youtube/v3/channels'
        f'?part=snippet,statistics&id={CHANNEL_ID}&key={API_KEY}'
    )
    response = requests.get(url)
    data = response.json()

    print("📊 Channel Title:", data['items'][0]['snippet']['title'])
    print("👥 Subscribers:", data['items'][0]['statistics']['subscriberCount'])

if __name__ == "__main__":
    get_channel_stats()