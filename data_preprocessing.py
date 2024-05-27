import requests
import pandas as pd
import concurrent.futures


def get_spotify_token(client_id, client_secret):
    url = 'https://accounts.spotify.com/api/token'
    headers = {'Content-Type': 'application/x-www-form-urlencoded'}
    data = {'grant_type': 'client_credentials'}
    response = requests.post(url, headers=headers, data=data, auth=(client_id, client_secret))
    if response.status_code == 200:
        return response.json()['access_token']
    else:
        print(f"Failed to get token: {response.text}")
        return None


def get_track_url_and_album_cover(data):
    track_name, artist_name, token = data
    query = f"{track_name} artist:{artist_name}"
    url = f"https://api.spotify.com/v1/search?q={query}&type=track&limit=1"
    headers = {'Authorization': f'Bearer {token}'}
    response = requests.get(url, headers=headers)
    if response.status_code == 200 and response.json()['tracks']['items']:
        track = response.json()['tracks']['items'][0]
        return track['external_urls']['spotify'], track['album']['images'][0]['url']
    return None, None


# Load your dataset
file_path = r'C:\Users\ACER\OneDrive\Desktop\Spotify\spotify-2023.csv'
spotify_data = pd.read_csv(file_path, encoding='ISO-8859-1')

# Authenticate with Spotify
client_id = '1125f12df57945e98cbe3dfbbcec450f'
client_secret = 'b118f897702b41718088da90a3334059'
token = get_spotify_token(client_id, client_secret)

if token:
    # Prepare data for concurrent processing
    data_for_processing = [(row['track_name'], row['artist(s)_name'], token) for index, row in spotify_data.iterrows()]

    # Use ThreadPoolExecutor to make requests in parallel
    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
        results = list(executor.map(get_track_url_and_album_cover, data_for_processing))

    # Update dataframe with results
    spotify_data['track_url'], spotify_data['album_cover_url'] = zip(*results)

    # Save the enriched dataset
    enriched_file_path = r'C:\Users\ACER\OneDrive\Desktop\Spotify\enriched_spotify_2023.csv'
    spotify_data.to_csv(enriched_file_path, index=False)
    print(f"Enriched dataset saved to {enriched_file_path}")
else:
    print("Failed to authenticate with Spotify. Check your Client ID and Client Secret.")
