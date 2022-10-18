import requests
import os


class Spotify:
    AUTH_URL = "https://accounts.spotify.com/api/token"
    BASE_URL = "https://api.spotify.com/v1"

    def __init__(self, client_id=None, client_secret=None) -> None:
        self.client_id = os.getenv("SPOTIFY_CLIENT_ID", client_id)
        self.client_secret = os.getenv("SPOTIFY_CLIENT_SECRET", client_secret)

    def authenticate(self):
        # POST
        auth_response = requests.post(self.AUTH_URL, self.authenticate_body())
        self.set_access_token(auth_response.json())

    def authenticate_body(self):
        return {
            "grant_type": "client_credentials",
            "client_id": self.client_id,
            "client_secret": self.client_secret,
        }

    def set_access_token(self, auth_response):
        self.access_token = auth_response["access_token"]

    def auth_headers(self):
        return {"Authorization": f"Bearer {self.access_token}"}

    def playlist_tracks(self, playlist_id):
        print(f"{self.BASE_URL}/playlists/{playlist_id}/tracks")
        print(self.auth_headers())
        r = requests.get(
            f"{self.BASE_URL}/playlists/{playlist_id}/tracks", headers=self.auth_headers()
        )
        if r.status_code == 200:
          return r.json()
        else:
          raise  Exception(f"Error while getting playlist {playlist_id}: {r.status_code} - {r.json()}")

    def artist(self, artist_id):
        r = requests.get(
            f"{self.BASE_URL}/artists/{artist_id}", headers=self.auth_headers()
        )
        if r.status_code == 200:
          return r.json()
        else:
          raise  Exception(f"Error while getting artist {artist_id}: {r.status_code} - {r.json()}")
