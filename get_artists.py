from api.spotify import Spotify
from datasets.tracks import Tracks
from datasets.artists import Artists
import pandas as pd
import datetime


def distinct_artist_id(tracks):
  return pd.unique(tracks[Tracks.ARTIST_ID])


def parse_artist(extract_date, artist_id, artist):
  return {
    Artists.NAME: artist.get("name"),
    Artists.ID: artist_id,
    Artists.DATE: extract_date,
    Artists.POPULARITY: artist.get("popularity"),
  }

def main(current_date = datetime.datetime.now().date()):
  spotify_client = Spotify()
  spotify_client.authenticate()

  tracks = Tracks.read(f"./CSV/tracks_{current_date.isoformat()}.csv")

  artists = []
  for artist_id in distinct_artist_id(tracks):
      artist = spotify_client.artist(artist_id)
      artist_info = parse_artist(current_date, artist_id, artist)
      artists.append(artist_info)

  Artists.write(artists, f"./CSV/artists_{current_date.isoformat()}.csv")

if __name__ == "__main__":
  main()
