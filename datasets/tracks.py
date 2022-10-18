import pandas as pd


class Tracks:
  PLAYLIST_ID = "playlist_id"
  TRACK_ID = "track_id"
  ARTIST_ID = "artist_id"
  DATE = "date"

  @staticmethod
  def write(tracks_l, path):
    pd.DataFrame(tracks_l).to_csv(path, index=False)

  @staticmethod
  def read(path):
    return pd.read_csv(path)
