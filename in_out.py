from spotify_playlist.datasets.tracks import Tracks

import pandas as pd

def in_out(previous_tracks, current_tracks):
  previous_tracks = previous_tracks.drop(Tracks.TRACK_ID, axis=1).drop_duplicates()
  current_tracks = current_tracks.drop(Tracks.TRACK_ID, axis=1).drop_duplicates()
  outer = previous_tracks.merge(current_tracks, how='outer', indicator=True)

  removed_artist = outer[(outer._merge=='left_only')].drop('_merge', axis=1)
  removed_artist["status"] = "out"
  new_artist = outer[(outer._merge=='right_only')].drop('_merge', axis=1)
  new_artist["status"] = "in"
  return pd.concat([removed_artist, new_artist])