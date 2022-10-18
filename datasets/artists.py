import pandas as pd


class Artists:
  ID = "id"
  NAME = "name"
  POPULARITY = "popularity"
  DATE = "date"

  def write(artists, path):
    pd.DataFrame(artists).to_csv(path, index=False)

  def read(path):
    return pd.read_csv(path)
