# Spotify playlist

## Init
- modify `playlists.csv` file with a playlist id on each line on `./spotify_playlist/CSV` folder
- run `hive -f ./spotify_playlist/create_tables.hql`
- set global variables `SPOTIFY_CLIENT_ID` and `SPOTIFY_CLIENT_SECRET` on `./spotify_playlist/dag_spotify.dag` to your spotify application credentials
- set global variable `path_home_user` on `./spotify_playlist/dag_spotify.dag` to your home folder
- copy `./spotify_playlist/dag_spotify.dag` on your dag folder

## run
- run `dag_spotify.dag` on your dag folder

