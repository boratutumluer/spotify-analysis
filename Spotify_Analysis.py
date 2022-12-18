import pandas as pd
import numpy as np
import glob
import warnings
import json
import requests
import asyncio
import datetime as dt
import psycopg2 as pg
from sqlalchemy import create_engine


#################################################################
# ADJUSTMENTS
#################################################################

warnings.filterwarnings("ignore")
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', 250)

#################################################################
# IMPORT DATA
#################################################################

all_files = glob.glob(r'datasets\my_spotify_data\MyData' + "/*.json")
file_list = [pd.read_json(f) for f in all_files if "Stream" in f]
df_stream = pd.concat(file_list, axis=0, ignore_index=True)

#################################################################
# DATABASE CONNECTIONS
#################################################################

try:
    host, dbname, user, password, port = "localhost", "postgis", "postgres", "******", "5432"
    conn = pg.connect(f"host='{host}' dbname='{dbname}' user='{user}' password='{password}' port='{port}'")
    conn.set_client_encoding('UTF8')
    cur = conn.cursor()
    engine = create_engine("postgresql+psycopg2://postgres:******@localhost:5432/postgis")
except Exception as ex:
    print("ERROR ==>  {}".format(ex))

#################################################################
# SPOTIF API
#################################################################

# authentication URL
AUTH_URL = 'https://accounts.spotify.com/api/token'

# POST
auth_response = requests.post(AUTH_URL, {
    'grant_type': 'client_credentials',
    'client_id': "ecbb39d0cbd340b0b3a12f347936116c",
    'client_secret': "8dc862d11c154bd0ad6f4054131c3c1c",
})

# convert the response to JSON
auth_response_data = auth_response.json()
# save the access token
access_token = auth_response_data['access_token']
# used for authenticating all API calls
headers = {'Authorization': 'Bearer {token}'.format(token=access_token)}
# base URL of all Spotify API endpoints
BASE_URL = 'https://api.spotify.com/v1/'



df_stream.insert(4, "uniqueid", df_stream["artistname"] + ':' +  df_stream["trackname"])
df_stream.columns = [col.lower() for col in df_stream.columns]
df_stream["mnplayed"] = round(df_stream["msplayed"] / 60000, 2)
df_stream["hour"] = df_stream["endtime"].apply(lambda x: round(dt.datetime.strptime(x, '%Y-%m-%d %H:%M').hour +
                                                         (dt.datetime.strptime(x, '%Y-%m-%d %H:%M').minute / 60)))
df_stream["artisturi"], df_stream["trackuri"], df_stream["genres"] = np.nan, np.nan, np.nan
df_stream.to_sql("stream", con=engine, if_exists="append")



#################################################################
# GET URI and GENRE
#################################################################

async def getURI(artist, track):
    r = requests.get(BASE_URL + 'search?query=remaster%2520track%3A' + track + '%2520artist%3A' + artist +
                     '&type=artist,track', headers=headers)
    data = r.json()
    artisturi = data["tracks"]["items"][0]["artists"][0]["uri"].split("artist:")[1]
    trackuri = data["tracks"]["items"][0]["uri"].split("track:")[1]
    genres = data["artists"]["items"][0]["genres"]
    return artisturi, trackuri, genres

#################################################################
# STORING RECORDS IN DATABASE
#################################################################

for i in range(0 , len(df_stream)):
    try:
        artisturi = asyncio.run(getURI(df_stream.iloc[i, 1], df_stream.iloc[i, 2]))[0]
        trackuri = asyncio.run(getURI(df_stream.iloc[i, 1], df_stream.iloc[i, 2]))[1]
        genres = asyncio.run(getURI(df_stream.iloc[i, 1], df_stream.iloc[i, 2]))[2]
        cur.execute("UPDATE stream SET artisturi = '{}', trackuri = '{}', genre = '{}' WHERE index = {};".format(
            artisturi, trackuri, genres))
        conn.commit()
    except (KeyError, ValueError, TypeError):
        continue



# get column name from database:
cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name = 'stream' ORDER BY ordinal_position;")
columns = cur.fetchall()
columns = [col[0] for col in columns]
columns.append("genres")

# new df_stream:
df_stream = pd.DataFrame.from_records(data, columns=columns).drop("index", axis=1)
df_stream.drop("genre", axis=1, inplace=True)
df_stream = df_stream.explode("genres")
df_stream.insert(0, "index", df_stream.index)
df_stream.reset_index(inplace=True, drop=True)
df_stream.head(100)

# we can drop song records that haven't listened
df_stream[df_stream["mnplayed"] == 0]["mnplayed"].count() # 519 records 0 minute
index_of_zeros = df_stream[df_stream["mnplayed"] == 0].index
df_stream = df_stream.drop(index_of_zeros, axis=0)

# find null genre records:
df_stream["genres"].isnull().sum() # 2002 records

# Matching the empty genre records with the artist name as a result of no response when sending a request,
# and matching if there is a full genre record that returns a response
df_genre_null = df_stream[df_stream["genres"].isnull()]
df_genre_not_null = df_stream[~df_stream["genres"].isnull()].drop_duplicates(subset="artistname")
merge_df = pd.merge(df_genre_null, df_genre_not_null, how="inner", on="artistname")
merge_df["genres_x"].fillna(merge_df["genres_y"], inplace=True)

index_of_null_genre = df_genre_null["index"].values
for index_df in index_of_null_genre:
    for index_merge in merge_df["index_x"].values:
        if index_df == index_merge:
            df_stream.loc[df_stream["index"] == index_df, "genres"] = merge_df.loc[merge_df["index_x"] == index_merge, "genres_x"].values[0]

df_stream["genres"].isnull().sum() # 731 records matched but not filled, we can drop those records
df_stream.dropna(subset="genres", how="all", inplace=True)


# for tableau
df_stream.to_csv("MyStream.csv")
