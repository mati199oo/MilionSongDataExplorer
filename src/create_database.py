import hdf5_getters as h5
import sqlite3 as sql
import numpy as np

example_song = "../resources/TRALLQX12903CD838C.h5"
db_file = "../resources/songs.db"

properties = [method.replace("get_", "") for method in dir(h5) if method.startswith("get")]
song_properties = [prop for prop in properties if not prop.startswith("artist") and not prop.startswith("similar_artists")]
artist_properties = [prop.replace("artist_", "") for prop in properties if prop.startswith("artist") or prop.startswith("similar_artists")]

def get_song_property(property, data):
    return getattr(h5, "get_" + property)(data)

def get_artist_property(property, data):
    if property == "similar_artists":
        return getattr(h5, "get_similar_artists")(data)
    return getattr(h5, "get_artist_" + property)(data)

def create_rel_artists_table(data, con):
    artists_rel_command = "CREATE TABLE IF NOT EXISTS ArtistsRel(artist1_id INT PRIMARY KEY NOT NULL, artist2_id INT NOT NULL)"
    print type(get_artist_property("similar_artists", data)[0])
    cursor = con.cursor()
    cursor.execute(artists_rel_command)
    con.commit()

def create_artists_table(data, con):
    artists_command = "CREATE TABLE IF NOT EXISTS Artists(a_id INT PRIMARY KEY NOT NULL"
    print artist_properties
    for p in artist_properties:
        property_type = type(get_artist_property(p, data))
        if p.startswith('7digitalid'):
            p = 'seven_digital_id'
        if property_type == np.ndarray:
            continue
        elif property_type == np.string_:
            artists_command += ", " + p + " TEXT"
        elif property_type == np.int32:
            artists_command += ", " + p + " INT"
        elif property_type == np.float64:
            artists_command += ", " + p + " REAL"

    artists_command += ")"
    print artists_command
    cursor = con.cursor()
    cursor.execute(artists_command)
    con.commit()

def create_songs_table(data, con):
    songs_command = "CREATE TABLE IF NOT EXISTS Songs(s_id INT PRIMARY KEY NOT NULL"
    for p in song_properties:
        propertyType =  type(get_song_property(p, data))
        if propertyType == np.ndarray:
            continue
        elif propertyType == np.string_:
            songs_command += ", " + p + " TEXT"
        elif propertyType == np.int32:
            songs_command += ", " + p + " INT"
        elif propertyType == np.float64:
            songs_command += ", " + p + " REAL"

    songs_command += ")"
    print songs_command
    cursor = con.cursor()
    cursor.execute(songs_command)
    con.commit()

def create_db():
    con = sql.connect(db_file)
    data = h5.open_h5_file_read(example_song)
    print song_properties
    create_songs_table(data, con)
    create_artists_table(data, con)
    create_rel_artists_table(data, con)
    data.close()

def main():
    create_db()

main()