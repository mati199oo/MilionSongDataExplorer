import hdf5_getters as h5
import sqlite3 as sql
import numpy as np

example_song = "../resources/TRAAFMS128F933AB23.h5"
db_file = "../resources/songs.db"

properties = [method.replace("get_", "") for method in dir(h5) if method.startswith("get") and not method.startswith("get_num")]
song_properties = [prop for prop in properties if not prop.startswith("artist") and not prop.startswith("similar_artists")]
artist_properties = [prop for prop in properties if prop.startswith("artist")]

def get_property(property, data):
    return getattr(h5, "get_" + property)(data)

def create_rel_table(con, command):
    cursor = con.cursor()
    cursor.execute(command)
    con.commit()

def create_table(data, con, command, properties):
    for property in properties:
        property_type = type(get_property(property, data))
        if property_type == np.string_ or property_type == np.ndarray:
            command += ", " + property + " TEXT"
        elif property_type == np.int32:
            command += ", " + property + " INT"
        elif property_type == np.float64:
            command += ", " + property + " REAL"

    command += ");"
    cursor = con.cursor()
    cursor.execute(command)
    con.commit()

def create_db():
    songs_command = "CREATE TABLE IF NOT EXISTS Songs(s_id INT PRIMARY KEY NOT NULL, artist_id INT"
    artists_command = "CREATE TABLE IF NOT EXISTS Artists(a_id INT PRIMARY KEY NOT NULL"
    artists_rel_command = "CREATE TABLE IF NOT EXISTS ArtistsRel(artist1_id INT PRIMARY KEY NOT NULL, artist2_id INT NOT NULL);"
    con = sql.connect(db_file)
    data = h5.open_h5_file_read(example_song)
    create_table(data, con, songs_command, song_properties)
    create_table(data, con, artists_command, artist_properties)
    create_rel_table(con, artists_rel_command)
    data.close()

def main():
    create_db()

main()