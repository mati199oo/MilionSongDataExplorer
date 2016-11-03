import hdf5_getters as h5
import os
import sqlite3 as sql
import numpy as np
import pickle as pic

base_dir = "../../../MillionSongSubset/data/"
db_file = "../resources/songs.db"

properties = [method.replace("get_", "") for method in dir(h5) if method.startswith("get") and not method.startswith("get_num")]
song_properties = [prop for prop in properties if not prop.startswith("artist") and not prop.startswith("similar_artists")]
artist_properties = [prop for prop in properties if prop.startswith("artist")]

def serialize(data):
    return pic.dumps(data)

def get_property(property, data, song_index):
    return getattr(h5, "get_" + property)(data, song_index)

def add_data_to_songs_table(con, data, song_index, artist_id):
    values = []

    for property in song_properties:
        song_data = get_property(property, data, song_index)
        if type(song_data) == np.ndarray:
            song_data = serialize(song_data)
        values.append(song_data)

    command = "INSERT INTO Songs (artist_id "
    for property in song_properties:
        command += ", " + property

    command += ") VALUES (" + str(artist_id)

    is_first = True
    for value in values:
        if not is_first:
            command += ", "
        else:
            is_first = False

        if type(value) == np.string_:
            command += ", '" + str(value) + "'"
        else:
            command += str(value)
    command += ");"

    cursor = con.cursor()
    cursor.execute(command)
    con.commit()

def add_data_to_artists_rel_table(con, data, song_index, artist_id):
    command = "INSERT INTO ArtistsRel (artist1_id, artist2_id) VALUES (" + str(artist_id) + ", "
    similar_artists = h5.get_similar_artists(data, song_index)
    for similar_artist_id in similar_artists:
        cursor = con.cursor()
        cursor.execute(command + "'" + str(similar_artist_id) + "');")
        con.commit()

def add_data_to_artists_table(con, data, song_index):
    values = []

    for property in artist_properties:
        artist_data = get_property(property, data, song_index)
        if type(artist_data) == np.ndarray:
            artist_data = serialize(artist_data)
        values.append(artist_data)

    command = "INSERT INTO Artists ("
    for property in artist_properties:
        command += property + ", "

    command += ") VALUES ("

    is_first = True
    for value in values:
        if not is_first:
            command += ", "
        else:
            is_first = False

        if type(value) == np.string_:
            command += "'" + str(value) + "'"
        else:
            command += str(value)
    command += ");"

    cursor = con.cursor()
    cursor.execute(command)
    con.commit()

    artist_id = get_artist_id(con, h5.get_artist_mbid(data, song_index))
    return artist_id

def get_artist_id(con, artist_mbid):
    query = "SELECT a_id FROM Artists WHERE artist_mbid = '" + str(artist_mbid) + "'"
    a_id = con.execute(query)
    for id in a_id:
        return id[0]
    return -1

def add_to_database(con, full_path):
    data = h5.open_h5_file_read(full_path)
    number_of_songs = h5.get_num_songs(data)
    for song_index in xrange(0, number_of_songs):
        artist_id = get_artist_id(con, h5.get_artist_mbid(data, song_index))
        if artist_id == -1:
            artist_id = add_data_to_artists_table(con, data, song_index)
            add_data_to_artists_rel_table(con, data, song_index, artist_id)
        add_data_to_songs_table(con, data, artist_id, song_index)
    data.close()

def add_all():
    con = sql.connect(db_file)
    for directory1 in os.listdir(base_dir):
        for directory2 in os.listdir(os.path.join(base_dir, directory1)):
            for directory3 in os.listdir(os.path.join(base_dir, directory1, directory2)):
                for file in os.listdir(os.path.join(base_dir, directory1, directory2, directory3)):
                    full_path = os.path.join(base_dir, directory1, directory2, directory3, file)
                    add_to_database(con, full_path)

def main():
    add_all()

main()