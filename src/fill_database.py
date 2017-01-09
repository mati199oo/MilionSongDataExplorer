import hdf5_getters as h5
import os
import sqlite3 as sql
import numpy as np
import pickle as pic

base_dir = "../../MillionSongSubset/data/"
db_file = "../resources/songs.db"

properties = [method.replace("get_", "") for method in dir(h5) if method.startswith("get") and not method.startswith("get_num")]
song_properties = [prop for prop in properties if not prop.startswith("artist") and not prop.startswith("similar_artists")]
artist_properties = [prop for prop in properties if prop.startswith("artist")]

def serialize(data):
    return sql.Binary(pic.dumps(data, pic.HIGHEST_PROTOCOL))

def get_property(property, data, song_index):
    return getattr(h5, "get_" + property)(data, song_index)

def add_data_to_songs_table(con, data, artist_id):
    values = [int(artist_id)]
    for property in song_properties:
        song_data = get_property(property, data, 0)

        if type(song_data) == np.ndarray:
            song_data = serialize(song_data)
        values.append(simple_type(song_data))

    command = "INSERT INTO Songs (artist_id "
    for property in song_properties:
        command += ", " + property

    command += ") VALUES ("

    is_first = True
    for value in values:
        if not is_first:
            command += ", ?"
        else:
            command += "?"
            is_first = False

    command += ");"
    cursor = con.cursor()
    cursor.execute(command, values)
    con.commit()

def add_data_to_artists_rel_table(con, data, song_index, artist_id):
    command = "INSERT INTO ArtistsRel (artist1_id, artist2_id) VALUES (?, ?);"
    similar_artists = h5.get_similar_artists(data, song_index)
    cursor = con.cursor()
    cursor.executemany(command, [(artist_id, similar_artist) for similar_artist in similar_artists])
    con.commit()

def simple_type(val):
    if type(val) == np.int32:
        return int(val)
    elif type(val) == np.string_:
        return str(val)
    elif type(val) == np.float64:
        return float(val)
    return val

def add_data_to_artists_table(con, data, song_index):
    values = []

    for property in artist_properties:
        artist_data = get_property(property, data, song_index)
        if type(artist_data) == np.ndarray:
            artist_data = serialize(artist_data)
        values.append(simple_type(artist_data))

    command = "INSERT INTO Artists ("
    is_first = True
    for property in artist_properties:
        if not is_first:
            command += ", " + property
        else:
            command += " " + property
            is_first = False

    command += ") VALUES ("

    is_first = True
    for _ in values:
        if not is_first:
            command += ", "
        else:
            is_first = False

        command += "?"

    command += ");"

    cursor = con.cursor()
    cursor.execute(command, values)
    con.commit()

    artist_id = get_artist_id(con, h5.get_artist_mbid(data, song_index))
    return artist_id

def get_artist_id(con, artist_mbid):
    query = "SELECT a_id FROM Artists WHERE artist_mbid like '" + str(artist_mbid) + "';"
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
        add_data_to_songs_table(con, data, artist_id)
    data.close()

def add_all():
    con = sql.connect(db_file)
    con.text_factory = str
    for directory1 in os.listdir(base_dir):
        for directory2 in os.listdir(os.path.join(base_dir, directory1)):
            for directory3 in os.listdir(os.path.join(base_dir, directory1, directory2)):
                for file in os.listdir(os.path.join(base_dir, directory1, directory2, directory3)):
                    full_path = os.path.join(base_dir, directory1, directory2, directory3, file)
                    add_to_database(con, full_path)

def main():
    add_all()

main()