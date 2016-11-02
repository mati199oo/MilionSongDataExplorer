import hdf5_getters as h5
import os
import sqlite3 as sql
import numpy as np

base_dir = "../../data/"
db_file = "../resources/songs.db"

properties = [method.replace("get_", "") for method in dir(h5) if method.startswith("get") and not method.startswith("get_num")]
song_properties = [prop for prop in properties if not prop.startswith("artist") and not prop.startswith("similar_artists")]
artist_properties = [prop for prop in properties if prop.startswith("artist")]

def serialize(data):
    pass

def get_property(property, data, song_index):
    return getattr(h5, "get_" + property)(data, song_index)

def add_data_to_songs_table(con, data, song_index, artist_id):
    values = []

    for property in song_properties:
        song_data = get_property(property, data, song_index)
        if type(song_data) == np.ndarray:
            song_data = serialize(song_data)
        values.append(song_data)

    command = "INSERT INTO Songs (artist_id, "
    for property in song_properties:
        command += property + ", "

    command += ") VALUES (" + str(artist_id) + ", "
    for value in values:
        command += str(value) + ", "
    command += ");"

    cursor = con.cursor()
    cursor.execute(command)
    con.commit()

def add_data_to_artist_table(con, data, song_index):
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
    for value in values:
        command += str(value) + ", "
    command += ");"

    cursor = con.cursor()
    cursor.execute(command)
    con.commit()

    #jakims cudem wytrzasnac teraz artist_id ktore sie stworzylo
    artist_id = []
    return artist_id

def add_to_database(con, full_path):
    data = h5.open_h5_file_read(full_path)
    number_of_songs = h5.get_num_songs(data)
    for song_index in xrange(0, number_of_songs):
        #tutaj jakos sprawdzac czy dany artysta czasem nie istnieje i jego artist_id jakos wyciagac i podawac do piosenki
        artist_id = add_data_to_artist_table(con, data)
        add_data_to_songs_table(con, data, artist_id)
        #tutaj na koncu zrobic jakos Artists_Rel
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