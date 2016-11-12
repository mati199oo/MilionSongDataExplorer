import sqlite3 as sql

db_file = "../resources/songs.db"

def get_similar_artists(con, artist_name):
    query = "SELECT ar.artist2_id FROM ArtistsRel AS ar WHERE ar.artist1_id = '" + str(artist_name) + "';"
    results = con.execute(query)
    similar_artists = []
    for result in results:
        similar_artists.append(result[0])
    return similar_artists

def get_all_other_songs_data(con, song_name):
    query = "SELECT s.title, s.year, s.release, s.song_hotttnesss, s.danceability, s.duration, " \
            "s.loudness, s.end_of_fade_in, s.start_of_fade_out, s.key, s.energy, s.mode, s.tempo, " \
            "s.time_signature, a.artist_name, a.artist_familiarity, a.artist_hotttnesss, a.artist_latitude, " \
            "a.artist_location, a.artist_longitude, a.artist_id, a.a_id FROM Songs AS s inner join Artists AS a ON " \
            "s.artist_id = a.a_id WHERE s.title <> '" + str(song_name) + "';"
    results = con.execute(query)
    for result in results:
        return result
    return None

def get_song_data(con, song_name):
    query = "SELECT s.title, s.year, s.release, s.song_hotttnesss, s.danceability, s.duration, " \
            "s.loudness, s.end_of_fade_in, s.start_of_fade_out, s.key, s.energy, s.mode, s.tempo, " \
            "s.time_signature, a.artist_name, a.artist_familiarity, a.artist_hotttnesss, a.artist_latitude, " \
            "a.artist_location, a.artist_longitude, a.artist_id, a.a_id FROM Songs AS s inner join Artists AS a ON " \
            "s.artist_id = a.a_id WHERE s.title = '" + str(song_name) + "';"
    results = con.execute(query)
    for result in results:
        return result
    return None

def print_similar_songs(con, song_name):
    song_data = get_song_data(con, song_name)
    songs_data = get_all_other_songs_data(con, song_name)
    similar_artists = get_similar_artists(con, song_data[21])
    #tutaj dorobic reszte procedury ktora ma wypisac n najblizszych wynikow

def print_albums_songs_count(con):
    query = "SELECT s.release, count(*) FROM Songs AS s GROUP BY s.release;"
    results = con.execute(query)
    for result in results :
        print str(result[0]) + " - " + str(result[1]) + " song(s)"

def print_artists_albums_count(con):
    query = "SELECT temp.artist_name, count(*) FROM (SELECT a.artist_name, s.release FROM Artists AS a " \
            "INNER JOIN Songs AS s ON a_id = s.artist_id GROUP BY a.artist_name, s.release) AS temp GROUP BY temp.artist_name;"
    results = con.execute(query)
    for result in results :
        print str(result[0]) + " - " + str(result[1]) + " album(s)"

def print_artists_songs_count(con):
    query = "SELECT a.artist_name, count(*) FROM Artists AS a INNER JOIN Songs AS s ON a_id = s.artist_id GROUP BY a.artist_name;"
    results = con.execute(query)
    for result in results :
        print str(result[0]) + " - " + str(result[1]) + " song(s)"

def print_database_statistics(con):
    print_artists_songs_count(con)
    print_artists_albums_count(con)
    print_albums_songs_count(con)

def main():
    con = sql.connect(db_file)
    con.text_factory = str
    print_database_statistics(con)

    song_name = "The Deceived"
    print_similar_songs(con, song_name)

main()