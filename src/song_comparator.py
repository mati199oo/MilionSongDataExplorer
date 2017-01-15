import sqlite3 as sql
import matplotlib.pyplot as plt
import numpy as np
from difflib import SequenceMatcher

db_file = "../resources/songs.db"

def visualize(authors_in_clusters):
    x = []
    y = []
    for author in authors_in_clusters:
        x.append(author[0])
        y.append(author[1])

    y_pos = np.arange(len(x))
    plt.bar(y_pos, y, align='center', alpha=1.0)
    plt.xticks(y_pos, x)
    plt.ylabel('Number of clusters')
    plt.title('Number of clusters author is in')

    plt.show()

def find_authors_in_clusters(authors, clusters):
    authors_in_clusters = []
    for author in authors:
        number_of_clusters = 0
        for cluster in clusters:
            is_in_cluster = False
            for song in cluster:
                if not is_in_cluster and song[1] == author:
                    number_of_clusters += 1
                    is_in_cluster = True
        authors_in_clusters.append([author, number_of_clusters])
    return authors_in_clusters

def visualize_authors(clusters):
    authors = ["Bullet For My Valentine", "Trivium", "Slipknot", "Britt Nicole", "Madonna", "Metallica",
               "Jessica Simpson", "Britney Spears"]
    authors_in_clusters = find_authors_in_clusters(authors, clusters)
    visualize(authors_in_clusters)

def print_all_clusters(clusters):
    print "Clusters: "
    print ""
    for cluster in clusters:
        for song in cluster:
            print str(song[0]) + ", " + str(song[1])
        print ""
        print ""

def create_cluster(cluster, songs, song, similar_artists):
    song_list = []
    if songs:
        for index in xrange(0, len(songs)):
            similarity = calculate_similarity(song, songs[index], similar_artists)
            if similarity > 40.0:
                song_list.append(songs[index])
            else:
                cluster.append([songs[index][0], songs[index][14]])
    return song_list, cluster

def create_list_from_records(songs_data):
    songs = []
    for song in songs_data:
        songs.append(song)
    return songs

def get_all_songs_data(con):
    query = "SELECT s.title, s.year, s.release, s.song_hotttnesss, s.danceability, s.duration, " \
            "s.loudness, s.end_of_fade_in, s.start_of_fade_out, s.key, s.energy, s.mode, s.tempo, " \
            "s.time_signature, a.artist_name, a.artist_familiarity, a.artist_hotttnesss, a.artist_latitude, " \
            "a.artist_location, a.artist_longitude, a.artist_id FROM Songs AS s inner join Artists AS a ON " \
            "s.artist_id = a.a_id"
    return con.execute(query)

def cluster_songs(con):
    songs_data = get_all_songs_data(con)
    songs_data = create_list_from_records(songs_data)
    clusters = []
    while songs_data:
        cluster = []
        song = songs_data[0]
        cluster.append([song[0], song[14]])
        del songs_data[0]
        similar_artists = get_similar_artists(con, song[20])
        songs_data, cluster = create_cluster(cluster, songs_data, song, similar_artists)
        clusters.append(cluster)
    print_all_clusters(clusters)
    return clusters

def similar(a, b):
    return SequenceMatcher(None, a, b).ratio()

def print_most_similar_songs(similar_songs, number_of_songs):
    similar_songs = sorted(similar_songs, key=lambda song: song[1])
    for i in xrange(0, number_of_songs):
        print str(similar_songs[i][0]) + ' : ' + str(similar_songs[i][1])

def calculate_similarity(typed_song, other_song, similar_artists):
    similarity_value = 0
    similarity_value += abs(typed_song[1] - other_song[1]) * 0.55
    if typed_song[3] is not None and other_song[3] is not None:
        similarity_value += abs(typed_song[3] - other_song[3]) * 0.08
    if typed_song[4] is not None and other_song[4] is not None:
        similarity_value += abs(typed_song[4] - other_song[4]) * 0.77
    similarity_value += abs(typed_song[5] - other_song[5]) * 0.04
    similarity_value += abs(typed_song[6] - other_song[6]) * 0.38
    similarity_value += abs(typed_song[7] - other_song[7]) * 0.01
    similarity_value += abs(typed_song[8] - other_song[8]) * 0.03
    similarity_value += abs(typed_song[9] - other_song[9]) * 0.5
    similarity_value += abs(typed_song[10] - other_song[10]) * 0.1
    similarity_value += abs(typed_song[11] - other_song[11]) * 0.2
    similarity_value += abs(typed_song[12] - other_song[12]) * 0.6
    similarity_value += abs(typed_song[13] - other_song[13]) * 0.3
    if typed_song[15] is not None and other_song[15] is not None:
        similarity_value += abs(typed_song[15] - other_song[15]) * 0.9
    if typed_song[16] is not None and other_song[16] is not None:
        similarity_value += abs(typed_song[16] - other_song[16]) * 0.96
    if typed_song[17] is not None and other_song[17] is not None:
        similarity_value += abs(typed_song[17] - other_song[17]) * 0.5
    if typed_song[19] is not None and other_song[19] is not None:
        similarity_value += abs(typed_song[19] - other_song[19]) * 0.3
    similarity_value += similar(typed_song[0], other_song[0]) * 0.02
    similarity_value += similar(typed_song[18], other_song[18]) * 0.01
    if typed_song[2] == other_song[2]:
        similarity_value *= 0.03
    if typed_song[14] == other_song[14]:
        similarity_value *= 0.04
    if other_song[20] in similar_artists:
        similarity_value *= 0.21
    return similarity_value

def create_similar_songs_list(typed_song, songs, similar_artists):
    similar_songs = []
    for song in songs:
        similarity_value = calculate_similarity(typed_song, song, similar_artists)
        similar_songs.append((song[0], similarity_value))
    return similar_songs

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
            "a.artist_location, a.artist_longitude, a.artist_id FROM Songs AS s inner join Artists AS a ON " \
            "s.artist_id = a.a_id WHERE s.title <> '" + str(song_name) + "';"
    return con.execute(query)

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

def print_similar_songs(con, song_name, number_of_songs):
    song_data = get_song_data(con, song_name)
    songs_data = get_all_other_songs_data(con, song_name)
    similar_artists = get_similar_artists(con, song_data[21])
    similar_songs = create_similar_songs_list(song_data, songs_data, similar_artists)
    print "Similar songs to " + str(song_name) + ":"
    print_most_similar_songs(similar_songs, number_of_songs)

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
    number_of_songs = 20
    print_similar_songs(con, song_name, number_of_songs)

    clusters = cluster_songs(con)
    visualize_authors(clusters)

main()