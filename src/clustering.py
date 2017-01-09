# -*- coding: utf-8 -*-

import sqlite3 as sql
from scipy import cluster
from scipy.spatial.distance import euclidean
import numpy as np
import pickle

db_file = "../resources/songs.db"


def get_all_songs_data(con):
    query = "SELECT * FROM Songs AS s inner join Artists AS a ON " \
            "s.artist_id = a.a_id"
    return con.execute(query)


def deserialize(data):
    arr = pickle.loads(data)
    if arr is not None:
        return arr.mean()
    return 0


def create_vector(song):
    vec = []
    song_data = {}
    # title
    #
    # key
    # mode
    # time sig
    # tempo
    # pitches
    # timbre
    # hotness
    song_data["title"] = song[36]
    song_data["artist"] = song[51]

    vec.append(song[12])
    vec.append(song[15])
    vec.append(deserialize(song[25]))
    vec.append(deserialize(song[27]))
    vec.append(song[28])
    vec.append(song[33])
    vec.append(song[34])

    for i in range(len(vec)):
        if vec[i] is None:
            vec[i] = 0.

    song_data["data"] = vec
    return song_data


def get_raw_data(songs):
    return np.array([song['data'] for song in songs])


def normalize(data):
    max = np.amax(data, axis=0)
    min = np.amin(data, axis=0)
    for i in range(7):
        data[:, i] -= min[i]
        data[:, i] /= (max-min)[i]
    return data


def kmeans(data, clusters_no):
    clustered = cluster.vq.kmeans(data, clusters_no, thresh=0.01)
    return clustered


def get_distances(data, centroids):
    dists = []
    for song in data:
        dist = []
        for centr in centroids:
            dst = euclidean(song, centr)
            dist.append(dst)
        dists.append(dist)
    return dists


def classify(dists, titles):
    clusters = {}
    song_nr = 0
    max_intra_cluster_dis = 0
    for dist in dists:
        minimum = min(dist)
        if minimum > max_intra_cluster_dis:
            max_intra_cluster_dis = minimum
        cluster_nr = dist.index(minimum)
        song = titles[song_nr]
        if cluster_nr in clusters.keys():
            clusters[cluster_nr].append(song)
        else:
            clusters[cluster_nr] = [song]
        song_nr += 1
    return clusters, max_intra_cluster_dis


def get_titles(data):
    return [(song["title"], song["artist"]) for song in data]


def min_inter_cluster_dist(centroids):
    cen_no = len(centroids)
    result = 0
    for i in range(cen_no):
        for j in range(i):
            dst = euclidean(centroids[i], centroids[j])
            if dst < result or result == 0:
                result = dst
    return result


def dunn_index(centroids, max_intra_cluster):
    min_inter_cluster = min_inter_cluster_dist(centroids)
    return min_inter_cluster / max_intra_cluster


def get_data_from_title(title, titles, norm):
    ind = titles.index(title)
    return norm[ind]


def main():
    con = sql.connect(db_file)
    all_songs = get_all_songs_data(con)
    all_songs_list = all_songs.fetchall()
    songs = []

    it = 0
    for song in all_songs_list:
        songs.append(create_vector(song))
        it += 1

    raw_data = get_raw_data(songs)
    norm = normalize(raw_data)
    result_str = ""
    for clusters_no in range(10, 1000, 100):

        kmns = kmeans(norm, clusters_no)
        dists = get_distances(raw_data, kmns[0])

        titles = get_titles(songs)

        classification = classify(dists, titles)
        clusters = classification[0]
        max_intra_cluster_dis = classification[1]

        clusters_non_empty = [cl for cl in clusters.values() if len(cl) > 0]

        result_str += "Clusters: "+str(len(clusters_non_empty))+"\n"
        for cl in clusters_non_empty:
            result_str += str(len(cl)) + "\n"
            for s in cl:
                result_str += str(s[0].encode('utf-8')) + " - " + str(s[1].encode('utf-8')) + "\n"
            result_str += "\n\n\n"

        dunn_ind = dunn_index(kmns[0], max_intra_cluster_dis)
        print dunn_ind
        result_str += str(dunn_ind) + "\n"
    result_fl = open("results.txt", 'w')
    result_fl.write(result_str)
    result_fl.close()

main()
