import pandas as pd
import numpy as np
from scipy.spatial import cKDTree
from sklearn.datasets import make_blobs
import src.util as util
from scipy.stats import lognorm
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score
import random
import json

path_FIT = "datasets/FIT/coords/FIT0_calc_coords.csv"
path_FIT_labels = "datasets/FIT/coords/locations.csv"

path_RIPE_ATLAS_filtered_2d = "datasets/atlas/time-0_2d.csv"
path_RIPE_ATLAS_filtered_2d_nmds = "datasets/atlas/time-0_2d_nmds.csv"
path_RIPE_ATLAS_filtered_3d = "datasets/atlas/time-0_3d.csv"
path_RIPE_ATLAS_filtered_RTT = "datasets/atlas/rtt_time-0.csv"
path_RIPE_ATLAS_labels = "datasets/atlas/id2country_t0_cleaned.json"

path_KING = "datasets/vivaldi_king.txt"
path_PLANETLAB = "datasets/planetlab.txt"
path_PLANETLAB_labels = "datasets/PL_labels.csv"


def transition_uniform_to_exponential(size, steps=10, takes=2, value_range=(1, 10), rate_param=0.005,
                                      transition_slowdown=1.5, seed=None):
    if seed:
        np.random.seed(seed)

    # Step 1: Generate the initial uniform distribution
    uniform_dist = np.random.uniform(low=value_range[0], high=value_range[1], size=size)
    uniform_dist = np.clip(uniform_dist, a_min=1, a_max=None)  # Ensure all values are >= 1

    # Step 2: Generate the target exponential distribution, shifted to start at 1
    exponential_dist = np.random.exponential(scale=1 / rate_param, size=size) + 2
    exponential_dist = np.clip(exponential_dist, a_min=1, a_max=None)  # Ensure all values are >= 1

    # Normalize exponential to match the total sum of the uniform distribution
    exponential_dist = exponential_dist / np.sum(exponential_dist) * np.sum(uniform_dist)

    # Step 3: Create intermediate distributions with nonlinear interpolation
    distributions = []
    for step in range(steps + 1):
        # Use a nonlinear interpolation factor (slower progression in initial steps)
        alpha = (step / steps) ** (1 / transition_slowdown)
        interpolated_dist = (1 - alpha) * uniform_dist + alpha * exponential_dist

        # Normalize the interpolated distribution
        interpolated_dist = np.clip(interpolated_dist, a_min=1, a_max=None)
        interpolated_dist = interpolated_dist / np.sum(interpolated_dist) * np.sum(uniform_dist)

        distributions.append(interpolated_dist.astype(int))  # Convert to integers if needed

    return distributions[::takes]


def get_lognorm_hist():
    mode = 1
    stddev = 45
    sigma, scale = util.lognorm_params(mode, stddev)
    print("sigma:", sigma, "scale:", scale)
    sample = lognorm.rvs(sigma, 0, scale, size=100000).astype(int)
    H, bins = np.histogram(sample, bins=100, range=(0, 100), density=True)
    return H, bins


def get_lognorm_samples(min, max, target_sum, num_samples, mu=0.8, sigma=1.5):
    lognorm_samples = lognorm.rvs(s=sigma, scale=np.exp(mu), size=num_samples)
    # Clip the generated samples to the specified range
    lognorm_samples = np.clip(lognorm_samples, min, max)
    lognorm_samples = lognorm_samples * (target_sum / np.sum(lognorm_samples))

    return lognorm_samples


def generate_random_integer_array(n, total_sum, min_value=0, max_value=10):
    # Step 1: Generate n random integers within the given range
    random_values = np.random.randint(min_value, max_value + 1, n)

    # Step 2: Scale the values so their sum matches total_sum
    current_sum = np.sum(random_values)
    difference = total_sum - current_sum

    # Step 3: Adjust the values to make the sum equal to total_sum
    while difference != 0:
        # Choose a random index to adjust
        index = np.random.randint(0, n)

        # If the difference is positive, try to increase the value
        if difference > 0 and random_values[index] < max_value:
            random_values[index] += 1
            difference -= 1
        # If the difference is negative, try to decrease the value
        elif difference < 0 and random_values[index] > min_value:
            random_values[index] -= 1
            difference += 1

    return random_values


def add_capacity_columns(df, H, max_capacity, c_capacity, size):
    slot_columns = []
    for i in range(len(H), 0, -1):
        if (i % 10 == 0) or (i == 5) or (i == 1):
            # probabilites
            p = np.array(H[i - 1:len(H)])
            p /= p.sum()  # normalize
            pop = np.arange(i - 1, len(H))

            slot_list = np.random.choice(pop, size - 1, p=p, replace=True)
            slot_list = np.insert(slot_list, 0, 0)

            col = "capacity_" + str(i)
            df[col] = pd.Series(slot_list, dtype="int")
            df["capacity_" + str(i)] = df[col] / df[col].sum() * max_capacity

            df[col] = np.ceil(df[col]).astype("int")
            df.at[0, col] = c_capacity
            slot_columns.append(col)
    return df, slot_columns


def coords_ripe_atlas(path=path_RIPE_ATLAS_filtered_2d, labels=path_RIPE_ATLAS_labels):
    df_atlas = pd.read_csv(path, sep=",", header=None, names=["x", "y"])
    if labels:
        with open(labels, "r") as json_file:
            countries = json.load(json_file)
        countries = list(countries.values())
        df_atlas["label"] = countries
        df_atlas = df_atlas.dropna(subset=["label"])
        df_atlas = df_atlas.reset_index(drop=True)
    return df_atlas


def rtt_ripe_atlas(path=path_RIPE_ATLAS_filtered_RTT):
    return pd.read_csv(path, sep=",", header=None, skiprows=1, dtype=int)


def coords_fit(path=path_FIT, labels=path_FIT_labels):
    df = pd.read_csv(path, header=None, names=["x", "y"])
    if labels:
        dfl = pd.read_csv(labels, header=None)
        df["label"] = dfl
    return df


def coords_KING(path=path_KING):
    df_king = pd.read_csv(path, sep=" ", header=None, names=["name", "x", "y"])
    return df_king[["x", "y"]]


def coords_PLANETLAB(path=path_PLANETLAB, labels=path_PLANETLAB_labels):
    df_plb = pd.read_csv(path, sep=" ", header=None, names=["name", "x", "y"])
    if labels:
        dfl = pd.read_csv(labels, header=None)
        df_plb["label"] = dfl
    return df_plb[["x", "y", "label"]]


def coords_sim(size, centers=40, x_dim_range=(0, 100), y_dim_range=(-50, 50), with_latency=False,
               seed=4, c_coords=None):
    np.random.seed(seed)
    device_number = size + 1  # first node is the SINK

    # blobs with varied variances
    stds = np.random.uniform(low=0.5, high=7.3, size=(centers,))
    coords, y = make_blobs(n_samples=device_number, centers=centers, n_features=2, shuffle=True,
                           cluster_std=stds,
                           center_box=((x_dim_range[0], y_dim_range[0]), (x_dim_range[1], y_dim_range[1])),
                           random_state=31)

    df = pd.DataFrame(coords, columns=["x", "y"])
    if with_latency:
        if c_coords is None:
            c_coords = df.iloc[0].to_numpy()
        else:
            df.loc[0, ["x", "y"]] = c_coords
        df['latency'] = list(zip(df.x, df.y))
        df['latency'] = df['latency'].apply(lambda x: np.linalg.norm(x - c_coords))

    return df


def get_coords_dict():
    out = {
        "planetlab": coords_PLANETLAB(),
        "king": coords_KING(),
        "fit": coords_fit(),
        "atlas": coords_ripe_atlas()
    }
    return out


def add_knn_labels(df, no_cluster, coord_cols):
    ch_indices = random.sample(range(1, df.shape[0]), no_cluster)
    ch_coords = np.array(df.iloc[ch_indices][coord_cols].apply(tuple, axis=1))

    # Build the k-d tree for the centroids
    kdtree = cKDTree(np.vstack(ch_coords))
    # Query the k-d tree to find the closest centroid for each node
    closest_centroids_indices = kdtree.query(df[coord_cols].to_numpy(), k=1)[1]
    df["cluster"] = closest_centroids_indices
    df.loc[0, "cluster"] = -1
    return df


def add_kmeans_labels(df, coord_cols, opt_k=None, kmin=2, kmax=30, kseed=20):
    coords = df[coord_cols]
    sil = []

    if not opt_k:
        # dissimilarity would not be defined for a single cluster, thus, minimum number of clusters should be 2
        for k in range(kmin, kmax + 1):
            # if k % 5 == 0:
            #    print(k)
            kmeans = KMeans(n_clusters=k, n_init='auto', random_state=kseed).fit(coords)
            labels = kmeans.labels_
            sil.append(silhouette_score(coords, labels, metric='euclidean'))

        opt_k = np.argmax(sil)
        opt_k = kmin + opt_k
        print("Optimal k is", opt_k)

    cluster_alg = KMeans(n_clusters=opt_k, n_init='auto', random_state=kseed).fit(coords)
    labels = cluster_alg.labels_
    centroids = cluster_alg.cluster_centers_

    df["cluster"] = labels
    df.loc[0, "cluster"] = -1
    return df, centroids, opt_k, sil


def setup_topology(df, coord_cols, sink_capacity=100, worker_capacity=(100, 100),
                   source_capacity=(100, 100), source_frac=1, seed=None):
    df = df.copy()
    coords = df[coord_cols].to_numpy()
    c_coords = coords[0]

    df['latency'] = list(zip(*[df[col] for col in coord_cols]))
    df['latency'] = df['latency'].apply(lambda x: np.linalg.norm(x - c_coords))

    no_sources = min(int(source_frac * (len(coords))), len(coords) - 1)
    if no_sources % 2 != 0:
        no_sources -= 1
    source_range = list(range(0, no_sources))
    workers_range = list(range(len(source_range), len(coords) - 1))
    type_list = ["SOURCE" for x in source_range] + ["WORKER" for x in workers_range]
    type_list.insert(0, "SINK")
    df["type"] = pd.Series(type_list, dtype="category")

    base_col = "capacity_inf"
    df[base_col] = 9999999

    source_capacities = transition_uniform_to_exponential(len(source_range), value_range=source_capacity, seed=seed)
    sink_capacities = np.array([[sink_capacity] for _ in range(len(source_capacities))], dtype=int)
    worker_capacities = transition_uniform_to_exponential(len(workers_range), value_range=worker_capacity, seed=seed)

    capacity_columns = []
    for i in range(len(source_capacities)):
        col_name = "capacity_" + str(i)
        df[col_name] = 0
        df.loc[df["type"] == "SINK", col_name] = sink_capacities[i]
        df.loc[df["type"] == "SOURCE", col_name] = source_capacities[i]
        df.loc[df["type"] == "WORKER", col_name] = worker_capacities[i]
        capacity_columns.append(col_name)

    return df, base_col, capacity_columns


def create_topologies_from_dict(topology_dict, coord_cols, sink_capacity=100, worker_capacity=(100, 100),
                                source_capacity=(100, 100), source_frac=1, with_clustering=True, kmin=2, kmax=30,
                                seed=20):
    out = {}
    for k, df in topology_dict.items():
        print("Creating df for", k)
        df["cluster"] = 0
        df, base_col, capacity_columns = setup_topology(df, coord_cols, sink_capacity=sink_capacity,
                                                        worker_capacity=worker_capacity,
                                                        source_capacity=source_capacity, source_frac=source_frac,
                                                        seed=seed)

        out[k] = df, base_col, capacity_columns
        if with_clustering:
            df, centroids, opt_k, sil = add_kmeans_labels(df, coord_cols, kmin=kmin, kmax=kmax, kseed=seed)
            out[k] = df, base_col, capacity_columns, centroids, opt_k, sil

    print("Done")
    return out
