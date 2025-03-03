import numpy as np
import skfuzzy as fuzz
from src.mst_prim import prim_mst_coords, create_routes


def dist_func(row, u, max_c, centers, capacity_col):
    dist = np.linalg.norm(row[["x", "y"]] - centers[row["cluster"], :])
    dist += row["latency"]
    dist = (1 - u[row["cluster"], row.name]) * dist
    dist = dist * row[capacity_col] / max_c
    return dist


def leachSFClusteringJoin(df, capacity_col, num_clusters):
    df["oindex"] = df.index
    max_c = df[capacity_col].max()
    # perform clustering
    centers, u, u0, d, jm, p, fpc = fuzz.cluster.cmeans(df[["x", "y"]].values.T, c=num_clusters, m=2,
                                                        error=0.005, maxiter=1000, init=None)
    df["cluster"] = np.argmax(u, axis=0)
    df['distance'] = df.apply(lambda row: dist_func(row, u, max_c, centers, capacity_col), axis=1)

    df.loc[0, "cluster"] = -1
    df.loc[0, "distance"] = 0

    # identify cluster head for each cluster
    ch_indices = df[df["cluster"] >= 0].groupby("cluster")["distance"].idxmin().to_numpy()

    # Perform the groupby and find the idxmin, but collect the values in a list
    parent_list = df.groupby('cluster')['distance'].transform('idxmin').to_list()
    df['parent'] = parent_list
    df.loc[0, "parent"] = np.nan

    # create routes
    df["route"] = [[]] * df.shape[0]
    df["level"] = 2

    # route and parent for cluster heads
    for ch_idx in ch_indices:
        df.at[ch_idx, 'route'] = [0]
        df.loc[ch_idx, "parent"] = 0
        df.loc[ch_idx, "level"] = 1

    for src_idx in df[df["type"] == "SOURCE"].index:
        if src_idx not in ch_indices:
            parent = int(df.loc[src_idx, "parent"])
            df.at[src_idx, 'route'] = [parent] + df.loc[parent, "route"]
            df.loc[src_idx, "level"] = 0

    df = df[["oindex", "x", "y", "type", "cluster", capacity_col, "parent", "route", "level"]]
    return df, ch_indices, centers, u


def leachSFClusteringTreeJoin(df, capacity_col, num_clusters, coord_cols):
    df["oindex"] = df.index
    max_c = df[capacity_col].max()
    # perform clustering
    centers, u, u0, d, jm, p, fpc = fuzz.cluster.cmeans(df[["x", "y"]].values.T, c=num_clusters, m=2,
                                                        error=0.005, maxiter=1000, init=None)
    df["cluster"] = np.argmax(u, axis=0)
    df['distance'] = df.apply(lambda row: dist_func(row, u, max_c, centers, capacity_col), axis=1)

    df.loc[0, "cluster"] = -1
    df.loc[0, "distance"] = 0

    # identify cluster head for each cluster
    ch_indices = df[df["cluster"] >= 0].groupby("cluster")["distance"].idxmin().to_numpy()

    # Perform the groupby and find the idxmin, but collect the values in a list
    parent_list = df.groupby('cluster')['distance'].transform('idxmin').to_list()
    df['parent'] = parent_list
    df.loc[0, "parent"] = np.nan

    c_coords = df.loc[0, ["x", "y"]].to_numpy()
    ch_coords = df.loc[ch_indices, coord_cols].to_numpy()
    mst_coords = np.vstack((c_coords, ch_coords))
    mst = prim_mst_coords(mst_coords)
    routes = create_routes(mst)

    # resolve routes to origin ids
    lookup = np.insert(ch_indices, 0, 0)
    for k, route in routes.items():
        routes[k] = [lookup[idx] for idx in route]

    df["route"] = [[]] * df.shape[0]
    max_route = max(len(sublist) for sublist in routes.values()) + 1
    df["level"] = max_route

    # route and parent for cluster heads
    for idx, route in routes.items():
        ch_idx = ch_indices[idx - 1]
        df.at[ch_idx, 'route'] = route.copy()
        df.loc[ch_idx, "parent"] = int(route[0])
        df.loc[ch_idx, "level"] = max_route - len(route)

    # route and parent for sources
    for src_idx in df[df["type"] == "SOURCE"].index:
        if src_idx not in ch_indices:
            parent = int(df.loc[src_idx, "parent"])
            df.at[src_idx, 'route'] = [parent] + df.loc[parent, "route"]
            df.loc[src_idx, "level"] = max_route - len(df.at[src_idx, 'route'])

    df = df[["oindex", "x", "y", "type", "cluster", capacity_col, "parent", "route", "level"]]

    return df, ch_indices, centers, u
