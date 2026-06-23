import numpy as np
import seaborn as sns
import pandas as pd
from matplotlib.lines import Line2D
from matplotlib.transforms import Bbox
import math, heapq
from scipy.spatial import cKDTree, voronoi_plot_2d, Voronoi
from scipy.optimize import minimize
from sklearn.metrics import confusion_matrix
import networkx as nx
from scipy.stats import gaussian_kde

lcl = "black"
cmarker = "D"
ccolor = lcl
lnode_marker = "^"
ch_marker = "x"
w_marker = "o"
src_marker = "s"

coordinator_label = Line2D([], [], color=lcl, marker=cmarker, linestyle='None', label='sink')
worker_label = Line2D([], [], color="grey", marker=w_marker, linestyle='None', label='workers', markersize=4)
source_label = Line2D([], [], color="grey", marker=src_marker, linestyle='None', label='sources', markersize=4)
reassigned_label = Line2D([0, 1], [0, 1], linestyle='--', color=lcl, label='reassigned')
centroid_label = Line2D([], [], color="grey", marker='o', linestyle='None', label='centroid')
ch_label = Line2D([], [], color=lcl, marker=ch_marker, linestyle='None', label='physical node')
log_opt_label = Line2D([], [], color=lcl, marker=lnode_marker, linestyle='None', label='optimum')
join_label = Line2D([], [], color='grey', label='join', linestyle='', lw=2, marker=r'$\longleftrightarrow$',
                    markersize=15)


def get_color_list(num_colors):
    colors = sns.color_palette(n_colors=num_colors)
    colors_hex = np.asarray(colors.as_hex())
    light_colors = [lighten_color(x) for x in colors_hex]
    return colors, colors_hex, light_colors


def lighten_color(color, amount=0.5):
    """
    Lightens the given color by multiplying (1-luminosity) by the given amount.
    Input can be matplotlib color string, hex string, or RGB tuple.

    Examples:
    >> lighten_color('g', 0.3)
    >> lighten_color('#F034A3', 0.6)
    >> lighten_color((.3,.55,.1), 0.5)
    """
    import matplotlib.colors as mc
    import colorsys
    try:
        c = mc.cnames[color]
    except:
        c = color
    c = colorsys.rgb_to_hls(*mc.to_rgb(c))
    return colorsys.hls_to_rgb(c[0], 1 - amount * (1 - c[1]), c[2])


def sigmoid(value, k=10):
    return 1 / (1 + math.exp(-k * (value - 0.5)))


def break_into_threshold_continue(number, thresholds):
    # Result list to hold the parts
    parts = []

    # Index to track the current position in the thresholds list
    threshold_index = 0

    while number > 0:
        # Get the current threshold, and if we've exhausted the list,
        # keep using the last threshold.
        if threshold_index < len(thresholds):
            current_threshold = thresholds[threshold_index]
        else:
            current_threshold = thresholds[-1]

        # Break the number into pieces according to the current threshold
        if number > current_threshold:
            parts.append(current_threshold)
            number -= current_threshold
        else:
            parts.append(number)
            number = 0

        # Move to the next threshold
        threshold_index += 1

    return parts


def break_into_threshold_with_rest(number, thresholds):
    # Result list to hold the parts
    parts = []

    # Index to track the current position in the thresholds list
    threshold_index = 0

    while number > 0:
        # Check if we still have a threshold to use
        if threshold_index < len(thresholds):
            current_threshold = thresholds[threshold_index]
        else:
            # If we run out of thresholds, append the rest and break
            parts.append(number)
            break

        # Break the number into pieces according to the current threshold
        if number > current_threshold:
            parts.append(current_threshold)
            number -= current_threshold
        else:
            parts.append(number)
            number = 0

        # Move to the next threshold
        threshold_index += 1

    return parts


def product_decomposition_with_mapping(a_size, b_size, thresholds, rest=True):
    # print(f"Breaking tuples: A={a_size}, B={b_size}, thresholds={thresholds}")
    # Break each table size into smaller parts based on the threshold
    if rest:
        parts_a = break_into_threshold_with_rest(a_size, thresholds)
        parts_b = break_into_threshold_with_rest(b_size, thresholds)
    else:
        parts_a = break_into_threshold_continue(a_size, thresholds)
        parts_b = break_into_threshold_continue(b_size, thresholds)

    subproducts = []
    total = 0
    a_start, b_start = 0, 0  # Track the starting index for ranges
    required = 0
    max_req_per_partition = 0
    partitions = len(parts_a) + len(parts_b)

    for part_a in parts_a:
        b_start = 0  # Reset b_start for each part_a
        for part_b in parts_b:
            part_a = int(part_a)
            part_b = int(part_b)
            # Compute the subproduct
            subproduct_value = part_a * part_b
            partitions_sum = part_a + part_b
            # Map the subproduct to tuple ranges from A and B
            range_a = (a_start, a_start + part_a - 1)
            range_b = (b_start, b_start + part_b - 1)
            subproducts.append({
                "subtuples": (part_a, part_b),
                "subproduct": subproduct_value,
                "A_range": range_a,
                "B_range": range_b
            })
            total += subproduct_value
            required += partitions_sum
            max_req_per_partition = max(max_req_per_partition, partitions_sum)
            # Move to the next range in B
            b_start += part_b
        # Move to the next range in A
        a_start += part_a

    # Return the mapped subproducts
    return subproducts, total, partitions, required, max_req_per_partition


def distribute_sums(sum1, sum2):
    # Expand the distributive property (a+b+...) * (c+d+...)
    expanded_terms = []
    for term1 in sum1:
        for term2 in sum2:
            expanded_terms.append((term1, term2))
    return expanded_terms


def get_partition_load(p_left, p_right):
    total_loads = {}
    dist_sums = distribute_sums(p_left, p_right)
    total_sum = sum(x + y for x, y in dist_sums)
    total_loads['partitions'] = len(p_left) + len(p_right)
    total_loads['load'] = total_sum
    total_loads['max'] = sum(max(dist_sums))
    return total_loads


def get_max_by_thresh(elements, threshold):
    max_k = np.argmax(elements)
    min_t = max(0, elements[max_k] - threshold)
    for i in range(len(elements) - 1, -1, -1):
        if elements[i] >= min_t:
            return i
        i += i
    return max_k


def find_first_overlap(list1, list2):
    # Convert list2 to a set for faster lookup
    set_list2 = set(list2)

    # Iterate through list1 and find the first element in set_list2
    for item in list1:
        if item in set_list2:
            return item

    # If no overlap is found, return None
    return None


def calculate_purity(labels_true, labels_pred):
    # Create a confusion matrix
    cm = confusion_matrix(labels_true, labels_pred)

    # Calculate purity
    purity = 0
    for i, cluster in enumerate(cm):
        purity += cluster[i]

    print(f"Wrong labeled {len(labels_true) - purity}/{len(labels_true)}")
    purity /= len(labels_true)

    return cm, purity


def euclidean_distance(point1, point2, decimals=0):
    """
    Calculate the Euclidean distance between two points represented as tuples.
    """
    if decimals <= 0:
        return int(np.linalg.norm(np.array(point1) - np.array(point2)))
    else:
        return round(np.linalg.norm(np.array(point1) - np.array(point2)), decimals)


def update_coordinates(origin_df, new_coords_df):
    # Update DataFrame based on values in another DataFrame
    df = origin_df.merge(new_coords_df, left_on='oindex', right_index=True, suffixes=('_orig', '_update'), how='left')

    # Choose values from the updated columns, fill NaN with the original values
    df['x'] = df['x_update'].fillna(df['x_orig'])
    df['y'] = df['y_update'].fillna(df['y_orig'])

    # Drop the intermediate columns
    df = df.drop(columns=['x_orig', 'y_orig', 'x_update', 'y_update'])
    return df


def get_coords(existing_coords, target_distances):
    def objective_function(coords, *args):
        existing_coords, target_distances = args
        errors = []
        for i in range(len(existing_coords)):
            current_distance = euclidean_distance(coords, existing_coords[i])
            errors.append((current_distance - target_distances[i]) ** 1)
        return sum(errors)

    # Initial guess for the coordinates of the 6th node
    initial_guess = np.zeros(2)

    # Use optimization to find the coordinates of the node
    result = minimize(objective_function, initial_guess, args=(existing_coords, target_distances), method='Powell')
    # coordinates, error
    return result.x, result.fun


def fit_coords(node_id, coord_df, rtt_df, k_neigh):
    n_coords = rtt_df.loc[node_id, ["x", "y"]]
    df = rtt_df.drop(node_id)
    df = df.sample(k_neigh)[["x", "y"]]
    df['latency'] = list(zip(df.x, df.y))
    df['latency'] = df['latency'].apply(lambda x: np.linalg.norm(x - n_coords))

    existing_coords = coord_df.loc[df.index][["x", "y"]].to_numpy()
    target_distances = df["latency"].to_numpy()
    new_coords, error = get_coords(existing_coords, target_distances)
    return new_coords, error


def get_nested_parents(node_ids, df, parent_col="parent"):
    parents = df.loc[node_ids, parent_col].to_list()
    parents = [tup[0] for sublist in parents if sublist for tup in sublist]
    parents = list(set(parents))
    return parents


def create_groups(coordinates, n):
    """
    Create groups of n closest points such that no index is alone in a group.

    Args:
        coordinates (list of tuples): List of tuples representing coordinates.
        n (int): Number of closest points per group.

    Example:
        coordinates = [(1, 2), (3, 4), (5, 6), (7, 8), (9, 10)], n = 3
        Result: [[0, 1, 2], [3, 4]]

    Returns:
        list of lists: List of groups where each group is a list of indices.
    """
    num_points = len(coordinates)
    groups = []
    visited = set()

    for i in range(num_points):
        if i not in visited:
            group = [i]
            visited.add(i)

            # Create a min-heap for storing (distance, index) pairs
            min_heap = [(euclidean_distance(coordinates[i], coordinates[j]), j) for j in range(num_points) if j != i]

            while len(group) < n:
                if not min_heap:
                    break

                # Get the closest point from the heap
                distance, closest_point = heapq.heappop(min_heap)

                # If the closest point is unvisited, add it to the group
                if closest_point not in visited:
                    group.append(closest_point)
                    visited.add(closest_point)

            groups.append(group)
    return groups


def calc_opt(point1, point2, w=0.5, k=0.1, num_iterations=100):
    # Convert input points to numpy arrays
    point1 = np.array(point1, dtype=np.float64)
    point2 = np.array(point2, dtype=np.float64)
    w2 = 1 - w

    for _ in range(num_iterations):
        # Calculate the displacement vector from point1 to point2
        displacement = point2 - point1

        # Calculate the force vector using Hooke's Law
        force_vector = k * displacement

        # Update the positions of point1 and point2 based on the force
        point1 += np.round(w * force_vector, 2)
        point2 -= np.round(w2 * force_vector, 2)

    # The optimal point location is the average of point1 and point2
    optimal_location = (point1 + point2) / 2.0
    return optimal_location


def evaluate_node_rec(node_id, df_placement, coords, df_rtt=None):
    node_df = df_placement[df_placement["oindex"] == node_id][["x", "y", "parent"]]
    latencies = []

    for idx, row in node_df.iterrows():
        parent_idx = row["parent"]

        if pd.isna(parent_idx):
            return [0]
        else:
            parent_lats = evaluate_node_rec(parent_idx, df_placement, coords)

            # latency is distance to parent + latency of parent
            if df_rtt is not None:
                latency = df_rtt.loc[idx, parent_idx]
            else:
                self_coords = row[["x", "y"]].to_numpy()
                parent_coords = coords.loc[parent_idx, ["x", "y"]].to_numpy()
                latency = np.linalg.norm(self_coords - parent_coords)

            latency += np.mean(parent_lats)
            latencies.append(latency)
    return latencies


def evaluate_routes(df_orig, join_matrix, join_pairs, source_cardinalities, routes, df_rtt=None):
    latency_hist = np.zeros(df_orig.shape[0])
    latency_hist_rtt = np.zeros(df_orig.shape[0])
    received_packets_hist = np.zeros(df_orig.shape[0])

    sources = df_orig[df_orig["type"] == "SOURCE"].index
    coords = df_orig[["x", "y"]].to_numpy()

    for i, j in join_pairs:
        if join_matrix[i, j] > 0:
            output_cardinality = join_matrix[i, j]
            join_node = find_first_overlap(routes[i], routes[j])
            for source in [i, j]:
                lat_route = routes[source]
                source_cardinality = source_cardinalities[source]
                overlapped = False
                for end in lat_route:
                    if overlapped:
                        received_packets_hist[end] += output_cardinality
                    else:
                        received_packets_hist[end] += source_cardinality
                    if end == join_node:
                        overlapped = True

    # calculate latencies
    for i in sources:
        dist = 0
        dist_rtt = 0
        lat_route = routes[i]
        start = i
        for end in lat_route:
            dist += np.linalg.norm(coords[start] - coords[end])
            if df_rtt is not None:
                dist_rtt += df_rtt.loc[start, end]
            start = end
        latency_hist[i] = dist
        if df_rtt is not None:
            latency_hist_rtt[i] = dist_rtt

    if df_rtt is not None:
        statistics = {"latency_distribution": latency_hist,
                      "latency_distribution_rtt": latency_hist_rtt,
                      "received_packets": received_packets_hist}
    else:
        statistics = {"latency_distribution": latency_hist,
                      "received_packets": received_packets_hist}
    df_stats = pd.DataFrame(statistics)
    return df_stats


def evaluate(df_placement, df_rtt=None):
    latency_dict = {}

    coords = df_placement.groupby('oindex')[["x", "y"]].first()
    df_sorted = df_placement.sort_values(by='level', ascending=False)

    calculated_parents = set()

    for level, level_df in df_sorted.groupby('level', sort=False):
        for idx, row in level_df.iterrows():
            self_idx = row["oindex"]
            parent_idx = row["parent"]

            if pd.isna(parent_idx):
                latency_dict[self_idx] = [0]
                continue
            if self_idx in calculated_parents:
                continue

            self_coords = row[["x", "y"]].to_numpy()
            parent_coords = coords.loc[parent_idx, ["x", "y"]].to_numpy()

            # latency is distance to parent + latency of parent
            if df_rtt is not None:
                latency = df_rtt.loc[self_idx, parent_idx]
            else:
                latency = np.linalg.norm(self_coords - parent_coords)

            if parent_idx not in latency_dict:
                parent_lats = evaluate_node_rec(parent_idx, df_placement, coords, df_rtt)
                latency_dict[parent_idx] = parent_lats
                calculated_parents.add(parent_idx)

            latency += np.mean(latency_dict[parent_idx])

            if self_idx in latency_dict:
                latency_dict[self_idx] += [latency]
            else:
                latency_dict[self_idx] = [latency]

    df_placement["load"] = df_placement["total_capacity"] - df_placement["free_capacity"]
    load_dict = df_placement.groupby("oindex")["load"].mean().to_dict()
    latency_dict = {key: np.mean(values) for key, values in latency_dict.items()}

    statistics = {"latency_distribution": latency_dict,
                  "received_packets": load_dict}
    df_stats = pd.DataFrame(statistics)
    return df_stats


def evaluate_graph(df_nemo, rep_plan, df_rtt=None, placements=None):
    df_nemo = df_nemo.copy()
    latency_dict = dict()
    latency_dict_rtt = dict()
    sources = df_nemo[df_nemo["type"] == "SOURCE"].index
    sinks = df_nemo[df_nemo["type"] == "SINK"].index

    for start_node in sources:
        distances = []
        distances_rtt = []
        for end_node in sinks:
            # Calculate distances of all paths from start_node to end_node
            all_paths = list(nx.all_simple_paths(rep_plan, source=start_node, target=end_node))

            # Calculate distances for each path
            for path in all_paths:
                # Sum weights of edges in the path
                distance = sum(rep_plan[u][v]["distance"] for u, v in zip(path[:-1], path[1:]))
                distances.append(distance)
                if df_rtt is not None:
                    # Sum weights of edges in the path
                    distance_rtt = sum(df_rtt.loc[placements[u], placements[v]] for u, v in zip(path[:-1], path[1:]))
                    distances_rtt.append(distance_rtt)
        if len(distances) == 0:
            print("Distance empty", start_node)
        else:
            latency_dict[start_node] = np.mean(distances)
            if df_rtt is not None:
                latency_dict_rtt[start_node] = np.mean(distances_rtt)

    df_nemo["load"] = df_nemo["total_capacity"] - df_nemo["free_capacity"]
    load_dict = df_nemo["load"].to_dict()

    # Combine keys
    all_keys = set(latency_dict.keys()).union(set(load_dict.keys()))

    # Create DataFrames from dictionaries, filling missing keys with 0
    df1 = pd.DataFrame.from_dict(latency_dict, orient='index', columns=['latency_distribution']).reindex(all_keys,
                                                                                                         fill_value=0)
    df2 = pd.DataFrame.from_dict(load_dict, orient='index', columns=['received_packets']).reindex(all_keys,
                                                                                                  fill_value=0)

    # Concatenate the DataFrames
    df_stats = pd.concat([df1, df2], axis=1).fillna(0).astype(int)

    if df_rtt is not None:
        df3 = pd.DataFrame.from_dict(latency_dict_rtt, orient='index', columns=['latency_distribution_rtt']).reindex(
            all_keys, fill_value=0)
        df_stats = pd.concat([df_stats, df3], axis=1).fillna(0).astype(int)
        df_stats = df_stats[['latency_distribution', 'latency_distribution_rtt', 'received_packets']]

    return df_stats


def replace(list, old_elem, new_elem):
    for pos, val in enumerate(list):
        if val == old_elem:
            list[pos] = new_elem


def lognorm_params(mode, stddev):
    """
    Given the mode and std. dev. of the log-normal distribution, this function
    returns the shape and scale parameters for scipy's parameterization of the
    distribution.
    """
    p = np.poly1d([1, -1, 0, 0, -(stddev / mode) ** 2])
    r = p.roots
    sol = r[(r.imag == 0) & (r.real > 0)].real
    shape = np.sqrt(np.log(sol))
    scale = mode * sol
    return shape, scale


def get_diff(arr1, arr2):
    output = list(set(arr1).symmetric_difference(set(arr2)))
    intersection = list(set(arr1).intersection(arr2))

    for ele in intersection:
        if not np.array_equiv(arr1[ele], arr2[ele]):
            output.append(ele)
    return output


def plot_with_single_color(*args, **kwargs):
    args = list(args)
    length = args[1]["cluster"].nunique()
    color = args[3]
    colors = np.full(length, color)
    args[3] = colors
    args = tuple(args)
    plot(*args, **kwargs)


def plot(ax, df_origin, df_plcmnt, colors, lval=0.2, symbol_size=100, scale_fac=0.25, leg_size=12, axis_label_size=20,
         line_style="-", line_width=0.8, highlight_color=None, plot_centroids=False, plot_lines=False,
         plot_voronoi=False, opt_dict=None):
    handles = [coordinator_label, worker_label, ch_label]
    clusters = df_plcmnt["cluster"].unique()
    levels = df_plcmnt.loc[0, "level"]
    df_plcmnt = df_plcmnt.dropna()

    for cluster in clusters:
        # plot points
        df_cluster = df_plcmnt[df_plcmnt["cluster"] == cluster]
        ax.scatter(df_cluster["x"], df_cluster["y"], s=scale_fac * symbol_size,
                   color=lighten_color(colors[cluster], lval), zorder=-1)

        parents = df_cluster["parent"].unique()
        if plot_centroids:
            point1 = df_cluster[["x", "y"]].mean()
            ax.scatter(point1["x"], point1["y"], s=symbol_size, color=colors[cluster], zorder=4, label="centroid")

            for parent in parents:
                if parent != 0:
                    point2 = df_origin.loc[parent, ["x", "y"]]
                    level = df_plcmnt[df_plcmnt["oindex"] == parent]["level"].to_numpy()[0]

                    child = df_plcmnt[df_plcmnt["parent"] == parent].iloc[0]["oindex"]
                    is_leaf = df_plcmnt[df_plcmnt["parent"] == child].empty

                    x_values = [point1["x"], point2["x"]]
                    y_values = [point1["y"], point2["y"]]
                    if plot_lines and (level == 1 or is_leaf):
                        ax.plot(x_values, y_values, "-", linewidth=line_width, zorder=3, color=colors[cluster])

        for parent in parents:
            # print("plotting parent={0} for cluster={1}".format(parent, cluster))
            # point 1 -> parent
            point1 = df_origin.loc[parent, ["x", "y"]]
            level = df_plcmnt[df_plcmnt["oindex"] == parent]["level"].max()
            if highlight_color is not None and level == levels - 1:
                color = highlight_color
                zorder = 5
            else:
                color = colors[cluster]
                zorder = 4
            ax.scatter(point1["x"], point1["y"], s=symbol_size, color=color, zorder=zorder, marker=ch_marker,
                       label="agg. point")

            # point 2 -> parent of parent
            parent_parents = df_plcmnt[df_plcmnt["oindex"] == parent][["parent"]]["parent"].unique()
            point2 = df_origin.loc[parent_parents, ["x", "y"]]
            if highlight_color is not None and level == levels - 2:
                color = highlight_color
                zorder = 4
            else:
                color = colors[cluster]
                zorder = 5

            ax.scatter(point2["x"], point2["y"], s=symbol_size, color=color, zorder=zorder, marker=ch_marker,
                       label="agg. point")

            for pp in parent_parents:
                # plot connections
                point2 = df_origin.loc[pp, ["x", "y"]]
                x_values = [point1["x"], point2["x"]]
                y_values = [point1["y"], point2["y"]]
                if plot_lines:
                    ax.plot(x_values, y_values, line_style, linewidth=line_width, zorder=3, color=colors[cluster])

    if opt_dict is not None and len(opt_dict) > 0:
        print("opt dict", opt_dict)
        cl_dict = opt_dict[len(opt_dict)]
        for cl_label, opt_coords in cl_dict.items():
            if highlight_color is not None:
                color = highlight_color
            else:
                color = colors[cl_label]

            ax.scatter(opt_coords[0], opt_coords[1], s=symbol_size, color=color, zorder=6, marker=lnode_marker,
                       label="agg. point")

    if plot_voronoi:
        centroids = df_plcmnt[df_plcmnt["cluster"] >= 0].groupby("cluster")[["x", "y"]].mean().to_numpy()
        vor = Voronoi(centroids)
        voronoi_plot_2d(vor, ax=ax, point_size=symbol_size, color="red", show_vertices=False, show_points=False)

    ax.scatter(df_origin.loc[0, "x"], df_origin.loc[0, "y"], s=2 * symbol_size, color=ccolor, marker=cmarker, zorder=20)

    ax.set_xlabel('$network$ $coordinate_1$', fontsize=axis_label_size)
    ax.set_ylabel('$network$ $coordinate_2$', fontsize=axis_label_size)

    if plot_centroids:
        handles.append(centroid_label)

    if opt_dict is not None:
        handles.append(log_opt_label)

    ax.legend(handles=handles, loc="upper left",
              bbox_to_anchor=(0, 1), prop={'size': leg_size})
    return ax


def plot_optimum(ax, df_origin, opt_dicts, colors, lval=0.2, symbol_size=100, scale_fac=0.25, leg_size=12,
                 axis_label_size=20, plot_centroid=False, plot_lines=False):
    ccords = df_origin.loc[0, ["x", "y"]].tolist()
    clusters = df_origin["cluster"][df_origin["cluster"] >= 0].unique()
    for cluster in clusters:
        df_cluster = df_origin[df_origin["cluster"] == cluster]
        ax.scatter(df_cluster["x"], df_cluster["y"], s=scale_fac * symbol_size,
                   color=lighten_color(colors[cluster], lval),
                   zorder=-1)

        point2 = opt_dicts[1][cluster]
        ax.scatter(point2[0], point2[1], s=symbol_size, color=colors[cluster], zorder=4, marker=lnode_marker,
                   label="agg. point")

        if plot_centroid:
            point1 = df_cluster[["x", "y"]].mean().tolist()
            ax.scatter(point1[0], point1[1], s=symbol_size, color=colors[cluster], zorder=10, label="centroid")
            if plot_lines:
                ax.plot([point1[0], point2[0]], [point1[1], point2[1]], "--", zorder=3, color=colors[cluster])
                ax.plot([point2[0], ccords[0]], [point2[1], ccords[1]], "--", zorder=3, color=colors[cluster])

    ax.scatter(df_origin.loc[0, "x"], df_origin.loc[0, "y"], s=2 * symbol_size, color=ccolor, marker=cmarker, zorder=5)

    ax.set_xlabel('$network$ $coordinate_1$', fontsize=axis_label_size)
    ax.set_ylabel('$network$ $coordinate_2$', fontsize=axis_label_size)

    if plot_centroid:
        handles = [coordinator_label, worker_label, centroid_label, log_opt_label]
    else:
        handles = [coordinator_label, worker_label, log_opt_label]
    ax.legend(handles=handles, loc="upper left",
              bbox_to_anchor=(0, 1), prop={'size': leg_size})


def plot_topology(ax, df, colors=None, plot_voronoi=False, plot_centroid=False, centroids=None, title=None,
                  symbol_size=100, lval=0.2, scale_fac=0.25, centroid_color="grey", point_color="grey",
                  leg_size=12, axis_label_size=20, show_legend=True, show_coordinator=True):
    c_coords = df.loc[0, ["x", "y"]].tolist()
    clusters = df["cluster"][df["cluster"] >= 0].unique()

    for cluster in clusters:
        df_cluster = df[df["cluster"] == cluster]
        df_cluster_sources = df_cluster[df_cluster["type"] == "SOURCE"]
        df_cluster_workers = df_cluster[df_cluster["type"] == "WORKER"]

        if colors is not None:
            ax.scatter(df_cluster_sources["x"], df_cluster_sources["y"], s=scale_fac * symbol_size,
                       color=lighten_color(colors[cluster], lval), marker=src_marker, zorder=-1)
            ax.scatter(df_cluster_workers["x"], df_cluster_workers["y"], s=scale_fac * symbol_size,
                       color=lighten_color(colors[cluster], lval), marker=w_marker, zorder=-1)
            centroid_color = colors[cluster]
        else:
            ax.scatter(df_cluster_sources["x"], df_cluster_sources["y"], s=scale_fac * symbol_size,
                       color=lighten_color(point_color, lval), marker=src_marker, zorder=-1)
            ax.scatter(df_cluster_workers["x"], df_cluster_workers["y"], s=scale_fac * symbol_size,
                       color=lighten_color(point_color, lval), marker=w_marker, zorder=-1)

        if plot_centroid:
            if centroids is None:
                centroid = df_cluster[["x", "y"]].mean()
            else:
                centroid = centroids[cluster]
            ax.scatter(centroid["x"], centroid["y"], s=symbol_size, color=centroid_color, zorder=3,
                       label="centroid")

    if plot_voronoi:
        if centroids is None:
            centroids = df[df["cluster"] >= 0].groupby("cluster")[["x", "y"]].mean().to_numpy()
        vor = Voronoi(centroids)
        voronoi_plot_2d(vor, ax=ax, point_size=symbol_size, color="red", show_vertices=False, show_points=False)

    if show_coordinator:
        ax.scatter(c_coords[0], c_coords[1], s=2 * symbol_size, marker=cmarker, color='black')

    if plot_centroid:
        handles = [coordinator_label, source_label, worker_label, centroid_label]
    else:
        handles = [coordinator_label, source_label, worker_label]

    if show_legend:
        ax.legend(handles=handles, loc="upper left", bbox_to_anchor=(0, 1), fontsize=leg_size)
    ax.set_xlabel('$network$ $coordinate_1$', fontsize=axis_label_size)
    ax.set_ylabel('$network$ $coordinate_2$', fontsize=axis_label_size)
    if title is not None:
        ax.set_title(title)
    return ax


def plot_groups(ax, df, colors):
    c_coords = df.loc[0, ["x", "y"]].to_numpy()
    labels = df["cluster"].to_numpy()
    df.plot.scatter(ax=ax, x="x", y="y", color=colors[labels], s=df["capacity_" + str(100)] * 0.15)

    ax.scatter(c_coords[0], c_coords[1], s=100, marker=cmarker, color='black')

    ax.legend(handles=[coordinator_label, worker_label], loc="upper left", bbox_to_anchor=(0, 1), fontsize=8)
    ax.set_xlabel('$network$ $coordinate_1$')
    ax.set_ylabel('$network$ $coordinate_2$')
    return ax


def full_extent(ax, pad=0.0):
    """Get the full extent of an axes, including axes labels, tick labels, and
    titles."""
    # For text objects, we need to draw the figure first, otherwise the extents
    # are undefined.
    ax.figure.canvas.draw()
    items = ax.get_xticklabels() + ax.get_yticklabels()
    items += [ax, ax.title, ax.xaxis.label, ax.yaxis.label]
    items += [ax, ax.title]
    bbox = Bbox.union([item.get_window_extent() for item in items])

    return bbox.expanded(1.0 + pad, 1.0 + pad)


def format_number(num):
    """
    Format an integer as a string with 'k' for thousands and 'M' for millions.

    Args:
        num (int): The number to format.

    Returns:
        str: The formatted number as a string.
    """
    if num >= 1_000_000:
        return f"{num / 1_000_000:.1f}M"  # Format as millions
    elif num >= 1_000:
        return f"{num / 1_000:.1f}k"  # Format as thousands
    else:
        return str(num)  # Return as is for smaller numbers


def plot_distributions_pdf(ax, distributions, colors):
    """
    Plot the probability density functions (PDFs) of the distributions at each step.

    Parameters:
        distributions (list of np.ndarray): List of distributions to plot.
        ax (matplotlib.axes.Axes, optional): Axes object to plot on. If None, creates a new figure.
    """
    for i, dist in enumerate(distributions):
        # Estimate the PDF using Gaussian KDE
        kde = gaussian_kde(dist, bw_method=0.3)  # Adjust `bw_method` for smoothness
        x = np.linspace(np.min(dist), np.max(dist), 1000)
        y = kde(x)
        median = int(np.median(dist))

        # Plot the PDF
        ax.plot(
            x, y,
            color=colors[i],
            label=f'{median}',
            linewidth=2,
            alpha=0.8
        )

    # Add labels, and legend
    ax.set_xlabel('Value', fontsize=14)
    ax.set_ylabel('Density', fontsize=14)
    ax.legend(fontsize=12, loc='upper right', title="median")
    ax.grid(axis='y', linestyle='--', alpha=0.7)
    return ax


def add_arrow_label(ax, arrow, label, fontsize=10, ha='center', va='bottom', align_with_angle=False, label_pos=0.5,
                    x_offset=0, y_offset=0):
    # Ensure label_pos is within [0, 1]
    label_pos = max(0.0, min(1.0, label_pos))

    # Extract start and end positions of the arrow
    start = arrow.get_path().vertices[0]
    end = arrow.get_path().vertices[-1]

    # Calculate the label position along the arrow
    label_x = (1 - label_pos) * start[0] + label_pos * end[0] + x_offset
    label_y = (1 - label_pos) * start[1] + label_pos * end[1] + y_offset

    # Calculate the angle of the arrow
    angle = np.degrees(np.arctan2(end[1] - start[1], end[0] - start[0]))  # Angle in degrees

    # Add the label at the specified position
    if align_with_angle:
        # Rotate the label to align with the arrow's angle
        ax.text(label_x, label_y, label, fontsize=fontsize, ha=ha, va=va,
                rotation=angle, rotation_mode='anchor')
    else:
        # Standard placement without rotation
        ax.text(label_x, label_y, label, fontsize=fontsize, ha=ha, va=va)
