import numpy as np

from src.util import euclidean_distance
import pandas as pd
import heapq


def sink_based_placement(coords_dict, sinks, join_pairs, source_cardinalities, df_rtt=None):
    latency_hist = np.zeros(len(coords_dict))
    latency_hist_rtt = np.zeros(len(coords_dict))
    received_packets_hist = np.zeros(len(coords_dict))

    for i, j in join_pairs:
        for sink_idx in sinks:
            sink_coords = coords_dict[sink_idx]
            i_coords = coords_dict[i]
            j_coords = coords_dict[j]
            # calculate euclidean distance which corresponds to the cost space (latency)
            if df_rtt is not None:
                latency_hist_rtt[i] = max(latency_hist_rtt[i], df_rtt.at[i, sink_idx])
                latency_hist_rtt[j] = max(latency_hist_rtt[j], df_rtt.at[j, sink_idx])
            latency_hist[i] = max(latency_hist[i], euclidean_distance(i_coords, sink_coords))
            latency_hist[j] = max(latency_hist[j], euclidean_distance(j_coords, sink_coords))
            received_packets_hist[sink_idx] += source_cardinalities[i] + source_cardinalities[j]

    if df_rtt is not None:
        statistics = {"latency_distribution": latency_hist,
                      "latency_distribution_rtt": latency_hist_rtt,
                      "received_packets": received_packets_hist}
    else:
        statistics = {"latency_distribution": latency_hist,
                      "received_packets": received_packets_hist}
    df_stats = pd.DataFrame(statistics)
    return df_stats


def source_based_placement(df, coords_dict, join_matrix, join_pairs, source_cardinalities, df_rtt=None):
    latency_hist = np.zeros(df.shape[0])
    latency_hist_rtt = np.zeros(df.shape[0])
    received_packets_hist = np.zeros(df.shape[0])
    sinks = df[df["type"] == "SINK"].index.tolist()

    for i, j in join_pairs:
        card_i = source_cardinalities[i]
        card_j = source_cardinalities[j]

        for sink_idx in sinks:
            sink_coords = coords_dict[sink_idx]
            if card_i < card_j:
                parent_idx = j
                transmitter_idx = i
                card_transmitter = card_i
            else:
                parent_idx = i
                transmitter_idx = j
                card_transmitter = card_j
            output_cardinality = join_matrix[i, j]  # card_transmitter*card_parent

            # calculate euclidean distance which corresponds to the cost space (latency)
            if df_rtt is not None:
                latency_hist_rtt[parent_idx] = df_rtt.at[parent_idx, sink_idx]
                latency_hist_rtt[transmitter_idx] = df_rtt.at[transmitter_idx, parent_idx] + latency_hist_rtt[
                    parent_idx]

            parent_coords = coords_dict[parent_idx]
            latency_hist[parent_idx] = euclidean_distance(parent_coords, sink_coords)
            latency_hist[transmitter_idx] = euclidean_distance(coords_dict[transmitter_idx], parent_coords) + \
                                            latency_hist[parent_idx]
            received_packets_hist[parent_idx] += card_transmitter
            received_packets_hist[sink_idx] += output_cardinality

    if df_rtt is not None:
        statistics = {"latency_distribution": latency_hist,
                      "latency_distribution_rtt": latency_hist_rtt,
                      "received_packets": received_packets_hist}
    else:
        statistics = {"latency_distribution": latency_hist,
                      "received_packets": received_packets_hist}
    df_stats = pd.DataFrame(statistics)
    return df_stats


def top_c(df, coords_dict, join_matrix, join_pairs, source_cardinalities, capacity_col, df_rtt=None):
    latency_hist = np.zeros(df.shape[0])
    latency_hist_rtt = np.zeros(df.shape[0])
    received_packets_hist = np.zeros(df.shape[0])

    workers = df[df["type"] != "SINK"]
    workers_heap = list(zip(-workers[capacity_col], workers.index))
    heapq.heapify(workers_heap)
    sinks = df[df["type"] == "SINK"].index.tolist()

    for i, j in join_pairs:
        av, parent_idx = heapq.heappop(workers_heap)
        av = -av

        card_i = source_cardinalities[i]
        card_j = source_cardinalities[j]
        output_cardinality = join_matrix[i, j]
        received_packets_hist[parent_idx] += card_i + card_j

        # update parent
        av -= card_i + card_j
        heapq.heappush(workers_heap, (-av, parent_idx))

        # calculate euclidean distance which corresponds to the cost space (latency)
        for sink_idx in sinks:
            sink_coords = coords_dict[sink_idx]
            parent_coords = coords_dict[parent_idx]
            received_packets_hist[sink_idx] += output_cardinality

            if df_rtt is not None:
                sink_latency = df_rtt.at[parent_idx, sink_idx]
                latency_hist_rtt[i] = max(latency_hist_rtt[i], df_rtt.at[i, parent_idx] + sink_latency)
                latency_hist_rtt[j] = max(latency_hist_rtt[j], df_rtt.at[j, parent_idx] + sink_latency)
            sink_latency = euclidean_distance(parent_coords, sink_coords)
            latency_hist[i] = max(latency_hist[i], euclidean_distance(coords_dict[i], parent_coords) + sink_latency)
            latency_hist[j] = max(latency_hist[j], euclidean_distance(coords_dict[j], parent_coords) + sink_latency)

    if df_rtt is not None:
        statistics = {"latency_distribution": latency_hist,
                      "latency_distribution_rtt": latency_hist_rtt,
                      "received_packets": received_packets_hist}
    else:
        statistics = {"latency_distribution": latency_hist,
                      "received_packets": received_packets_hist}
    df_stats = pd.DataFrame(statistics)
    return df_stats
