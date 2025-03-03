import numpy as np
from scipy.spatial import cKDTree
import heapq
import sys
from src.util import euclidean_distance


def prim_mst(nodes, root=0, df_rtt=None):
    if df_rtt is not None:
        g = Graph(df_rtt.shape[0])
        g.graph = df_rtt.values
        return g.primMST()

    # Create a list of coordinates and a mapping from index to node ID
    node_ids = list(nodes.keys())
    coordinates = [nodes[node_id] for node_id in node_ids]
    kd_tree = cKDTree(coordinates)

    # Initializations
    mst_set = set()  # Set of nodes included in MST
    parent = {node: None for node in nodes}  # Parent dictionary for MST
    min_heap = [(0, root, None)]  # (distance, current_node, parent)

    # Map node IDs to their index in the coordinates list
    node_index_map = {node_id: i for i, node_id in enumerate(node_ids)}

    # Main loop to build MST
    while len(mst_set) < len(nodes):
        # Pop the nearest node from the min heap
        weight, current_node, parent_node = heapq.heappop(min_heap)

        # Skip if already in the MST
        if current_node in mst_set:
            continue

        # Add current node to MST and record its parent
        mst_set.add(current_node)
        parent[current_node] = parent_node

        # Get the k-d tree index of the current node
        current_index = node_index_map[current_node]

        # Query the k-d tree to find the nearest neighbors within a reasonable search radius
        distances, indices = kd_tree.query(coordinates[current_index], k=len(nodes))

        # Process the nearest neighbors
        for distance, index in zip(distances, indices):
            neighbor = node_ids[index]
            if neighbor not in mst_set:
                heapq.heappush(min_heap, (distance, neighbor, current_node))

    # Remove the root node from the parent dictionary
    del parent[root]
    return parent


def create_routes_dict(mst_dict, root=0):
    routes = {}
    routes[root] = []
    for node in mst_dict.keys():
        route = []
        current = node
        while current != root:
            current = mst_dict[current]
            route.append(current)
        routes[node] = route
    return routes


def prim_mst_coords(coordinates, df_rtt=None):
    if df_rtt is not None:
        g = Graph(df_rtt.shape[0])
        g.graph = df_rtt.values
        return g.primMST()

    num_nodes = len(coordinates)
    visited = np.zeros(num_nodes, dtype=bool)
    parents = [-1] * num_nodes

    # Start with the first node as the root
    visited[0] = True

    # Build the k-d tree for efficient nearest neighbor search
    kdtree = cKDTree(coordinates)

    # Priority queue to keep track of the closest edge to the MST
    priority_queue = []
    for neighbor_idx in kdtree.query(coordinates[0], k=num_nodes)[1]:
        heapq.heappush(priority_queue, (euclidean_distance(coordinates[0], coordinates[neighbor_idx]), neighbor_idx, 0))

    while priority_queue:
        # Get the closest edge from the priority queue
        distance, node_idx, parent_idx = heapq.heappop(priority_queue)

        if not visited[node_idx]:
            # Add the edge to the MST and update the parent
            parents[node_idx] = parent_idx
            visited[node_idx] = True

            # Update the priority queue with the new closest edges
            for neighbor_idx in kdtree.query(coordinates[node_idx], k=num_nodes)[1]:
                if not visited[neighbor_idx]:
                    heapq.heappush(priority_queue, (
                        euclidean_distance(coordinates[node_idx], coordinates[neighbor_idx]), neighbor_idx, node_idx))

    return parents


def create_routes(mst):
    routes = {}
    for i in range(1, len(mst)):
        route = []
        j = i
        # route from one node
        while True:
            node = mst[j]
            route.append(node)
            j = mst[j]
            if j == 0:
                routes[i] = route.copy()
                break
    return routes


class Graph():
    def __init__(self, vertices):
        self.V = vertices
        self.graph = [[0 for column in range(vertices)]
                      for row in range(vertices)]

    # A utility function to print
    # the constructed MST stored in parent[]
    def printMST(self, parent):
        print("Edge \tWeight")
        for i in range(1, self.V):
            print(parent[i], "-", i, "\t", self.graph[i][parent[i]])

    # A utility function to find the vertex with
    # minimum distance value, from the set of vertices
    # not yet included in shortest path tree
    def minKey(self, key, mstSet):

        # Initialize min value
        min = sys.maxsize

        for v in range(self.V):
            if key[v] < min and mstSet[v] == False:
                min = key[v]
                min_index = v

        return min_index

    # Function to construct and print MST for a graph
    # represented using adjacency matrix representation
    def primMST(self):

        # Key values used to pick minimum weight edge in cut
        key = [sys.maxsize] * self.V
        parent = [None] * self.V  # Array to store constructed MST
        # Make key 0 so that this vertex is picked as first vertex
        key[0] = 0
        mstSet = [False] * self.V

        parent[0] = -1  # First node is always the root of

        for cout in range(self.V):

            # Pick the minimum distance vertex from
            # the set of vertices not yet processed.
            # u is always equal to src in first iteration
            u = self.minKey(key, mstSet)

            # Put the minimum distance vertex in
            # the shortest path tree
            mstSet[u] = True

            # Update dist value of the adjacent vertices
            # of the picked vertex only if the current
            # distance is greater than new distance and
            # the vertex in not in the shortest path tree
            for v in range(self.V):

                # graph[u][v] is non zero only for adjacent vertices of m
                # mstSet[v] is false for vertices not yet included in MST
                # Update the key only if graph[u][v] is smaller than key[v]
                if self.graph[u][v] > 0 and mstSet[v] == False \
                        and key[v] > self.graph[u][v]:
                    key[v] = self.graph[u][v]
                    parent[v] = u

        return parent
