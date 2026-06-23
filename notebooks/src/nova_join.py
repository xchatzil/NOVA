import itertools
import numpy as np
import pandas as pd
import networkx as nx
from scipy.spatial import cKDTree
from collections import defaultdict
import src.util as util
from src.util import evaluate, product_decomposition_with_mapping, sigmoid, euclidean_distance
from src.spring_optimization import gradient_descent_with_momentum, spring_relaxation_parallel
from enum import Enum
from src.load_distribution import greedy_assignment_batched, eval_overloaded_nodes_batched
from annoy import AnnoyIndex
import timeit


# Define an enum class
class OperatorType(Enum):
    SOURCE = 1
    SINK = 2
    AGG = 3
    FILTER = 4
    JOIN = 5
    UDF = 6
    NA = 7

    # Override the __str__ method to customize string output
    def __str__(self):
        return f'{self.name}'


class Operator:
    def __init__(self, id, operator_type, is_decomposable):
        self.id = id
        self.operator_type = operator_type
        self.is_decomposable = is_decomposable

    def __str__(self):
        return f"{self.operator_type}"


class NovaSolver:
    def __init__(self, df, logical_plan, cardinality_dict=None, coord_cols=None, cluster_col=None,
                 knn_neighbors=None, knn_radius=None, pre_knn=False, k_resources=0, knn_trees=10,
                 latency_col='latency', type_col='type', capacity_col='capacity', use_weight=True,
                 reduction_fac=0.2, tolerance=1e-5, max_iterations=100, num_workers=1):
        print(f"Initializing Nova for {df.shape[0]}")
        self.placement = False
        # existing labels
        self.coord_cols = coord_cols
        self.cluster_col = cluster_col
        self.latency_col = latency_col
        self.type_col = type_col
        self.capacity_col = capacity_col
        # new labels
        self.parent_col = 'parent'
        self.av_col = 'free_capacity'
        self.unbalanced_col = 'unbalanced'
        self.weight_col = 'weight'
        self.real_weight_col = 'real_weight'
        # output columns
        if self.cluster_col is None:
            self.nova_columns = [*self.coord_cols, self.latency_col, self.type_col, self.capacity_col, 'level',
                                 self.av_col, self.unbalanced_col, self.weight_col, self.real_weight_col, 'oindex',
                                 self.parent_col]
            self.out_cols = (
                    self.coord_cols + [self.type_col, self.unbalanced_col, self.weight_col, self.real_weight_col,
                                       self.capacity_col, self.av_col] + ["level", self.parent_col])
        else:
            self.nova_columns = [*self.coord_cols, self.latency_col, self.cluster_col, self.type_col,
                                 self.capacity_col, 'level', self.av_col,
                                 self.unbalanced_col, self.weight_col, self.real_weight_col,
                                 'oindex', self.parent_col]
            self.out_cols = ([*self.coord_cols, self.cluster_col, self.type_col, self.unbalanced_col, self.weight_col,
                              self.real_weight_col, self.capacity_col, self.av_col] + ["level", self.parent_col])

        # parameters
        self.use_weight = use_weight
        self.reduction_fac = reduction_fac
        self.logical_plan = logical_plan
        self.cardinality_dict = cardinality_dict
        self.tolerance = tolerance
        self.max_iterations = max_iterations
        self.num_workers = num_workers
        self.knn_neighbors = knn_neighbors
        self.knn_radius = knn_radius
        self.k_resources = k_resources

        # coordinates
        if coord_cols is None:
            coord_cols = ['x', 'y']
        if cluster_col:
            self.required_attributes = [*coord_cols, latency_col, cluster_col, type_col, capacity_col]
        else:
            self.required_attributes = [*coord_cols, latency_col, type_col, capacity_col]

        self.df_nova = df[self.required_attributes].copy()
        self.max_index = len(df.index)
        if OperatorType.NA.name not in self.df_nova[self.type_col].cat.categories:
            self.df_nova[self.type_col] = self.df_nova[self.type_col].cat.add_categories([OperatorType.NA.name])
        self.init_coords = self.df_nova[self.coord_cols].mean().to_numpy()
        self.coord_dict = self.df_nova[self.coord_cols].apply(list, axis=1).to_dict()

        # levels
        self.df_nova['level'] = 0
        self.opt_dict_levels = dict()

        # resources
        self.df_nova[self.av_col] = self.df_nova[self.capacity_col]
        self.df_nova[self.av_col] = self.df_nova[self.av_col].astype(float)
        self.df_nova[self.unbalanced_col] = 0.0
        self.df_nova[self.weight_col] = 0
        self.df_nova[self.real_weight_col] = 0

        # assignments
        knn_mask = (self.df_nova['type'] != OperatorType.SINK.name) & (self.df_nova['type'] != OperatorType.NA.name)
        self.knn_indices = set(self.df_nova.loc[knn_mask].index)
        self.sink_indices = set(self.df_nova[self.df_nova['type'] == OperatorType.SINK.name].index)
        self.source_indices = set(self.df_nova[self.df_nova['type'] == OperatorType.SOURCE.name].index)
        self.df_nova['oindex'] = self.df_nova.index

        # parents of each node
        self.df_nova[self.parent_col] = [[]] * self.df_nova.shape[0]

        # replication plan
        self.replication_plan = nx.DiGraph()
        self.new_op_index = max(self.df_nova.index) + 1
        self.operator_dict = dict()

        # knn
        self.loaded_nodes = 0
        self.knn_trees = knn_trees
        self.annoy_index = None
        if self.cluster_col:
            df_filtered = self.df_nova[self.df_nova['cluster'] >= 0]
            self.df_centroids = df_filtered.groupby('cluster')[self.coord_cols].mean().reset_index()
            print(self.df_centroids)
            self.num_clusters = self.df_centroids.shape[0]
            knn_coords = self.df_centroids[self.coord_cols].to_numpy()
            self.centroid_tree = cKDTree(knn_coords)

        if pre_knn:
            self.annoy_index = AnnoyIndex(len(self.coord_cols), 'euclidean')
            self.knn_nodes = self.df_nova.loc[list(self.knn_indices)]
            self.knn_nodes = self.knn_nodes[self.knn_nodes[self.av_col] >= k_resources].index.tolist()
            print("Creating kNN index")
            for i, idx in enumerate(self.knn_nodes):
                self.annoy_index.add_item(idx, self.coord_dict[i])
            self.annoy_index.build(self.knn_trees)
            print("Finished kNN index")
        else:
            self.knn_nodes = list(self.knn_indices)

        # placement dict
        self.relaxed_positions = dict()
        self.placed_positions = dict()
        self.placed_operators = defaultdict(set)
        # lookups
        self.children_dict = defaultdict(set)

    def knn_search(self, opt, neighbor_size, max_required):
        if not self.annoy_index and self.k_resources > 0:
            mask = self.df_nova[self.av_col] >= self.k_resources
            knn_nodes = self.df_nova.loc[list(self.knn_indices)].loc[mask].index.tolist()
            if neighbor_size:
                k_size = neighbor_size
            else:
                k_size = max(2, max_required // self.df_nova.loc[knn_nodes, self.av_col].median())
        else:
            knn_nodes = self.knn_nodes
            k_size = neighbor_size + self.loaded_nodes

        if len(knn_nodes) < k_size:
            print(f"Warning: neighbor_size={k_size} > knn_nodes={len(knn_nodes)}")
            if len(knn_nodes) <= 0:
                print("No suitable knn nodes found, expanding knn nodes to topology and setting neighbors to 1")
                k_size = 1
                knn_nodes = list(self.knn_indices)
            else:
                print("Setting neighbor_size={len(knn_nodes)}")
                k_size = len(knn_nodes)

        print("Searching knn", opt, len(knn_nodes), neighbor_size, k_size, self.loaded_nodes)
        idx_order = []
        if self.annoy_index:
            idx_order = self.annoy_index.get_nns_by_vector(opt, k_size, include_distances=False)
        elif self.cluster_col:
            idx_order = self.centroid_tree.query(opt, k=self.num_clusters)[1]
            df = self.df_nova[self.df_nova['cluster'] == idx_order[0]].copy()
            df = df.sort_values(by=self.av_col, ascending=False)
            idx_order = df.index.tolist()
        else:
            knn_coords = self.df_nova.loc[knn_nodes, self.coord_cols].to_numpy()
            full_kdtree = cKDTree(knn_coords)
            if self.knn_radius:
                init_r = self.knn_radius
                while (len(idx_order) < k_size) and (len(idx_order) < len(knn_coords)):
                    print(init_r, len(idx_order), len(knn_coords))
                    idx_order = full_kdtree.query_ball_point(opt, r=init_r)
                    init_r = 2 * init_r
            else:
                idx_order = full_kdtree.query(opt, k=k_size)[1]

            idx_order = list(np.take(knn_nodes, idx_order))

        idx_order = self.df_nova.iloc[idx_order].sort_values(by=self.av_col, ascending=False).index.tolist()
        return idx_order

    def resolve_pinned_operators(self, source=1):
        # pinned operators are sources and sinks
        upstream_op = None
        pinned_nodes = set()
        for log_op in nx.dfs_preorder_nodes(self.logical_plan, source=source):
            if upstream_op:
                up_type = self.logical_plan.nodes[upstream_op]["type"].name
                down_type = self.logical_plan.nodes[log_op]["type"].name
                if up_type == OperatorType.SOURCE.name:
                    up_ids = self.df_nova[self.df_nova[self.type_col] == up_type].index
                    self.operator_dict[upstream_op] = up_ids.tolist()
                else:
                    # if upstream nodes are no sources, get the id of the existing operator
                    up_ids = self.operator_dict[upstream_op]

                if down_type == OperatorType.SINK.name:
                    down_ids = self.df_nova[self.df_nova[self.type_col] == down_type].index
                    self.operator_dict[log_op] = down_ids.tolist()
                else:
                    # if downstream nodes are no sinks, create new virtual operator
                    self.operator_dict[log_op] = [self.new_op_index]
                    down_ids = [self.new_op_index]
                    self.new_op_index += 1

                for up_id in up_ids:
                    for down_id in down_ids:
                        self.replication_plan.add_node(up_id, type=up_type, origin=upstream_op)
                        self.replication_plan.add_node(down_id, type=down_type, origin=log_op)

                        if all(x in [up_id, down_id] for x in self.df_nova.index):
                            weight = np.linalg.norm(self.coord_dict[up_id] - self.coord_dict[down_id])
                        else:
                            weight = np.nan

                        if log_op in self.cardinality_dict:
                            spring_k = self.cardinality_dict[log_op - 1][up_id]
                        else:
                            spring_k = np.nan

                        # spring_k = 1 + sigmoid(spring_k, self.sigmoid_k)
                        self.replication_plan.add_edge(up_id, down_id, distance=weight, cardinality=spring_k)

                        if {up_type} & {OperatorType.SOURCE.name, OperatorType.SINK.name}:
                            pinned_nodes.add(up_id)
                        if {down_type} & {OperatorType.SOURCE.name, OperatorType.SINK.name}:
                            pinned_nodes.add(down_id)

            upstream_op = log_op
        return pinned_nodes

    def resolve_join_operators(self):
        node_removes = []
        for op in self.logical_plan.nodes:
            if self.logical_plan.nodes[op]["type"].name == OperatorType.JOIN.name:
                rep_op_id = self.operator_dict[op][0]
                self.operator_dict[op] = []

                # look up the connections in the adjacency matrix
                if op in self.cardinality_dict:
                    # if an adjacency matrix exists, use these connections
                    adj_matrix, join_pairs = self.cardinality_dict[op]
                    for i, j in join_pairs:
                        if adj_matrix[i, j] > 0:
                            # create a join replica for each connection in the adjacency matrix
                            self.create_var_join_operator(i, j, op, rep_op_id)
                else:
                    # all nodes need to be joined
                    upstream_pairs = self.replication_plan.predecessors(rep_op_id)
                    for n1, n2 in itertools.combinations(upstream_pairs, 2):
                        self.create_var_join_operator(n1, n2, op, rep_op_id)

                # remove the old join operator
                node_removes.append(rep_op_id)
        self.replication_plan.remove_nodes_from(node_removes)

    def create_var_join_operator(self, l_node, r_node, log_op_id, rep_op_id):
        new_op_id = self.new_op_index
        self.new_op_index += 1
        self.operator_dict[log_op_id].append(new_op_id)
        # get cards upstream nodes
        left_card = self.cardinality_dict[log_op_id - 1][l_node]
        right_card = self.cardinality_dict[log_op_id - 1][r_node]
        join_matrix, join_pairs = self.cardinality_dict[log_op_id]
        output_card = join_matrix[l_node, r_node]

        # create the edges in the graph
        self.replication_plan.add_node(new_op_id, type=OperatorType.JOIN, origin=log_op_id)
        self.replication_plan.add_edge(l_node, new_op_id, distance=np.nan, cardinality=left_card)
        self.replication_plan.add_edge(r_node, new_op_id, distance=np.nan, cardinality=right_card)
        for downstream_node in self.replication_plan.successors(rep_op_id):
            if left_card and right_card:
                out_card = output_card
            else:
                out_card = np.nan
            self.replication_plan.add_edge(new_op_id, downstream_node, distance=np.nan, cardinality=out_card)

    def nova_full(self, source=1):
        if self.placement:
            raise RuntimeError('Nova already terminated. Use re-optimize or re-instantiate NovaSolver')
        print(f"Running Nova on {self.capacity_col}")

        # create replication plan
        print("Resolving pinned operators")
        fixed_ids = self.resolve_pinned_operators(source=source)
        print("Resolving join operators")
        self.resolve_join_operators()

        # get nodes and edges of replication plan
        print("Preparing nodes and edges")
        nodes = self.df_nova.loc[list(fixed_ids), self.coord_cols].apply(lambda row: row.to_numpy(), axis=1).to_dict()

        # set init coords for var nodes, which is needed for spring relaxation
        print("Initializing coordinates")
        var_nodes = set(self.replication_plan.nodes).difference(fixed_ids)
        nodes.update(dict.fromkeys(var_nodes, self.init_coords))

        # compute force equilibrium with spring relaxation
        print(f"Computing relaxed positions for n={len(nodes)} nodes")
        self.relaxed_positions = gradient_descent_with_momentum(
            nodes, var_nodes, self.replication_plan, tolerance=self.tolerance, max_iters=self.max_iterations,
            use_weight=self.use_weight)

        print("Relaxed positions:", len(self.relaxed_positions))
        level = 0
        for log_op in nx.dfs_preorder_nodes(self.logical_plan, source=source):
            replicas = self.operator_dict[log_op].copy()
            print(f"Replicas for {log_op}:{len(replicas)}")
            rep_count = 0
            for replica in replicas:
                if replica in fixed_ids:
                    # node is pinned
                    self.placed_positions[replica] = replica
                    self.placed_operators[replica].add(replica)

                    if self.logical_plan.nodes[log_op]['type'].name == OperatorType.SINK.name:
                        # set parent of upstream nodes to the sink
                        for upstream_node in self.logical_plan.predecessors(log_op):
                            for upstream_replica in self.operator_dict[upstream_node]:
                                # get the nodes where the replicas are placed
                                child_idx = self.placed_positions[upstream_replica]
                                resources = self.replication_plan.edges[upstream_replica, replica]['cardinality']
                                self.add_parent(child_idx, replica, resources)
                                self.df_nova.at[replica, "level"] = level
                                w = util.euclidean_distance(self.coord_dict[child_idx], self.coord_dict[replica])
                                self.replication_plan.edges[upstream_replica, replica]["distance"] = w
                                rep_count += 1
                else:
                    # node is variable, place it
                    all_upstream_nodes = list(self.replication_plan.predecessors(replica))
                    # self.placed_positions is updated in the method
                    print(
                        f"Distributing load: operator_id={replica}, opt={self.relaxed_positions[replica]}, upstream_nodes=({all_upstream_nodes})")
                    self.place_and_replicate_operator(replica, all_upstream_nodes, self.relaxed_positions[replica],
                                                      level=level)
            print(f"Replication count for upstream operators to {log_op} -> {rep_count}")
            level += 1

        df_out = self.df_nova[self.out_cols]
        df_out = df_out.rename(columns={self.capacity_col: "total_capacity"})

        self.placement = True
        df_out[self.unbalanced_col] = df_out[self.unbalanced_col].apply(lambda x: 0 if abs(x) < 0.0001 else x)
        return df_out, self.replication_plan, self.relaxed_positions, self.placed_positions

    def place_and_replicate_operator(self, op_id, upstream_nodes, opt, level=1):
        log_op = self.replication_plan.nodes[op_id]['origin']
        op_type = self.replication_plan.nodes[op_id]["type"]

        if op_type.name is OperatorType.JOIN.name:
            assert (len(upstream_nodes) == 2)
            join_matrix, join_pairs = self.cardinality_dict[log_op]
            left_node = upstream_nodes[0]
            right_node = upstream_nodes[1]

            # if nodes have common keys
            if join_matrix[left_node, right_node] > 0:
                # compute the neighborhood
                left_card = self.replication_plan.get_edge_data(left_node, op_id).get('cardinality')
                right_card = self.replication_plan.get_edge_data(right_node, op_id).get('cardinality')

                # create batches equal to the number of tuples
                load_reduction_threshold = max(1, (self.reduction_fac * (left_card + right_card)) // 2)
                batches, total, partitions, required, part_req = product_decomposition_with_mapping(
                    left_card, right_card, [load_reduction_threshold], False)
                assert total == left_card * right_card  # sanity check, should never fail

                print(
                    f"Left={left_card}, right={right_card}, batches={len(batches)}, batch_factor={self.reduction_fac}, "
                    f"batch_threshold={load_reduction_threshold}, required={required}, part_req={part_req} "
                    f"resource_limit={self.k_resources}, neighbor_size={self.knn_neighbors}")

                idx_order = self.knn_search(opt, self.knn_neighbors, required)
                # print("Index order:", idx_order)

                # distribute the load of the join across nodes in idx_order
                new_ops, unbalanced = self.distribute_load_join(left_node, right_node, idx_order, batches, op_id,
                                                                opt, level=level)
                self.replication_plan.remove_node(op_id)
                self.operator_dict[log_op].remove(op_id)
                del self.relaxed_positions[op_id]
                return new_ops, unbalanced
            else:
                print(f"Error: Nodes={left_node, right_node} not in join matrix")
        else:
            print("Operator not supported for replication", op_id, op_type.name)
            # operator cannot be replicated, just place it to the closest not-sink node
            idx_order = self.knn_search(opt, 1, None)
            self.placed_positions[op_id] = idx_order[0]
            self.placed_operators[idx_order[0]].add(op_id)
        return self.placed_positions[op_id], None

    def distribute_load_join(self, left_node, right_node, dest_nodes, batches, rep_id, opt, level=1):
        log_op = self.replication_plan.nodes[rep_id]['origin']
        left_card = self.replication_plan.get_edge_data(left_node, rep_id).get('cardinality')
        right_card = self.replication_plan.get_edge_data(right_node, rep_id).get('cardinality')
        join_matrix, join_pairs = self.cardinality_dict[log_op]
        output_card = join_matrix[left_node, right_node]

        self.df_nova.at[left_node, self.weight_col] = left_card
        self.df_nova.at[right_node, self.weight_col] = right_card

        workers = self.df_nova.loc[dest_nodes, self.av_col].to_dict()
        assignments, uniques = greedy_assignment_batched(workers, batches)
        total_card = left_card * right_card
        # for worker_id, sub_product in uniques.items():
        #    total_card += len(sub_product[0]) * len(sub_product[1])

        output_load = 0
        new_operators = []
        for worker_id, sub_product in uniques.items():
            sub_cardinality_left = len(sub_product[0])
            sub_cardinality_right = len(sub_product[1])
            if sub_cardinality_left * sub_cardinality_right == 0:
                continue

            cardinality = ((sub_cardinality_left * sub_cardinality_right) / total_card) * output_card
            cardinality = round(cardinality, 2)

            # updates parents in df_nova
            self.df_nova.at[left_node, self.unbalanced_col] += sub_cardinality_left
            self.df_nova.at[left_node, self.real_weight_col] += sub_cardinality_left
            self.df_nova.at[right_node, self.unbalanced_col] += sub_cardinality_right
            self.df_nova.at[right_node, self.real_weight_col] += sub_cardinality_right

            self.add_parent(left_node, worker_id, sub_cardinality_left)
            self.add_parent(right_node, worker_id, sub_cardinality_right)
            self.df_nova.at[worker_id, self.unbalanced_col] += cardinality
            self.df_nova.at[worker_id, "level"] = level

            # update replication plan
            new_op_id = self.new_op_index
            self.new_op_index += 1
            self.replication_plan.add_node(new_op_id, type=OperatorType.JOIN, origin=log_op)
            self.operator_dict[log_op].append(new_op_id)
            self.relaxed_positions[new_op_id] = opt
            self.placed_positions[new_op_id] = worker_id
            self.placed_operators[worker_id].add(new_op_id)

            # create the edges in the graph
            for out_edge in self.replication_plan.out_edges(rep_id, data=True):
                self.replication_plan.add_edge(new_op_id, out_edge[1], distance=np.nan, cardinality=cardinality)
                # print(f"Assigning output cardinality to parent {worker_id}={cardinality}")

            w_left = util.euclidean_distance(self.coord_dict[left_node], self.coord_dict[worker_id])
            self.replication_plan.add_edge(left_node, new_op_id, distance=w_left, cardinality=sub_cardinality_left)

            w_right = util.euclidean_distance(self.coord_dict[right_node], self.coord_dict[worker_id])
            self.replication_plan.add_edge(right_node, new_op_id, distance=w_right, cardinality=sub_cardinality_right)

            output_load += cardinality
            new_operators.append(new_op_id)

        output_load = output_card - output_load
        print(f"Distributing finished: operator_id={rep_id} ({left_node}, {right_node}) to n={len(assignments)}, "
              f"opt={self.relaxed_positions[rep_id]}, upstream_nodes={list(assignments.keys())}, additional load={output_load}")
        return new_operators, output_load

    def remove_node(self, node_id):
        if (node_id not in self.df_nova.index) or (self.df_nova.at[node_id, self.type_col] == OperatorType.NA.name):
            print("Node not in index", node_id)
            df_out = self.df_nova[self.out_cols]
            df_out = df_out.rename(columns={self.capacity_col: "total_capacity"})
            return df_out, self.replication_plan, self.relaxed_positions, self.placed_positions

        affected_operators = self.placed_operators[node_id]
        re_placements = set()
        removes = set()

        # remove node from indices
        self.df_nova.at[node_id, self.type_col] = OperatorType.NA.name
        self.knn_indices.remove(node_id)
        if self.df_nova.at[node_id, self.type_col] == OperatorType.SOURCE.name:
            self.source_indices.remove(node_id)

        # identify affected operators
        for op in affected_operators:
            for n1, n2 in list(nx.bfs_edges(self.replication_plan, source=op)):
                n_type = self.replication_plan.nodes[n1]['type']

                if str(n_type) == 'JOIN':
                    join_nodes = set(self.replication_plan.predecessors(n1))
                    if node_id in join_nodes:
                        removes.add(n1)
                    else:
                        re_placements.add(n1)
                elif str(n_type) == 'SOURCE':
                    removes.add(n1)
                else:
                    raise RuntimeError(f"Operator with type {n_type} not supported for removal")

        print("Removals", removes)
        for op in removes:
            n_type = str(self.replication_plan.nodes[op]['type'])
            if n_type == 'SOURCE':
                self.remove_parents([op])
            elif n_type == 'JOIN':
                out_edges = self.replication_plan.out_edges(op, data=True)
                in_edges = self.replication_plan.in_edges(op, data=True)
                for n1, n2, data in list(out_edges) + list(in_edges):
                    print(n1, n2, data)
                    child = self.placed_positions[n1]
                    parent = self.placed_positions[n2]
                    cardinality = data['cardinality']
                    self.remove_parent(child, parent, cardinality)
            else:
                raise RuntimeError(f"Operator with type {n_type} not supported for removal")
            self.operator_dict[self.replication_plan.nodes[op]['origin']].remove(op)
            self.replication_plan.remove_node(op)
            del self.relaxed_positions[op]
            del self.placed_positions[op]
        del self.placed_operators[node_id]

        print("Re-placements", re_placements)
        for op in re_placements:
            out_edges = self.replication_plan.out_edges(op, data=True)
            in_edges = self.replication_plan.in_edges(op, data=True)
            for n1, n2, data in list(out_edges) + list(in_edges):
                child = self.placed_positions[n1]
                parent = self.placed_positions[n2]
                cardinality = data['cardinality']
                self.remove_parent(child, parent, cardinality)

            all_upstream_nodes = list(self.replication_plan.predecessors(op))
            if len(all_upstream_nodes) == 2:
                # self.placed_positions is updated in the method
                level = self.replication_plan.nodes[op]['origin'] - 1
                print(
                    f"Replacing operator={op}, opt={self.relaxed_positions[op]}, upstream_nodes=({all_upstream_nodes})")
                new_ops, unbalanced = self.place_and_replicate_operator(op, all_upstream_nodes,
                                                                        self.relaxed_positions[op],
                                                                        level=level)
                # set parent of upstream nodes to the sink
                for replica in self.sink_indices:
                    for upstream_replica in new_ops:
                        # get the nodes where the replicas are placed
                        child_idx = self.placed_positions[upstream_replica]
                        resources = self.replication_plan.edges[upstream_replica, replica]['cardinality']
                        self.add_parent(child_idx, replica, resources)
                        self.df_nova.at[replica, "level"] = level
                        w = euclidean_distance(self.coord_dict[child_idx], self.coord_dict[replica])
                        self.replication_plan.edges[upstream_replica, replica]["distance"] = w
            else:
                print(f"Removing operator={op} due to missing upstream nodes ({all_upstream_nodes})")
                self.replication_plan.remove_node(op)

        df_out = self.df_nova[self.out_cols]
        df_out = df_out.rename(columns={self.capacity_col: "total_capacity"})
        return df_out, self.replication_plan, self.relaxed_positions, self.placed_positions

    def add_node(self, value_dict, join_dict):
        # Columns to check for in the DataFrame
        required_keys = ['x', 'y', 'weight', 'capacity', 'type']
        if not all(key in value_dict for key in required_keys):
            raise ValueError('Attributes are missing for node', value_dict)

        # prepare missing values
        node_id = self.new_op_index
        self.new_op_index += 1
        node_coords = np.array([value_dict['x'], value_dict['y']])
        value_dict[self.latency_col] = euclidean_distance(node_coords, self.coord_dict[0])
        value_dict[self.capacity_col] = value_dict['capacity']
        value_dict['level'] = 0
        value_dict[self.av_col] = value_dict[self.capacity_col]
        value_dict[self.unbalanced_col] = value_dict[self.weight_col]
        value_dict[self.real_weight_col] = value_dict[self.weight_col]
        value_dict['oindex'] = node_id
        value_dict[self.parent_col] = []
        # insert new node to df
        df = pd.DataFrame([value_dict])
        df = df[self.nova_columns]
        self.df_nova.loc[node_id] = df.loc[0, :]
        self.knn_indices.add(node_id)
        self.coord_dict[node_id] = node_coords

        print(f"Adding new node with ID={node_id} and values={value_dict}")
        if value_dict['type'] in ['SOURCE']:
            self.source_indices.add(node_id)
            self.operator_dict[1].append(node_id)
            cardinality_dict = self.cardinality_dict[1]
            cardinality_dict[node_id] = value_dict['weight']
            self.replication_plan.add_node(node_id, type=OperatorType.SOURCE, origin=1)
            self.placed_positions[node_id] = node_id
            self.placed_operators[node_id].add(node_id)

            # update join matrix and pairs
            log_op_id = 2
            join_matrix, join_pairs = self.cardinality_dict[log_op_id]
            join_matrix.resize((node_id + 1, node_id + 1))
            for join_node, output_card in join_dict.items():
                join_matrix[node_id, join_node] = output_card
                join_matrix[join_node, node_id] = output_card

            nodes = {node_id}
            var_nodes = []
            for r_node in join_dict.keys():
                join_pairs.append((node_id, r_node))

                new_op_id = self.new_op_index
                self.new_op_index += 1
                self.operator_dict[log_op_id].append(new_op_id)
                var_nodes.append(new_op_id)

                # get cards upstream nodes
                left_card = cardinality_dict[node_id]
                right_card = cardinality_dict[r_node]
                output_card = join_matrix[node_id, r_node]

                # create the edges in the graph
                self.replication_plan.add_node(new_op_id, type=OperatorType.JOIN, origin=log_op_id)
                self.replication_plan.add_edge(node_id, new_op_id, distance=np.nan, cardinality=left_card)
                self.replication_plan.add_edge(r_node, new_op_id, distance=np.nan, cardinality=right_card)
                for downstream_node in self.sink_indices:
                    if left_card and right_card:
                        out_card = output_card
                    else:
                        out_card = np.nan
                    self.replication_plan.add_edge(new_op_id, downstream_node, distance=np.nan, cardinality=out_card)
                    nodes.update({r_node, downstream_node})

            # place the newly created operator
            nodes = self.df_nova.loc[list(nodes), self.coord_cols].apply(lambda row: row.to_numpy(),
                                                                         axis=1).to_dict()
            nodes.update(dict.fromkeys(var_nodes, self.init_coords))

            new_relaxed_positions = gradient_descent_with_momentum(
                nodes, var_nodes, self.replication_plan, tolerance=self.tolerance,
                max_iters=self.max_iterations,
                use_weight=self.use_weight)
            self.relaxed_positions.update(new_relaxed_positions)

            for new_op in var_nodes:
                all_upstream_nodes = list(self.replication_plan.predecessors(new_op))
                print(
                    f"Distributing load: operator_id={new_op}, opt={self.relaxed_positions[new_op]}, upstream_nodes=({all_upstream_nodes})")
                new_ops, unbalanced = self.place_and_replicate_operator(new_op, all_upstream_nodes,
                                                                        self.relaxed_positions[new_op], level=1)
                # set parent of upstream nodes to the sink
                for sink_idx in self.sink_indices:
                    for op_rep in new_ops:
                        # get the nodes where the replicas are placed
                        child_idx = self.placed_positions[op_rep]
                        resources = self.replication_plan.edges[op_rep, sink_idx]['cardinality']
                        self.add_parent(child_idx, sink_idx, resources)
                        self.df_nova.at[sink_idx, "level"] = 2
                        w = euclidean_distance(self.coord_dict[child_idx], self.coord_dict[sink_idx])
                        self.replication_plan.edges[op_rep, sink_idx]["distance"] = w

        elif value_dict['type'] not in ['SOURCE', 'WORKER']:
            raise RuntimeError(f'Attribute type={value_dict["type"]} not supported for insertion')
        df_out = self.df_nova[self.out_cols]
        df_out = df_out.rename(columns={self.capacity_col: "total_capacity"})
        return df_out, self.replication_plan, self.relaxed_positions, self.placed_positions

    def remove_parents(self, node_ids):
        for node_id in node_ids:
            parents = self.df_nova.at[node_id, self.parent_col]
            print("Removing parents", node_id, "->", parents)
            for parent_idx, used_weight in parents:
                self.remove_parent(node_id, parent_idx)
        return self.df_nova

    def remove_parent(self, node_id, parent_id, weight=None):
        print("Removing parent for", node_id, "->", parent_id, weight)
        parents = self.df_nova.at[node_id, self.parent_col]
        new_parents = []
        unbalanced_load = self.df_nova.at[node_id, self.unbalanced_col]

        # update parent capacities
        cnt = 0
        for parent_idx, used_weight in parents:
            if parent_id == parent_idx and weight is None:
                unbalanced_load += used_weight
                self.remove_parent_update_capacities(node_id, parent_id, used_weight)
            elif parent_id == parent_idx and weight == used_weight and cnt == 0:
                unbalanced_load += used_weight
                self.remove_parent_update_capacities(node_id, parent_id, used_weight)
                cnt = 1
            else:
                new_parents.append((parent_idx, used_weight))

        # empty parents
        self.df_nova.at[node_id, self.parent_col] = new_parents
        # assign new unbalanced load
        self.df_nova.at[node_id, self.unbalanced_col] = unbalanced_load

        unbalanced_load = self.df_nova.at[node_id, self.unbalanced_col]
        assert unbalanced_load > 0, f'Negative load reached for node {node_id}: {unbalanced_load}'

        return unbalanced_load, self.df_nova

    def remove_parent_update_capacities(self, node_id, parent_id, used_weight):
        # print(f"Removing: Node={node_id}, parent={parent_id}, weight={used_weight}, "
        #      f"current parents={self.df_nova.at[node_id, self.parent_col]}, "
        #      f"children of parent={self.children_dict[parent_id]}")
        self.df_nova.at[parent_id, self.av_col] += used_weight

        if node_id in self.children_dict[parent_id]:
            self.children_dict[parent_id].remove(node_id)
        else:
            print(f"Warning: Node {node_id} not in children of parent {parent_id}")
        if len(self.children_dict[parent_id]) == 0:
            self.df_nova.at[parent_id, 'level'] = 0
            del self.children_dict[parent_id]

    def add_parent(self, child_idx, parent_idx, mapped_resources):
        # print("Adding child", child_idx, "->", parent_idx, "resources:", mapped_resources)
        if mapped_resources == 0:
            print(f'Error adding child={child_idx} to parent={parent_idx}. Assigned resources={mapped_resources}')
            return

        self.children_dict[parent_idx].add(child_idx)

        # update parent resources
        av_resources = self.df_nova.at[parent_idx, self.av_col]
        if self.df_nova.at[parent_idx, self.av_col] < mapped_resources:
            print(
                f"Warning: Overloading parent={parent_idx} with child {child_idx}, resources={mapped_resources}/{av_resources}")
            self.loaded_nodes += 1

        self.df_nova.at[parent_idx, self.av_col] -= mapped_resources

        # update balance at added node
        self.df_nova.at[child_idx, self.unbalanced_col] -= mapped_resources
        # update routes of the added node
        self.df_nova.at[child_idx, self.parent_col] = self.df_nova.at[child_idx, self.parent_col] + [
            (parent_idx, mapped_resources)]

    def expand_df(self, slot_col):
        rows = []
        max_idx = self.df_nova['level'].idxmax()
        if max_idx > 0:
            self.df_nova.at[0, 'level'] = self.df_nova.at[max_idx, 'level'] + 1

        # Iterate over rows in the DataFrame
        for idx, row in self.df_nova.iterrows():
            if not row['parent']:
                # handle the sinks
                new_row = {'oindex': row['oindex'], 'x': row['x'], 'y': row['y'], 'type': row['type'],
                           'total_weight': pd.NA, 'unbalanced_weight': pd.NA, 'used_weight': pd.NA,
                           'total_capacity': row[slot_col],
                           'free_capacity': row[self.av_col], 'level': row['level'], 'parent': pd.NA}
                rows.append(new_row)
            else:
                for item in row['parent']:
                    children = self.children_dict[item[0]]
                    if idx not in children:
                        raise RuntimeError(f'{idx} not in children of {item[0]}')

                    # Create a new row with 'type', 'tuple_element_1', 'tuple_element_2', and 'prev_index' columns
                    new_row = {'oindex': row['oindex'], 'x': row['x'], 'y': row['y'], 'type': row['type'],
                               'total_weight': row[self.weight_col], 'unbalanced_weight': row[self.unbalanced_col],
                               'used_weight': item[1], 'total_capacity': row[slot_col],
                               'free_capacity': row[self.av_col], 'level': row['level'], 'parent': item[0]}
                    rows.append(new_row)

        # Create a new DataFrame from the list of rows
        out = pd.DataFrame(rows)
        return out
