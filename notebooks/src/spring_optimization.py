import numpy as np
from concurrent.futures import ProcessPoolExecutor
from itertools import chain


def compute_edge_gradient(var_point_i, point_j, k_ij):
    """
    Compute the gradient (force) acting on a variable point and a connected point due to a spring between them.
    Now the spring constant `k_ij` is specific to the edge connecting points i and j.
    """
    displacement = var_point_i - point_j
    distance_squared = np.dot(displacement, displacement)

    if distance_squared > 0:
        distance = np.sqrt(distance_squared)
        gradient = -k_ij * displacement / distance  # Use edge-specific spring constant `k_ij`
    else:
        gradient = np.zeros(var_point_i.shape)

    return gradient


# Spring relaxation implementation with edge weights
def spring_relaxation_single(nodes, fixed_ids, edges, tolerance=0.1, max_iters=200):
    variable_points = {node_id: np.array(coord, dtype=np.float64) for node_id, coord in nodes.items() if
                       node_id not in fixed_ids}
    fixed_points = {node_id: np.array(coord, dtype=np.float64) for node_id, coord in nodes.items() if
                    node_id in fixed_ids}
    var_ids = list(variable_points.keys())
    var_coords = np.array(list(variable_points.values()))

    variable_edges = []
    fixed_edges = []

    for i, j, weight in edges:
        if i in variable_points and j in variable_points:
            variable_edges.append((var_ids.index(i), var_ids.index(j), weight))
        elif i in variable_points and j in fixed_points:
            fixed_edges.append((var_ids.index(i), fixed_points[j], weight))
        elif j in variable_points and i in fixed_points:
            fixed_edges.append((var_ids.index(j), fixed_points[i], weight))

    iteration = 0
    while iteration < max_iters:
        max_force = 0
        displacement_coords = np.zeros_like(var_coords, dtype=np.float64)

        for (i, j, weight) in variable_edges:
            grad_i = compute_edge_gradient(var_coords[i], var_coords[j], weight)
            grad_j = compute_edge_gradient(var_coords[j], var_coords[i], weight)
            displacement_coords[i] += grad_i
            displacement_coords[j] += grad_j
            max_force = max(max_force, np.linalg.norm(grad_i), np.linalg.norm(grad_j))

        for (i_var, j_fixed, weight) in fixed_edges:
            grad_i = compute_edge_gradient(var_coords[i_var], j_fixed, weight)
            displacement_coords[i_var] += grad_i
            max_force = max(max_force, np.linalg.norm(grad_i))

        var_coords += displacement_coords
        iteration += 1
        if max_force < tolerance:
            break

    relaxed_positions = {var_ids[i]: var_coords[i] for i in range(len(var_ids))}
    relaxed_positions.update({k: v.tolist() for k, v in fixed_points.items()})
    return relaxed_positions


def compute_gradients_for_edges(edges, var_coords):
    """
    Compute the displacement updates for a subset of edges.
    This function no longer uses `fixed_points` since the edges already contain
    the necessary fixed-point information where applicable.
    """
    displacement_coords = np.zeros_like(var_coords)
    max_force = 0

    for (i, j, weight) in edges:
        if isinstance(j, np.ndarray):  # This is a variable-fixed edge
            grad_i = compute_edge_gradient(var_coords[i], j, weight)
            displacement_coords[i] += grad_i
            max_force = max(max_force, np.linalg.norm(grad_i))
        else:  # This is a variable-variable edge
            grad_i = compute_edge_gradient(var_coords[i], var_coords[j], weight)
            grad_j = compute_edge_gradient(var_coords[j], var_coords[i], weight)
            displacement_coords[i] += grad_i
            displacement_coords[j] += grad_j
            max_force = max(max_force, np.linalg.norm(grad_i), np.linalg.norm(grad_j))

    return displacement_coords, max_force


def spring_relaxation_parallel(nodes, fixed_ids, edges, tolerance, max_iters=1000, num_workers=1):
    """
    Parallelized spring relaxation method to adjust the positions of variable points
    based on a graph structure with fixed points.
    """
    # Step 1: Extract variable and fixed points from the `nodes` dictionary
    variable_points = {node_id: np.array(coord, dtype=np.float64) for node_id, coord in nodes.items() if
                       node_id not in fixed_ids}
    fixed_points = {node_id: np.array(coord, dtype=np.float64) for node_id, coord in nodes.items() if
                    node_id in fixed_ids}
    var_ids = list(variable_points.keys())
    var_coords = np.array(list(variable_points.values()))

    # Step 2: Create two types of edges: variable-variable and variable-fixed
    variable_edges = []
    fixed_edges = []

    for i, j, weight in edges:
        if i in variable_points and j in variable_points:
            variable_edges.append((var_ids.index(i), var_ids.index(j), weight))
        elif i in variable_points and j in fixed_points:
            fixed_edges.append((var_ids.index(i), fixed_points[j], weight))
        elif j in variable_points and i in fixed_points:
            fixed_edges.append((var_ids.index(j), fixed_points[i], weight))

    all_edges = variable_edges + fixed_edges

    iteration = 0
    while iteration < max_iters:
        max_force = 0
        displacement_coords = np.zeros_like(var_coords)

        # Split edges into chunks for each worker
        chunk_size = len(all_edges) // num_workers
        chunks = [all_edges[i:i + chunk_size] for i in range(0, len(all_edges), chunk_size)]

        # Step 3: Parallel computation of gradients using ThreadPoolExecutor
        with ProcessPoolExecutor(max_workers=num_workers) as executor:
            futures = [executor.submit(compute_gradients_for_edges, chunk, var_coords) for chunk in chunks]

            for future in futures:
                displacements, local_max_force = future.result()
                displacement_coords += displacements
                max_force = max(max_force, local_max_force)

        # Apply the accumulated displacements
        var_coords += displacement_coords

        iteration += 1

        # Stop if maximum force is below tolerance
        if max_force < tolerance:
            break

    # Step 4: Return the relaxed positions of all nodes (both fixed and variable points)
    relaxed_positions = {var_ids[i]: var_coords[i] for i in range(len(var_ids))}
    relaxed_positions.update({k: v.tolist() for k, v in fixed_points.items()})

    return relaxed_positions


def gradient_descent_with_momentum(nodes, non_fixed_ids, edges, k=0.5, learning_rate=0.01, momentum=0.8, tolerance=1e-5,
                                   max_iters=100, use_weight=False):
    # Initialize coordinates and velocity
    coordinates = dict()
    velocity = dict()
    for node_id, coord in nodes.items():
        coordinates[node_id] = [float(coord[0]), float(coord[1])]
        velocity[node_id] = [0.0, 0.0]

    for iteration in range(max_iters):
        max_change = 0

        # Iterate only over non-fixed nodes
        for node_id in non_fixed_ids:
            net_force = [0.0, 0.0]

            # Calculate forces from all connected edges
            incoming_edges = edges.in_edges(node_id, data=True)
            outgoing_edges = edges.out_edges(node_id, data=True)
            for A, B, data in chain(incoming_edges, outgoing_edges):
                if use_weight:
                    weight = data['cardinality']
                else:
                    weight = 1
                    # if B == 0:
                    #    weight = 1

                if A == node_id:
                    neighbor_id = B
                else:
                    neighbor_id = A

                direction = [coordinates[neighbor_id][i] - coordinates[node_id][i] for i in range(2)]
                distance = (direction[0] ** 2 + direction[1] ** 2) ** 0.5
                if distance == 0:
                    distance = 1e-10  # Avoid division by zero
                direction = [d / distance for d in direction]
                force_magnitude = k * weight * (distance - 1)
                net_force = [net_force[i] + force_magnitude * direction[i] for i in range(2)]

            # Update velocity and position using momentum
            velocity[node_id] = [momentum * velocity[node_id][i] + learning_rate * net_force[i] for i in range(2)]
            coordinates[node_id] = [coordinates[node_id][i] + velocity[node_id][i] for i in range(2)]
            max_change = max(max_change, (velocity[node_id][0] ** 2 + velocity[node_id][1] ** 2) ** 0.5)

        # Check for convergence
        if max_change < tolerance:
            break

    return coordinates
