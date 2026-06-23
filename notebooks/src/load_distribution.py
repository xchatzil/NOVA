import random
import numpy as np
import time


def greedy_assignment_batched(workers, batches):
    # Initialize assignments for each worker
    assignments = dict()
    assignments_unique = dict()
    worker_ids = list(workers.keys())
    worker_capacities = dict()
    unassigned = []
    overloaded_workers = set()

    def assign_to_worker(worker_id, batch_idx, batch, force_assign):
        worker_capacity = workers[worker_id]
        if worker_id not in assignments:
            assignments[worker_id] = []
        if worker_id not in assignments_unique:
            assignments_unique[worker_id] = (set(), set())
        start_A, end_A = int(batch["A_range"][0]), int(batch["A_range"][1])
        start_B, end_B = int(batch["B_range"][0]), int(batch["B_range"][1])

        # update the two sets containing the unique tuples
        uniques_A = assignments_unique[worker_id][0]
        uniques_B = assignments_unique[worker_id][1]

        if not force_assign:
            # copy the uniques to rollback in case of lacking resources
            uniques_A = uniques_A.copy()
            uniques_B = uniques_B.copy()

        uniques_A.update(range(start_A, end_A + 1))
        uniques_B.update(range(start_B, end_B + 1))
        contained_tuples = len(uniques_A) + len(uniques_B)

        if (contained_tuples > worker_capacity) and (force_assign == False):
            # worker has not enough resources
            return False
        else:
            # Assign batch to this worker
            assignments_unique[worker_id] = (uniques_A, uniques_B)
            assignments[worker_id].append(batch_idx)
            worker_capacities[worker_id] = worker_capacity - contained_tuples
            return True

    # Assign tuples to workers
    worker_idx = 0
    batch_idx = 0
    while batch_idx < len(batches):
        batch = batches[batch_idx]
        if len(worker_ids) < worker_idx + 1:
            # no more resources available
            worker_id = worker_ids[batch_idx % worker_idx]
            unassigned.append(batch_idx)
            overloaded_workers.add(worker_id)
            assign_to_worker(worker_id, batch_idx, batch, True)
            batch_idx += 1
        else:
            worker_id = worker_ids[worker_idx]
            if assign_to_worker(worker_id, batch_idx, batch, False):
                # worker is overloaded, move to the next one
                batch_idx += 1
            else:
                # worker has enough available resources
                worker_idx += 1

    if unassigned:
        print(
            f"Missing resources during load distribution. Remaining batches={len(unassigned)} assigned to workers={len(overloaded_workers)}")

    return assignments, assignments_unique


def random_assignment_batched(workers, batches, unique_elements=None):
    uniques = None
    assignment = {worker_id: [] for worker_id in workers.keys()}
    for batch_id, batch in enumerate(batches):
        assignment[str(np.random.randint(1, len(workers) + 1))].append(batch_id)
    return assignment, uniques


def gradient_descent_assignment_unique_batched(workers, batches, unique_elements, learning_rate=0.01, max_iters=100,
                                               tol=1e-6):
    num_batches = len(batches)
    num_workers = len(workers)
    worker_ids = list(workers.keys())

    # Initialize x randomly, ensuring normalization
    x = np.random.rand(num_batches, num_workers)
    x = x / x.sum(axis=1, keepdims=True)

    # Get worker capacities
    capacities = np.array(list(workers.values()))

    # best k
    best_x = None
    best_cost = float('inf')

    best_k = 1
    cost = float('inf')
    for iteration in range(max_iters):
        # Compute loads for each worker
        loads = x.sum(axis=0)

        # Get unique assignment count for each worker
        assignments = continuous_to_binary_batched(x, worker_ids)
        overloaded_nodes, total_assignments, unique_assignments, assigned_workers, load_factors, unique_dict \
            = eval_overloaded_nodes_batched(workers, assignments, batches)
        unique_assignments_idx = np.array(list(unique_dict.values()))

        # Compute penalties for workers exceeding capacity
        penalties = np.maximum(0, loads - capacities)

        # Compute gradients
        gradients = np.zeros_like(x)
        for t in range(num_batches):
            for w in range(num_workers):
                if unique_assignments_idx[w] > capacities[w]:
                    gradients[t, w] = 2 * penalties[w] + unique_assignments_idx[w]

        # Update x using gradients
        x -= learning_rate * gradients

        # Add random perturbations periodically
        if iteration % 20 == 0:
            perturbation = np.random.uniform(-0.01, 0.01, x.shape)
            x += perturbation

        # Normalize x to ensure each row sums to 1
        x = np.maximum(x, 0)  # Ensure non-negativity
        # Avoid division by zero by checking row sums
        row_sums = x.sum(axis=1, keepdims=True)
        row_sums[row_sums == 0] = 1  # Replace zeros with 1 to avoid division by zero
        x = x / row_sums

        # Get the indexes of the k highest values
        k = max(1, min(num_workers, best_k + np.random.randint(-2, 3)))
        indices = np.argsort(loads)[-k:][::-1]
        # Create a mask for the columns
        mask = np.zeros(x.shape[1], dtype=bool)
        mask[indices] = True

        # Set values in all other columns to 0
        x[:, ~mask] = 0

        # Check for convergence
        current_cost = (unique_assignments // unique_elements) * (1 + overloaded_nodes)
        # print(unique_assignments, unique_elements, overloaded_nodes, current_cost)
        if current_cost < best_cost:
            best_k = k
            best_x = x
            best_cost = current_cost

        if cost < tol:
            break
        cost = current_cost

    print("Best cost=", best_cost)
    return best_x


def continuous_to_binary_batched(result_matrix, worker_ids):
    assignments = {worker_id: [] for worker_id in worker_ids}

    # Convert continuous to binary assignments
    for batch_idx, assignment in enumerate(result_matrix):
        # Assign the tuple (a, b) to the worker with the highest proportion
        best_worker_idx = np.argmax(assignment)
        best_worker_id = worker_ids[best_worker_idx]
        assignments[best_worker_id].append(batch_idx)
    return assignments


def gradient_descent_assignment_batched(workers, batches, unique_elements, learning_rate=0.1, max_iters=1000,
                                        tol=1e-10):
    worker_ids = list(workers.keys())
    result_matrix = gradient_descent_assignment_unique_batched(workers, batches, unique_elements,
                                                               learning_rate, max_iters, tol)
    assignments = continuous_to_binary_batched(result_matrix, worker_ids)
    return assignments


def eval_overloaded_nodes_batched(workers, batched_assignments, batches):
    overloaded_nodes = 0
    total_assignments = 0
    unique_assignments = 0
    assigned_workers = 0
    unique_dict = dict()
    load_factors = list()

    for worker_id, worker_capacity in workers.items():
        # Use a set to store unique elements
        uniques_A = set()
        uniques_B = set()
        if worker_id in batched_assignments and batched_assignments[worker_id]:
            assigned_workers += 1
            for batch_id in batched_assignments[worker_id]:
                batch = batches[batch_id]
                start_A, end_A = int(batch["A_range"][0]), int(batch["A_range"][1])
                start_B, end_B = int(batch["B_range"][0]), int(batch["B_range"][1])
                uniques_A.update(list(range(start_A, end_A + 1)))
                uniques_B.update(list(range(start_B, end_B + 1)))

        assigned_load = len(uniques_A) + len(uniques_B)
        if assigned_load > worker_capacity:
            overloaded_nodes += 1
        total_assignments += len(uniques_A) * len(uniques_B)
        unique_assignments += assigned_load
        unique_dict[worker_id] = (len(uniques_A), len(uniques_B))
        load_factors.append(assigned_load / max(1, worker_capacity))
    return overloaded_nodes, total_assignments, unique_assignments, assigned_workers, load_factors, unique_dict


def evaluate_approach_batched(workers, batches, approach):
    start_time = time.time()
    assignment, uniques = approach(workers, batches)
    end_time = time.time()
    runtime = end_time - start_time
    overloaded_nodes, total_assignments, unique_assignments, assigned_workers, load_factors, unique_dict = (
        eval_overloaded_nodes_batched(workers, assignment, batches))

    return {
        "approach": approach.__name__,
        "runtime": runtime,
        "overloaded_nodes": overloaded_nodes,
        "total_assignments": total_assignments,
        "unique_assignments": unique_assignments,
        "assigned_workers": assigned_workers,
        "load_factors(mean)": np.mean(load_factors),
        "load_factors(median)": np.median(load_factors)
    }


def create_workers(num_workers, total_capacity):
    """
    Creates a list of workers with random capacities that sum to the total capacity.

    Args:
        num_workers (int): Number of workers to create.
        total_capacity (int): Total capacity to distribute among workers.

    Returns:
        list: A list of dictionaries, each representing a worker with an ID, capacity, and initial load of 0.
    """
    if num_workers <= 0 or total_capacity <= 0:
        raise ValueError("Number of workers and total capacity must be greater than 0.")

    # Generate random capacities that sum to total_capacity
    capacities = [random.randint(1, total_capacity) for _ in range(num_workers - 1)]
    capacities.append(total_capacity)  # Include total_capacity to ensure distribution
    capacities.sort()
    capacities = [capacities[i] - capacities[i - 1] if i > 0 else capacities[i] for i in range(len(capacities))]

    # Create the workers list
    workers = {str(i + 1): capacities[i] for i in range(num_workers)}

    return workers
