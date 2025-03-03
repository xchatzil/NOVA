import pandas as pd
import numpy as np
import random

from scipy.sparse import lil_matrix


def generate_random_distributions(values, value_range, seed=None):
    """
    Generate a random distribution for each value in the given list within the specified range.

    Parameters:
    values (list): A list of distinct values for which the distributions will be generated.
    value_range (tuple): A tuple (min_count, max_count) representing the range of random counts.
    seed (int, optional): Seed for reproducibility of random numbers.

    Returns:
    dict: A dictionary where keys are values from the input list and values are random counts.
    """
    # Set the seed for reproducibility, if provided
    if seed is not None:
        np.random.seed(seed)

    min_count, max_count = value_range  # Unpack the range

    # Ensure the range is valid
    if min_count > max_count:
        raise ValueError("min_count cannot be greater than max_count")

    # Use numpy to generate random integers for each value in the list
    random_counts = np.random.randint(min_count, max_count + 1, size=len(values))
    # Create the distribution dictionary
    distribution = {value: count for value, count in zip(values, random_counts)}

    return distribution


def generate_random_integer_dict(ids, total_sum, min_value=0, max_value=10, seed=None):
    if seed is not None:
        np.random.seed(seed)
        random.seed(seed)

    n = len(ids)  # Number of values to generate
    print(f"Generating n={n} random integers between {min_value} and {max_value}, sum={total_sum}")

    # Step 1: Generate n random integers within the given range
    random_values = np.random.randint(min_value, max_value + 1, n)

    # Step 2: Scale the values so their sum matches total_sum
    current_sum = np.sum(random_values)
    difference = total_sum - current_sum

    # Step 3: Adjust the values to make the sum equal to total_sum
    i = 0
    timeout = n ** 4
    while difference != 0 and i < timeout:
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
        i = i + 1

    if i >= timeout:
        raise RuntimeError(f"Unable to create dict for n={n} ({min_value, max_value}) with sum {total_sum}")

    # Step 4: Create a dictionary with IDs as keys and random values as values
    result_dict = {id_: value for id_, value in zip(ids, random_values)}

    return result_dict


def create_unique_n_tuples(elements, n, shuffle=False):
    if shuffle:
        random.shuffle(elements)  # Shuffle the elements to randomize n-tuples
    # Create n-tuples
    n_tuples = [tuple(elements[i:i + n]) for i in range(0, len(elements) - len(elements) % n, n)]
    # Collect unassigned elements
    unassigned = elements[len(elements) - len(elements) % n:]
    return n_tuples, unassigned


def create_adjacency_matrix_from_pairs(size, unique_pairs, output_range, output_sum=None, seed=None, with_df=True):
    if seed:
        random.seed(seed)

    output_cardinalities = None
    if output_sum:
        output_cardinalities = generate_random_integer_dict(list(range(0, len(unique_pairs))), output_sum,
                                                            output_range[0], output_range[1])

    # Create a DataFrame with the index and columns set to the labels
    # df = pd.DataFrame(0, index=labels, columns=labels, dtype=int)
    sparse_matrix = lil_matrix((size, size), dtype=int)

    total = 0
    i = 0
    for left_idx, right_idx in unique_pairs:
        # left_idx -= 1
        # right_idx -= 1
        if output_sum:
            output_card = output_cardinalities[i]
        else:
            output_card = random.randint(*output_range)
        sparse_matrix[left_idx, right_idx] = output_card
        sparse_matrix[right_idx, left_idx] = output_card
        total += output_card
        i += 1

    if with_df:
        df = pd.DataFrame.sparse.from_spmatrix(sparse_matrix)
        return df, sparse_matrix, total
    else:
        return sparse_matrix, total


def create_cardinalities(source_ids, range=(1, 10), seed=None):
    if seed:
        random.seed(seed)

    cardinalities = dict()
    for idx in source_ids:
        cardinalities[idx] = random.randint(*range)
    return cardinalities


def create_random_join_matrix(n, seed=None):
    """
    Creates a random join adjaceny matrix for n tables.
    """

    if seed is not None:
        np.random.seed(seed)

    # Initialize an n x n matrix with zeros
    matrix = np.zeros((n, n), dtype=int)

    # Set a single random position in each row to 1
    for i in range(1, n):
        random_col = np.random.randint(1, n)  # Get a random column index
        while random_col == i:
            random_col = np.random.randint(1, n)  # Get a random column index
        matrix[i, random_col] = 1  # Set the position to 1
        matrix[random_col, i] = 1  # Set the position to 1
    return matrix


def create_adjacency_matrix(distributions):
    """
    Create an adjacency matrix based on key/value distributions of different tables.

    Parameters:
    distributions (dict): A dictionary where the keys are table names and the values are dictionaries representing
                          the key/value distributions for each table.

    Returns:
    pd.DataFrame: A pandas DataFrame representing the adjacency matrix.
    """

    # Get the list of tables
    tables = list(distributions.keys())

    # Initialize an empty adjacency matrix (square matrix with size equal to the number of tables)
    adjacency_matrix = pd.DataFrame(0, index=tables, columns=tables)

    # Compare each pair of tables
    for i, table_a in enumerate(tables):
        for j, table_b in enumerate(tables):
            if i == j:
                # Skip comparing the table with itself
                continue

            # Get the set of keys for each table
            keys_a = set(distributions[table_a].keys())
            keys_b = set(distributions[table_b].keys())

            # Check if the two tables have common keys
            if keys_a & keys_b:  # Intersection of keys
                adjacency_matrix.at[table_a, table_b] = 1
                adjacency_matrix.at[table_b, table_a] = 1  # Symmetric matrix

    return adjacency_matrix


def get_max_cardinality(cardinality_dict):
    """
    Get the maximum value from a nested dictionary (dict of dicts).

    Parameters:
    - nested_dict (dict): A dictionary where values are also dictionaries.

    Returns:
    - max_value: The maximum value found in the nested dictionaries.
    """
    # Use a generator to flatten the inner dictionaries' values and find the max
    return max(
        value
        for inner_dict in cardinality_dict.values()  # Iterate over inner dictionaries
        for value in inner_dict.values()  # Iterate over values in each inner dictionary
    )
