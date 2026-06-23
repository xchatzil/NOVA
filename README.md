# Nova: Scalable Streaming Join Placement and Parallelization in Resource-Constrained Geo-Distributed Environments

Nova is an optimization approach for the **Operator Placement and Replication (OPR)** problem for **join operators** in geo-distributed stream processing environments.
It targets IoT and edge settings with heterogeneous, resource-constrained nodes, where applications require efficient regional stream joins near data sources while minimizing latency and avoiding node overload.

Nova embeds a discrete topology into a Euclidean cost space, resolves join operators in the continuous space using gradient descent, decomposes joins into bandwidth-aware sub-joins, and assigns replicas to physical nodes in a resource-aware manner.
Nova builds on [NEMO](https://www.vldb.org/pvldb/vol17/p1501-chatziliadis.pdf), extending its cost-space placement approach from decomposable aggregation functions to join operators.
The approach supports efficient re-optimizations under topology changes and scales to large geo-distributed topologies.

### Approach

Nova operates in the following phases:

1. **Cost space construction** — The topology is projected into a Euclidean network coordinate space (NCS), where distances approximate communication cost (e.g., latency).
2. **Virtual join placement** — Join operators are placed in the cost space using radient descent to approximate optimal virtual locations.
3. **Join decomposition and stream partitioning** — Joins are decomposed into sub-joins with bandwidth-aware batching to respect node capacities and link constraints.
4. **Replication and physical assignment** — Virtual placements are mapped to physical nodes via k-nearest-neighbor search and greedy load distribution, creating replicas as needed.
5. **Re-optimization** — Partial re-placements adapt to node additions, removals, and capacity changes without recomputing the full placement.

### Repository Structure

This repository contains the Python simulation code and Jupyter notebooks used to evaluate Nova in the EDBT 2026 paper.

| Component | Description |
|-----------|-------------|
| [notebooks](notebooks) | Jupyter notebooks for experiments, plots, and evaluation results across real-world and artificial topologies. |
| [notebooks/src/nova_join.py](notebooks/src/nova_join.py) | Core Nova join placement solver (`NovaSolver`). |
| [notebooks/src/baselines.py](notebooks/src/baselines.py) | Baseline placement strategies for join queries. |
| [notebooks/src/leachSFtree.py](notebooks/src/leachSFtree.py) | LEACH-SF and LEACH-SF tree baseline implementations for joins. |
| [notebooks/src/mst_prim.py](notebooks/src/mst_prim.py) | MST (Prim-based) baseline for tree routing. |
| [notebooks/src/spring_optimization.py](notebooks/src/spring_optimization.py) | Spring relaxation and gradient descent for virtual operator placement. |
| [notebooks/src/load_distribution.py](notebooks/src/load_distribution.py) | Greedy load assignment and overload evaluation. |
| [notebooks/src/cardinalities.py](notebooks/src/cardinalities.py) | Join cardinality estimation utilities. |
| [notebooks/src/topology.py](notebooks/src/topology.py) | Topology generation and preprocessing utilities. |
| [notebooks/src/util.py](notebooks/src/util.py) | Shared evaluation, plotting, and decomposition utilities. |
| [notebooks/src/eval_tools.py](notebooks/src/eval_tools.py) | Evaluation helpers for latency and load statistics. |
| [notebooks/datasets](notebooks/datasets) | Latency measurements and NCS coordinates for evaluated topologies. |
| [notebooks/plots](notebooks/plots) | Generated figures from the paper evaluation. |

### Baselines

The simulation code includes Python implementations of the following join placement approaches:

- **Nova**
- **Sink-based** — places all join processing at the sink
- **Source-based** — pushes join processing toward sources
- **Top-C** — assigns joins to the highest-capacity nodes
- **LEACH-SF** and **LEACH-SF tree**
- **MST** (minimum spanning tree)

### Notebooks

| Notebook | Description |
|----------|-------------|
| [ASimulations_A.ipynb](notebooks/ASimulations_A.ipynb) | Artificial topology experiments (part A). |
| [ASimulations_B.ipynb](notebooks/ASimulations_B.ipynb) | Artificial topology experiments (part B). |
| [ASimulations_FitLab.ipynb](notebooks/ASimulations_FitLab.ipynb) | Evaluation on the FIT IoT Lab topology. |
| [ASimulations_King.ipynb](notebooks/ASimulations_King.ipynb) | Evaluation on the King DNS server topology. |
| [ASimulations_PlanetLab.ipynb](notebooks/ASimulations_PlanetLab.ipynb) | Evaluation on the PlanetLab topology. |
| [ASimulations_RipeAtlas.ipynb](notebooks/ASimulations_RipeAtlas.ipynb) | Evaluation on the RIPE Atlas topology. |
| [Example.ipynb](notebooks/Example.ipynb) | Step-by-step walkthrough of Nova on a small example topology. |
| [Heatmaps.ipynb](notebooks/Heatmaps.ipynb) | Latency heatmaps across approaches and topologies. |
| [ScalabilityTestsJoin.ipynb](notebooks/ScalabilityTestsJoin.ipynb) | Scalability experiments for join placement. |
| [ChangingTopology_RipeAtlas.ipynb](notebooks/ChangingTopology_RipeAtlas.ipynb) | Robustness experiments under topology changes on RIPE Atlas. |
| [Eval_latency.ipynb](notebooks/Eval_latency.ipynb) | End-to-end latency evaluation. |
| [Eval_perf.ipynb](notebooks/Eval_perf.ipynb) | End-to-end throughput and performance evaluation. |

### Running Simulations

Install the required Python packages and run the notebooks from the `notebooks` directory:

```sh
pip install numpy pandas scipy scikit-learn matplotlib seaborn scikit-fuzzy networkx annoy jupyter
cd notebooks
jupyter notebook
```

### Datasets

Topology latency measurements and network coordinate files are available in [notebooks/datasets](notebooks/datasets):

- **FIT** — FIT IoT Lab coordinates
- **atlas** — RIPE Atlas RTT matrices and coordinates
- **planetlab.txt**, **PL_coords.txt**, **PL_labels.csv** — PlanetLab data
- **vivaldi_king.txt** — King DNS server topology

### End-to-End Experiments

The end-to-end experiments in the paper were conducted with [NebulaStream](https://github.com/nebulastream/nebulastream).

### Publication

This repository accompanies the following publication:

```BibTeX
@inproceedings{DBLP:conf/edbt/ChatziliadisZAE26,
  author       = {Xenofon Chatziliadis and
                  Eleni Tzirita Zacharatou and
                  Samira Akili and
                  Alphan Eracar and
                  Volker Markl},
  editor       = {Wolfgang Lehner and
                  Vanessa Braganholo and
                  Kostas Stefanidis and
                  Zheying Zhang and
                  Alexander Krause and
                  Jo{\~{a}}o Felipe Nicolaci Pimentel},
  title        = {Nova: Scalable Streaming Join Placement and Parallelization in Resource-Constrained
                  Geo-Distributed Environments},
  booktitle    = {Proceedings 29th International Conference on Extending Database Technology,
                  {EDBT} 2026, Tampere, Finland, March 24-27, 2026},
  pages        = {433--446},
  publisher    = {OpenProceedings.org},
  year         = {2026},
  url          = {https://doi.org/10.48786/edbt.2026.35},
  doi          = {10.48786/EDBT.2026.35},
  biburl       = {https://dblp.org/rec/conf/edbt/ChatziliadisZAE26.bib},
  bibsource    = {dblp computer science bibliography, https://dblp.org}
}
```

### Related Work

The following work is closely related to Nova and influenced its design:

* [Chatziliadis et al.](https://www.vldb.org/pvldb/vol17/p1501-chatziliadis.pdf): Efficient Placement of Decomposable Aggregation Functions for Stream Processing over Large Geo-Distributed Topologies (VLDB 2024) — introduces NEMO, the precursor work on cost-space operator placement that Nova extends to joins.
* [Pietzuch et al.](https://doi.org/10.1109/ICDE.2006.105): Network-Aware Operator Placement for Stream-Processing Systems (ICDE 2006).
* [Rizou et al.](https://doi.org/10.1109/ICCCN.2010.5560127): Solving the Multi-Operator Placement Problem in Large-Scale Operator Networks (ICCCN 2010).
* [Dabek et al.](https://doi.org/10.1145/1015467.1015471): Vivaldi: A decentralized network coordinate system (SIGCOMM 2004).
* [Zeuch et al.](https://www.cidrdb.org/cidr2020/papers/p7-zeuch-cidr20.pdf): The NebulaStream Platform for Data and Application Management in the Internet of Things (CIDR 2020).
