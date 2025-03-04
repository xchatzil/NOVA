# NOVA
Code for **Nova: Operator Placement for Parallel Stream Join Processing in Heterogeneous Geo-Distributed Environments**

The repository contains two projects:
1. notebooks: Jupyter notebooks of the local experiments and plots of all evaluation results
2. nebulastream: A fork of NebulaStream with the NOVA integration

## Python Ιncludes 
- Python implementations of the following approaches:
    - NOVA
- Jupyter notebooks with evaluations of the implemented approaches for the following topologies:
    - ASimulations.ipynb
        - Experiments on artificial topologies of different sizes
    - FitLab.ipynb
        - Experiments on the FIT IoT Lab
    - King.ipynb
        - Experiments on the King dataset
    - Planetlab.ipynb
        - Experiments on the PlanetLab dataset
    - RIPEAtlas.ipynb
        - Experiments on the RIPE Atlas dataset
- Jupyter notebooks with additional plots of conducted experiments:
    - Example: Contain the plots for the example in the paper
    - Heatmaps: Contain heatmaps about latency statistics of the implemented approach for the tested topologies
    - ScalabilityTestsJoin: Contain the scalability experiments and plots

## Datasets
All datasets of the topologies and results of our experiments can be found in notebooks/datasets

## NebulaStream
NOVA is currently part of the master branch of NebulaStream: https://nebula.stream/

We provide in this repository a fork of NebulaStream with our integration of NOVA. A locally runnable test can be found in nebulastream/nes-coordinator/tests/E2e/E2ENemoJoinTest.cpp

For a detailed guide how to run and deploy NebulaStream, we refer to: https://docs.nebula.stream/
