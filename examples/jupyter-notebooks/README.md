# Hazelcast Python Client Jupyter Notebooks

This folder contains the IPython notebook tutorials for Hazelcast Python Client. See the details below for 
each of the files.

## 1) Hazelcast SQL Support with Hazelcast Viridian Notebook

This notebook demonstrates how to use SQL functions with Hazelcast Python Client. 
Hazelcast provides in-depth SQL support for Map data structures kept in Hazelcast Clusters.
Using Hazelcast SQL, you can create mappings between your data and a Map and execute SQL queries against the Map. 
This support provides fast in-memory computing using SQL without writing complex functions that iterate through your 
maps. To run this notebook file:

- Use Google Colaboratory environment. It will install all the packages, including Hazelcast Client,
into a virtual environment,  and you will not have to do any local installation. It requires connecting to Hazelcast 
Viridian for the Hazelcast instance, so you do not have to run a local cluster. Open the following link and create 
a copy for yourself. You can start to run cells once connected to a virtual machine.
https://colab.research.google.com/drive/1ujUt_XJI2moWSWMcF5_MPiWPg4LCJuot?usp=sharing


- You need to have Jupyter Notebook on your machine to run this notebook on your local. If you already have, 
download the `Hazelcast Python Client SQL Support with Viridian.ipyb` file from this folder and open 
it from Jupyter dashboard.