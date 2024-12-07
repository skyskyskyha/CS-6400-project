# CS 6400 project group 13




## Benchmark analysis for yelp dataset with MySQL, Neo4j and MongoDB

### Dataset/
- dataset link: https://www.yelp.com/dataset/download
- `dataset.ipynb`: Jupyter Notebook for dataset processing.
- `datasetUtils.py`: Utility functions for dataset operations.

### MongoDB/
- `mongo_query.py`: Script for executing MongoDB queries.
- `mongodb_bulkload.py`: Script for bulk loading data into MongoDB.

### MySQL/
- `mysql_bulkload.ipynb`: Jupyter Notebook for bulk loading into MySQL.
- `mysql_bulkload.sql`: SQL script for bulk loading into MySQL.

- setup and benchmark for mysql (innodb):
```
tar -xvf yelp_dataset.tar
python bulkload.py
mysql -u %your_username -p
\T /path/to/logfile.txt
\timing
SOURCE mysql_bulkload.sql;
\T
```
### Neo4j/
- `Neo4jER.txt`: Description of the entity-relationship model for Neo4j.
- `neo4j.ipynb`: Jupyter Notebook for Neo4j operations.
- `neo4jUtils.py`: Utility functions for Neo4j operations.
#### How to execute `neo4j.ipynb`
- Download Neo4j Desktop and start a neo4j database with default config (my version is 5.24.0), set your own password
- Go into the jupyter notebook and change the config cell, including the password field to the password set above
- Execute each cell, which contains bulkload and query to view the query time


### Slide
- https://docs.google.com/presentation/d/1Dc_ngkzWde_cgbo4Q4-RZtajtEnEDjD0yA4CKbBmGsc/edit#slide=id.g31a96d1b912_0_0

### Document
- https://docs.google.com/document/d/1NJ4bZ0tosEDycj4DykT6yFxT9cvd_DO3KLIsinYfl2k/edit?tab=t.0
