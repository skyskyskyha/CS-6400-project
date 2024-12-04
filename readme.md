#CS 6400 project group 13




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
- `GenerateDataset.py`: Script to generate datasets for Neo4j.
- `Neo4jER.txt`: Description of the entity-relationship model for Neo4j.
- `neo4j.ipynb`: Jupyter Notebook for Neo4j operations.
- `neo4jUtils.py`: Utility functions for Neo4j operations.


