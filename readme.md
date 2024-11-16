CS 6400 project group 13

dataset: https://www.yelp.com/dataset/download

setup and benchmark for mysql (innodb):
```
tar -xvf yelp_dataset.tar
python bulkload.py
mysql -u %your_username -p
\T /path/to/logfile.txt
\timing
SOURCE mysql_bulkload.sql;
\T
```