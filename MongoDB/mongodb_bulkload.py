import json
import logging
import time
from typing import Dict, Any

import pymongo
from pymongo import InsertOne


class MongoDBLoader:
    def __init__(self, db_uri: str, db_name: str, batch_size: int = 1000):
        self.client = pymongo.MongoClient(db_uri)
        self.db = self.client[db_name]
        self.batch_size = batch_size
        self.setup_logging()

    def setup_logging(self):
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(message)s',
            handlers=[
                logging.FileHandler('mongodb_loader.log'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)

    def load_json_in_batches(self, file_path: str, collection_name: str) -> Dict[str, Any]:
        start_time = time.time()
        collection = self.db[collection_name]
        doc_count = 0

        try:
            batch = []
            with open(file_path, 'r') as file:
                for line in file:
                    try:
                        document = json.loads(line.strip())
                        batch.append(InsertOne(document))
                        doc_count += 1

                        if len(batch) >= self.batch_size:
                            collection.bulk_write(batch, ordered=False)
                            current_time = time.time() - start_time
                            batch = []

                    except json.JSONDecodeError as e:
                        self.logger.error(f"Error decoding JSON: {e}")
                        continue
                if batch:
                    collection.bulk_write(batch, ordered=False)
                    final_time = time.time() - start_time
                    self.logger.info(
                        f"{collection_name}: Completed {doc_count:,} documents in {final_time:.2f} seconds " +
                        f"({doc_count / final_time:.2f} documents/second)")

        except Exception as e:
            self.logger.error(f"Error during file processing: {e}")
            raise

        return {
            'collection': collection_name,
            'total_documents': doc_count,
            'total_time': time.time() - start_time
        }


def main():
    MONGODB_URI = "mongodb://localhost:27017/"
    DB_NAME = "yelp_db"
    BATCH_SIZE = 1000
    loader = MongoDBLoader(MONGODB_URI, DB_NAME, BATCH_SIZE)

    files = {
        'business': '/Users/puxuanwang/Downloads/yelp_dataset/reduced_businesses.json',
        'tip': '/Users/puxuanwang/Downloads/yelp_dataset/reduced_tips.json',
        'user': '/Users/puxuanwang/Downloads/yelp_dataset/reduced_users.json'
    }

    for collection_name, file_path in files.items():
        try:
            metrics = loader.load_json_in_batches(file_path, collection_name)
            print(f"\nSummary for {collection_name}:")
            print(f"Total documents: {metrics['total_documents']:,}")
            print(f"Total time: {metrics['total_time']:.2f} seconds")
            print(f"Average speed: {metrics['total_documents'] / metrics['total_time']:.2f} documents/second")
            print("-" * 50)
        except Exception as e:
            print(f"Failed to process {file_path}: {e}")


if __name__ == "__main__":
    main()

# 2024-11-16 20:45:57,139 - business: Completed 150,346 documents in 3.41 seconds (44038.31 documents/second)
#
# Summary for business:
#     Total documents: 150,346
# Total time: 3.41 seconds
# Average speed: 44035.50 documents/second
# --------------------------------------------------
#
# Summary for review:
#     Total documents: 6,990,280
# Total time: 114.18 seconds
# Average speed: 61224.09 documents/second
# --------------------------------------------------
# 2024-11-16 20:47:51,313 - review: Completed 6,990,280 documents in 114.17 seconds (61224.87 documents/second)
#
# Summary for user:
#     Total documents: 1,987,897
# Total time: 57.18 seconds
# Average speed: 34767.24 documents/second
# --------------------------------------------------
# 2024-11-16 20:48:48,492 - user: Completed 1,987,897 documents in 57.18 seconds (34767.50 documents/second)
