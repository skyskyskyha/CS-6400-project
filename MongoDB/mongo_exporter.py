import json
import logging
import time
import csv
from typing import Dict, Any, List
from pathlib import Path

import pymongo


class MongoDBExporter:
    def __init__(self, db_uri: str, db_name: str):
        self.client = pymongo.MongoClient(db_uri)
        self.db = self.client[db_name]
        self.setup_logging()

    def setup_logging(self):
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(message)s',
            handlers=[
                logging.FileHandler('../mongodb_export.log'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)

    def export_to_csv(self, collection_name: str, output_path: str, batch_size: int = 1000) -> Dict[str, Any]:
        start_time = time.time()
        collection = self.db[collection_name]
        doc_count = 0

        try:
            first_doc = collection.find_one()
            if not first_doc:
                self.logger.error(f"No documents found in collection {collection_name}")
                return
            headers = self._get_flattened_headers(first_doc)

            with open(output_path, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=headers)
                writer.writeheader()
                cursor = collection.find({})
                batch = []

                for document in cursor:
                    flattened_doc = self._flatten_document(document)
                    batch.append(flattened_doc)
                    doc_count += 1

                    if len(batch) >= batch_size:
                        writer.writerows(batch)
                        self.logger.info(f"Processed {doc_count:,} documents...")
                        batch = []
                if batch:
                    writer.writerows(batch)

        except Exception as e:
            self.logger.error(f"Error during export: {e}")
            raise

        final_time = time.time() - start_time
        self.logger.info(
            f"Export completed: {doc_count:,} documents in {final_time:.2f} seconds " +
            f"({doc_count / final_time:.2f} documents/second)"
        )

        return {
            'collection': collection_name,
            'total_documents': doc_count,
            'total_time': final_time,
            'output_file': output_path
        }

    def _flatten_document(self, document: Dict, parent_key: str = '', sep: str = '_') -> Dict:
        items: List = []

        for key, value in document.items():
            new_key = f"{parent_key}{sep}{key}" if parent_key else key

            if isinstance(value, dict):
                items.extend(self._flatten_document(value, new_key, sep).items())
            elif isinstance(value, list):
                items.append((new_key, str(value)))
            else:
                items.append((new_key, value))

        return dict(items)

    def _get_flattened_headers(self, document: Dict) -> List[str]:
        return list(self._flatten_document(document).keys())


def main():
    MONGODB_URI = "mongodb://localhost:27017/"
    DB_NAME = "yelp_db"
    OUTPUT_DIR = Path("../")
    OUTPUT_DIR.mkdir(exist_ok=True)

    exporter = MongoDBExporter(MONGODB_URI, DB_NAME)

    try:
        metrics = exporter.export_to_csv(
            collection_name="user",
            output_path=str(OUTPUT_DIR / "mongo_users.csv")
        )

        print("\nExport Summary:")
        print(f"Total documents: {metrics['total_documents']:,}")
        print(f"Total time: {metrics['total_time']:.2f} seconds")
        print(f"Output file: {metrics['output_file']}")

    except Exception as e:
        print(f"Export failed: {e}")


if __name__ == "__main__":
    main()

# Export Summary:
# Total documents: 100,000
# Total time: 18.19 seconds
# Output file: mongo_users.csv
