from neo4j import GraphDatabase
import json
from datetime import datetime
import logging
from typing import Dict, Any
import time
from pathlib import Path


class Neo4jLoader:
    def __init__(self, uri: str, user: str, password: str):
        """Initialize Neo4j connection and set up logging."""
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

        # Set up logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('neo4j_import.log'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)

    def close(self):
        """Close the Neo4j driver connection."""
        self.driver.close()

    def clear_database(self):
        """Clear all nodes and relationships from the database."""
        with self.driver.session() as session:
            session.run("MATCH (n) DETACH DELETE n")
            self.logger.info("Database cleared")

    def create_constraints(self):
        """Create necessary constraints and indexes for business, user, and tip data."""
        constraints = [
            # Primary key constraints
            "CREATE CONSTRAINT business_id IF NOT EXISTS FOR (b:Business) REQUIRE b.business_id IS UNIQUE",
            "CREATE CONSTRAINT user_id IF NOT EXISTS FOR (u:User) REQUIRE u.user_id IS UNIQUE",
            "CREATE CONSTRAINT category_name IF NOT EXISTS FOR (c:Category) REQUIRE c.name IS UNIQUE",

            # Composite constraint for tip (since tips might be repeated by same user for same business)
            "CREATE CONSTRAINT tip_composite IF NOT EXISTS FOR (t:Tip) REQUIRE (t.user_id, t.business_id, t.date) IS UNIQUE",

            # Indexes for frequent lookups
            "CREATE INDEX business_name IF NOT EXISTS FOR (b:Business) ON (b.name)",
            "CREATE INDEX business_city IF NOT EXISTS FOR (b:Business) ON (b.city)",
            "CREATE INDEX user_name IF NOT EXISTS FOR (u:User) ON (u.name)",
        ]

        with self.driver.session() as session:
            for constraint in constraints:
                try:
                    session.run(constraint)
                    self.logger.info(f"Created constraint/index: {constraint}")
                except Exception as e:
                    self.logger.warning(f"Constraint/index creation failed: {str(e)}")
            self.logger.info("Finished creating constraints and indexes")

    def load_businesses(self, file_path: str, batch_size: int = 1000):
        """Load businesses from JSON file in batches with flattened hours."""
        self.logger.info("Starting business import")
        businesses_created = 0

        def create_business_batch(tx, batch):
            # First query: Create businesses with all properties
            businesses_query = """
            UNWIND $batch AS business
            MERGE (b:Business {business_id: business.business_id})
            SET 
                b.name = business.name,
                b.address = business.address,
                b.city = business.city,
                b.state = business.state,
                b.postal_code = business.postal_code,
                b.latitude = business.latitude,
                b.longitude = business.longitude,
                b.stars = business.stars,
                b.review_count = business.review_count,
                b.is_open = business.is_open,
                b.hours_monday = COALESCE(business.hours.Monday, null),
                b.hours_tuesday = COALESCE(business.hours.Tuesday, null),
                b.hours_wednesday = COALESCE(business.hours.Wednesday, null),
                b.hours_thursday = COALESCE(business.hours.Thursday, null),
                b.hours_friday = COALESCE(business.hours.Friday, null),
                b.hours_saturday = COALESCE(business.hours.Saturday, null),
                b.hours_sunday = COALESCE(business.hours.Sunday, null)
            """
            tx.run(businesses_query, batch=batch)

            # Second query: Create categories and relationships
            categories_query = """
                    UNWIND $batch AS business
                    MATCH (b:Business {business_id: business.business_id})
                    UNWIND business.categories AS category
                    MERGE (c:Category {name: category})
                    MERGE (b)-[:IN_CATEGORY]->(c)
                    """
            result = tx.run(categories_query, batch=batch)
            return result.consume().counters.nodes_created

        batch = []
        try:
            with open(file_path, 'r', encoding='utf-8') as file, self.driver.session() as session:
                for line in file:
                    try:
                        business = json.loads(line.strip())
                    except json.JSONDecodeError as e:
                        self.logger.warning(f"Failed to parse JSON line: {e}")
                        continue

                    # Handle missing hours and categories gracefully
                    business['hours'] = business.get('hours', {})
                    business['categories'] = business.get('categories', [])
                    batch.append(business)

                    if len(batch) >= batch_size:
                        try:
                            businesses_created += session.write_transaction(create_business_batch, batch)
                            self.logger.info(f"Processed {businesses_created} businesses")
                        except Exception as e:
                            self.logger.error(f"Failed to process batch: {e}")
                        batch = []

                # Process remaining businesses
                if batch:
                    try:
                        businesses_created += session.write_transaction(create_business_batch, batch)
                    except Exception as e:
                        self.logger.error(f"Failed to process remaining batch: {e}")

        except FileNotFoundError as e:
            self.logger.error(f"File not found: {e}")

        self.logger.info(f"Completed business import. Total businesses created: {businesses_created}")

    def load_users(self, file_path: str, batch_size: int = 1000):
        """Load users from JSON file in batches with separate friend relationship handling."""
        self.logger.info("Starting user import")
        users_created = 0

        def create_users_only(tx, batch):
            """Create user nodes without relationships."""
            users_query = """UNWIND $batch AS user
    MERGE (u:User {user_id: user.user_id})
    SET u.name = user.name,
        u.review_count = user.review_count,
        u.yelping_since = user.yelping_since,
        u.average_stars = user.average_stars,
        u.fans = user.fans,
        u.elite_years = user.elite,
        u.compliment_hot = user.compliment_hot,
        u.compliment_more = user.compliment_more,
        u.compliment_profile = user.compliment_profile,
        u.compliment_cute = user.compliment_cute,
        u.compliment_list = user.compliment_list,
        u.compliment_note = user.compliment_note,
        u.compliment_plain = user.compliment_plain,
        u.compliment_cool = user.compliment_cool,
        u.compliment_funny = user.compliment_funny,
        u.compliment_writer = user.compliment_writer,
        u.compliment_photos = user.compliment_photos"""
            result = tx.run(users_query, batch=batch)
            return result.consume().counters.nodes_created

        def create_friend_relationships(tx, user_id, friend_batch):
            """Create friend relationships in small batches."""
            friends_query = """
            MATCH (u:User {user_id: $user_id})
            UNWIND $friends AS friend_id
            MERGE (friend:User {user_id: friend_id})
            MERGE (u)-[:FRIENDS_WITH]->(friend)"""
            tx.run(friends_query, user_id=user_id, friends=friend_batch)

        def clean_user_data(user_data):
            """Clean and validate user data before import."""
            # if 'elite' not in user_data:
            #     user_data['elite'] = []
            # if 'friends' not in user_data:
            #     user_data['friends'] = []
            #
            # # Ensure all compliment fields exist
            # compliment_types = ['hot', 'more', 'profile', 'cute', 'list', 'note',
            #                     'plain', 'cool', 'funny', 'writer', 'photos']
            # for comp_type in compliment_types:
            #     comp_key = f'compliment_{comp_type}'
            #     if comp_key not in user_data:
            #         user_data[comp_key] = 0

            # Store yelping_since as a string
            if 'yelping_since' in user_data:
                try:
                    dt = datetime.strptime(user_data['yelping_since'], '%Y-%m-%d %H:%M:%S')
                    user_data['yelping_since'] = dt.strftime('%Y-%m-%d')
                except ValueError:
                    try:
                        datetime.strptime(user_data['yelping_since'], '%Y-%m-%d')
                    except ValueError:
                        user_data['yelping_since'] = '2000-01-01'
            else:
                user_data['yelping_since'] = '2000-01-01'

            return user_data

        # First pass: Create all user nodes
        batch = []
        with open(file_path, 'r', encoding='utf-8') as file:
            for line in file:
                try:
                    user = json.loads(line.strip())
                    # Store friends separately and remove from main user data
                    friends = user.get('friends', [])
                    user['friends'] = []  # Empty the friends list for node creation
                    user = clean_user_data(user)
                    batch.append(user)

                    if len(batch) >= batch_size:
                        with self.driver.session() as session:
                            users_created += session.write_transaction(create_users_only, batch)
                            self.logger.info(f"Processed {users_created} users")
                            batch = []
                except json.JSONDecodeError:
                    self.logger.error(f"Failed to parse JSON line: {line.strip()}")
                    continue
                except Exception as e:
                    self.logger.error(f"Error processing user: {str(e)}")
                    continue

            # Process remaining users
            if batch:
                with self.driver.session() as session:
                    users_created += session.write_transaction(create_users_only, batch)

        # Second pass: Create friend relationships in smaller batches
        self.logger.info("Starting friend relationships creation")
        friend_batch_size = 100  # Smaller batch size for friend relationships

        with open(file_path, 'r', encoding='utf-8') as file:
            for line in file:
                try:
                    user = json.loads(line.strip())
                    user_id = user['user_id']
                    friends = user.get('friends', [])

                    # Process friends in smaller batches
                    for i in range(0, len(friends), friend_batch_size):
                        friend_batch = friends[i:i + friend_batch_size]
                        with self.driver.session() as session:
                            session.write_transaction(create_friend_relationships, user_id, friend_batch)

                    if len(friends) > 0:
                        self.logger.info(f"Processed {len(friends)} friends for user {user_id}")

                except Exception as e:
                    self.logger.error(f"Error processing friends for user: {str(e)}")
                    continue

        self.logger.info(f"Completed user import. Total users created: {users_created}")


    def load_tips(self, file_path: str, batch_size: int = 1000):
        """Load tips from JSON file in batches."""
        self.logger.info("Starting tips import")
        tips_created = 0

        def create_tips_batch(tx, batch):
            # Create tips and relationships
            tips_query = """
            UNWIND $batch AS tip
            MATCH (u:User {user_id: tip.user_id})
            MATCH (b:Business {business_id: tip.business_id})
            MERGE (t:Tip {
                text: tip.text,
                date: tip.date,
                user_id: tip.user_id,
                business_id: tip.business_id
            })
            SET t.compliment_count = tip.compliment_count
            MERGE (u)-[:WROTE_TIP]->(t)
            MERGE (t)-[:ABOUT]->(b)
            """

            result = tx.run(tips_query, batch=batch)
            return result.consume().counters.nodes_created

        batch = []
        with open(file_path, 'r', encoding='utf-8') as file:
            for line in file:
                try:
                    tip = json.loads(line.strip())
                    batch.append(tip)

                    if len(batch) >= batch_size:
                        with self.driver.session() as session:
                            tips_created += session.write_transaction(create_tips_batch, batch)
                            self.logger.info(f"Processed {tips_created} tips")
                            batch = []
                except json.JSONDecodeError:
                    self.logger.error(f"Failed to parse JSON line: {line.strip()}")
                    continue
                except Exception as e:
                    self.logger.error(f"Error processing tip: {str(e)}")
                    continue

            # Process remaining tips
            if batch:
                with self.driver.session() as session:
                    tips_created += session.write_transaction(create_tips_batch, batch)

        self.logger.info(f"Completed tips import. Total tips created: {tips_created}")

    def run_query(self, query: str, parameters: Dict[str, Any] = None):
        with self.driver.session() as session:
            result = session.run(query, parameters)
            return result.data()