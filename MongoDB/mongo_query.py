from collections import defaultdict

import pymongo
import time


def find_top_rated_restaurant_in_nashville():
    MONGODB_URI = "mongodb://localhost:27017/"
    DB_NAME = "yelp_db"
    COLLECTION_NAME = "business"

    client = pymongo.MongoClient(MONGODB_URI)
    db = client[DB_NAME]
    collection = db[COLLECTION_NAME]

    start_time = time.time()
    top_restaurant = collection.find_one(
        {
            "city": "Nashville",
            "categories": {"$regex": ".*Restaurant.*"}
        },
        sort=[
            ("stars", pymongo.DESCENDING),
            ("review_count", pymongo.DESCENDING)
        ]
    )
    query_time = time.time() - start_time

    if top_restaurant:
        print("Top-rated restaurant in Nashville:")
        print(f"Name: {top_restaurant['name']}")
        print(f"Stars: {top_restaurant['stars']}")
        print(f"Address: {top_restaurant['address']}")
        print(f"Reviews: {top_restaurant['review_count']}")
        if 'categories' in top_restaurant:
            print(f"Categories: {top_restaurant['categories']}")
    else:
        print("No restaurants found in Nashville.")

    print(f"Query time: {query_time:.4f} seconds")
    client.close()


def calculate_average_stars_per_city():
    MONGODB_URI = "mongodb://localhost:27017/"
    DB_NAME = "yelp_db"
    COLLECTION_NAME = "business"

    client = pymongo.MongoClient(MONGODB_URI)
    db = client[DB_NAME]
    collection = db[COLLECTION_NAME]

    start_time = time.time()

    pipeline = [
        {
            "$group": {
                "_id": "$city",
                "average_stars": {"$avg": "$stars"},
                "business_count": {"$sum": 1}
            }
        },
        {
            "$sort": {"average_stars": -1}
        }
    ]

    results = collection.aggregate(pipeline)
    query_time = time.time() - start_time

    print("Average Stars by City:")
    print("-" * 50)
    print(f"{'City':<30} {'Avg Stars':<10} {'# Businesses'}")
    print("-" * 50)

    for result in results:
        city = result['_id']
        avg_stars = result['average_stars']
        count = result['business_count']
        print(f"{city:<30} {avg_stars:,.2f}      {count:,}")

    print("-" * 50)
    print(f"Query execution time: {query_time:.4f} seconds")
    client.close()


def find_user_reviews(user_id):
    MONGODB_URI = "mongodb://localhost:27017/"
    DB_NAME = "yelp_db"
    COLLECTION_NAME = "tip"

    client = pymongo.MongoClient(MONGODB_URI)
    db = client[DB_NAME]
    collection = db[COLLECTION_NAME]

    start_time = time.time()

    tips = collection.find(
        {"user_id": user_id}
    ).sort("date", pymongo.DESCENDING)

    query_time = time.time() - start_time

    print(f"Reviews for user {user_id}:")
    print("-" * 70)

    tip_count = 0
    for tip in tips:
        tip_count += 1
        print(f"Business ID: {tip.get('business_id', 'N/A')}")
        print(f"Date: {tip.get('date', 'N/A')}")
        print(f"Text: {tip.get('text', 'N/A')}")
        print(f"Compliment Count: {tip.get('compliment_count', 0)}")
        print("-" * 70)

    if tip_count == 0:
        print("No reviews found for this user.")
    else:
        print(f"Total reviews found: {tip_count}")

    print(f"Query execution time: {query_time:.4f} seconds")

    client.close()


def calculate_users_review_count():
    MONGODB_URI = "mongodb://localhost:27017/"
    DB_NAME = "yelp_db"
    COLLECTION_NAME = "tip"

    client = pymongo.MongoClient(MONGODB_URI)
    db = client[DB_NAME]
    collection = db[COLLECTION_NAME]

    start_time = time.time()

    pipeline = [
        {
            "$group": {
                "_id": "$user_id",
                "review_count": {"$sum": 1},
                "avg_compliments": {"$avg": "$compliment_count"},
                "latest_review": {"$max": "$date"}
            }
        },
        {
            "$sort": {"review_count": -1}
        }
    ]

    results = collection.aggregate(pipeline)
    query_time = time.time() - start_time

    print("User Review Statistics:")
    print("-" * 85)
    print(f"{'User ID':<35} {'Review Count':<12} {'Avg Compliments':<15} {'Latest Review'}")
    print("-" * 85)

    user_count = 0
    total_reviews = 0

    for result in results:
        user_count += 1
        total_reviews += result['review_count']
        print(
            f"{result['_id']:<35} {result['review_count']:<12} {result['avg_compliments']:,.2f}           {result['latest_review']}")

        if user_count == 25:
            break

    print("-" * 85)
    print(f"Total number of users: {user_count}")
    print(f"Total number of reviews: {total_reviews}")
    print(f"Query execution time: {query_time:.4f} seconds")

    client.close()


def find_business_pairs_tipped_by_same_people():
    MONGODB_URI = "mongodb://localhost:27017/"
    DB_NAME = "yelp_db"
    COLLECTION_NAME = "tip"

    client = pymongo.MongoClient(MONGODB_URI)
    db = client[DB_NAME]
    collection = db[COLLECTION_NAME]

    start_time = time.time()

    pipeline = [
        {
            "$group": {
                "_id": "$user_id",
                "businesses": {"$addToSet": "$business_id"}
            }
        }
    ]

    results = collection.aggregate(pipeline)

    business_pairs = defaultdict(set)
    pair_count = 0

    for result in results:
        user_id = result['_id']
        businesses = sorted(result['businesses'])

        for i in range(len(businesses)):
            for j in range(i + 1, len(businesses)):
                pair = (businesses[i], businesses[j])
                business_pairs[pair].add(user_id)
                pair_count += 1

    query_time = time.time() - start_time

    print("Business Pairs with Common Reviewers:")
    print("-" * 100)
    print(f"{'Business 1':<45} {'Business 2':<45} {'Common Reviewers':<10}")
    print("-" * 100)

    sorted_pairs = sorted(business_pairs.items(),
                          key=lambda x: len(x[1]),
                          reverse=True)

    for i, (pair, user_set) in enumerate(sorted_pairs):
        if i >= 25:
            break

        business1, business2 = pair
        print(f"{business1:<45} {business2:<45} {len(user_set):<10}")

    print("-" * 100)
    print(f"Total number of business pairs found: {pair_count}")
    print(f"Query execution time: {query_time:.4f} seconds")

    client.close()


def find_top_users_with_most_tips_and_compliments():
    MONGODB_URI = "mongodb://localhost:27017/"
    DB_NAME = "yelp_db"
    COLLECTION_NAME = "tip"

    client = pymongo.MongoClient(MONGODB_URI)
    db = client[DB_NAME]
    collection = db[COLLECTION_NAME]

    start_time = time.time()

    pipeline = [
        {
            "$group": {
                "_id": "$user_id",
                "tip_count": {"$sum": 1},
                "total_compliments": {"$sum": "$compliment_count"},
                "avg_compliments": {"$avg": "$compliment_count"},
                "latest_tip": {"$max": "$date"}
            }
        },
        {
            "$match": {
                "total_compliments": {"$gt": 100}
            }
        },
        {
            "$sort": {
                "tip_count": -1
            }
        },
        {
            "$limit": 10
        }
    ]

    results = collection.aggregate(pipeline)
    query_time = time.time() - start_time

    print("Top 10 Users with Most Tips (>100 total compliments):")
    print("-" * 110)
    print(f"{'User ID':<35} {'Tips':<8} {'Total Compliments':<18} {'Avg Compliments':<16} {'Latest Tip'}")
    print("-" * 110)

    for result in results:
        print(f"{result['_id']:<35} {result['tip_count']:<8} {result['total_compliments']:<18} "
              f"{result['avg_compliments']:,.2f}           {result['latest_tip']}")

    print("-" * 110)
    print(f"Query execution time: {query_time:.4f} seconds")

    client.close()


def find_top_business_categories_by_compliments():
    MONGODB_URI = "mongodb://localhost:27017/"
    DB_NAME = "yelp_db"
    COLLECTION_NAME = "tip"

    client = pymongo.MongoClient(MONGODB_URI)
    db = client[DB_NAME]
    tip_collection = db[COLLECTION_NAME]
    business_collection = db['business']

    start_time = time.time()

    pipeline = [
        {
            "$lookup": {
                "from": "business",
                "localField": "business_id",
                "foreignField": "business_id",
                "as": "business_info"
            }
        },
        {"$unwind": "$business_info"},
        {"$unwind": "$business_info.categories"},
        {
            "$group": {
                "_id": "$business_info.categories",
                "total_compliments": {"$sum": "$compliment_count"},
                "tip_count": {"$sum": 1},
                "business_count": {"$addToSet": "$business_id"}
            }
        },
        {"$sort": {"total_compliments": -1}},
        {"$limit": 5}
    ]

    results = tip_collection.aggregate(pipeline)
    query_time = time.time() - start_time

    print("Top 5 Business Categories by Tip Compliments:")
    print("-" * 85)
    print(f"{'Category':<35} {'Total Compliments':<20} {'Tips':<10} {'Businesses'}")
    print("-" * 85)

    for result in results:
        category = result['_id']
        total_compliments = result['total_compliments']
        tip_count = result['tip_count']
        business_count = len(result['business_count'])

        print(f"{category:<35} {total_compliments:<20} {tip_count:<10} {business_count}")

    print("-" * 85)
    print(f"Query execution time: {query_time:.4f} seconds")

    client.close()


def find_businesses_with_tip_compliment_percentages():
    MONGODB_URI = "mongodb://localhost:27017/"
    DB_NAME = "yelp_db"
    COLLECTION_NAME = "tip"

    client = pymongo.MongoClient(MONGODB_URI)
    db = client[DB_NAME]
    tip_collection = db[COLLECTION_NAME]
    business_collection = db['business']

    start_time = time.time()

    pipeline = [
        {
            "$group": {
                "_id": "$business_id",
                "total_tips": {"$sum": 1},
                "high_compliments": {
                    "$sum": {
                        "$cond": [{"$gt": ["$compliment_count", 10]}, 1, 0]
                    }
                },
                "low_compliments": {
                    "$sum": {
                        "$cond": [{"$lte": ["$compliment_count", 10]}, 1, 0]
                    }
                },
                "avg_compliments": {"$avg": "$compliment_count"}
            }
        },
        {
            "$match": {
                "total_tips": {"$gte": 50}
            }
        },
        {
            "$lookup": {
                "from": "business",
                "localField": "_id",
                "foreignField": "business_id",
                "as": "business_info"
            }
        },
        {"$unwind": "$business_info"},
        {
            "$project": {
                "business_name": "$business_info.name",
                "total_tips": 1,
                "avg_compliments": 1,
                "percent_above_10": {
                    "$multiply": [
                        {"$divide": ["$high_compliments", "$total_tips"]},
                        100
                    ]
                },
                "percent_below_10": {
                    "$multiply": [
                        {"$divide": ["$low_compliments", "$total_tips"]},
                        100
                    ]
                }
            }
        },
        {"$sort": {"total_tips": -1}}
    ]

    results = tip_collection.aggregate(pipeline)
    query_time = time.time() - start_time

    print("Businesses with 50+ Tips - Compliment Distribution Analysis:")
    print("-" * 120)
    print(f"{'Business Name':<40} {'Business ID':<25} {'Total Tips':<12} "
          f"{'% >10':<8} {'% ≤10':<8} {'Avg Comp.'}")
    print("-" * 120)

    for result in results:
        print(f"{result['business_name'][:38]:<40} {result['_id']:<25} {result['total_tips']:<12} "
              f"{result['percent_above_10']:>6.1f}% {result['percent_below_10']:>6.1f}% "
              f"{result['avg_compliments']:>8.1f}")

    print("-" * 120)
    print(f"Query execution time: {query_time:.4f} seconds")

    client.close()


def find_top_businesses_in_each_category():
    MONGODB_URI = "mongodb://localhost:27017/"
    DB_NAME = "yelp_db"
    COLLECTION_NAME = "tip"

    client = pymongo.MongoClient(MONGODB_URI)
    db = client[DB_NAME]
    tip_collection = db[COLLECTION_NAME]

    start_time = time.time()

    pipeline = [
        {
            "$lookup": {
                "from": "business",
                "localField": "business_id",
                "foreignField": "business_id",
                "as": "business_info"
            }
        },
        {"$unwind": "$business_info"},
        {"$unwind": "$business_info.categories"},
        {
            "$group": {
                "_id": {
                    "category": "$business_info.categories",
                    "business_id": "$business_id",
                    "business_name": "$business_info.name"
                },
                "total_compliments": {"$sum": "$compliment_count"},
                "tip_count": {"$sum": 1}
            }
        },
        {
            "$sort": {
                "_id.category": 1,
                "total_compliments": -1
            }
        }
    ]

    results = tip_collection.aggregate(pipeline)

    categories = defaultdict(list)
    for result in results:
        category = result["_id"]["category"]
        if len(categories[category]) < 5:
            categories[category].append(result)

    query_time = time.time() - start_time

    print("Top 5 Businesses in Each Category by Compliments:")
    print("-" * 120)

    for category in sorted(categories.keys()):
        print(f"\nCategory: {category}")
        print("-" * 120)
        print(f"{'Business Name':<40} {'Business ID':<25} {'Total Compliments':<20} {'Tips'}")
        print("-" * 120)

        for business in categories[category]:
            print(f"{business['_id']['business_name'][:38]:<40} "
                  f"{business['_id']['business_id']:<25} "
                  f"{business['total_compliments']:<20} "
                  f"{business['tip_count']}")

    print(f"\nQuery execution time: {query_time:.4f} seconds")

    client.close()


def rank_categories_by_compliments():
    MONGODB_URI = "mongodb://localhost:27017/"
    DB_NAME = "yelp_db"
    COLLECTION_NAME = "tip"

    client = pymongo.MongoClient(MONGODB_URI)
    db = client[DB_NAME]
    tip_collection = db[COLLECTION_NAME]

    start_time = time.time()

    pipeline = [
        {
            "$lookup": {
                "from": "business",
                "localField": "business_id",
                "foreignField": "business_id",
                "as": "business_info"
            }
        },
        {"$unwind": "$business_info"},
        {"$unwind": "$business_info.categories"},
        {
            "$group": {
                "_id": "$business_info.categories",
                "total_compliments": {"$sum": "$compliment_count"},
                "tip_count": {"$sum": 1},
                "business_count": {"$addToSet": "$business_id"},
                "avg_compliments_per_tip": {"$avg": "$compliment_count"}
            }
        },
        {"$sort": {"total_compliments": -1}}
    ]

    results = tip_collection.aggregate(pipeline)
    query_time = time.time() - start_time

    print("Categories Ranked by Total Compliments:")
    print("-" * 100)
    print(f"{'Rank':<6} {'Category':<35} {'Total Compliments':<20} {'Tips':<10} "
          f"{'Businesses':<12} {'Avg/Tip'}")
    print("-" * 100)

    for rank, result in enumerate(results, 1):
        category = result['_id']
        total_compliments = result['total_compliments']
        tip_count = result['tip_count']
        business_count = len(result['business_count'])
        avg_per_tip = result['avg_compliments_per_tip']

        print(f"{rank:<6} {category[:33]:<35} {total_compliments:<20,} "
              f"{tip_count:<10,} {business_count:<12,} {avg_per_tip:.2f}")

    print("-" * 100)
    print(f"Query execution time: {query_time:.4f} seconds")

    client.close()


def find_user_tips_per_year_with_elite_status():
    MONGODB_URI = "mongodb://localhost:27017/"
    DB_NAME = "yelp_db"
    COLLECTION_NAME = "tip"

    client = pymongo.MongoClient(MONGODB_URI)
    db = client[DB_NAME]
    tip_collection = db[COLLECTION_NAME]

    start_time = time.time()

    pipeline = [
        {
            "$lookup": {
                "from": "user",
                "localField": "user_id",
                "foreignField": "user_id",
                "as": "user_info"
            }
        },
        {"$unwind": "$user_info"},
        {
            "$addFields": {
                "year": {"$year": {"$dateFromString": {"dateString": "$date"}}}
            }
        },
        {
            "$group": {
                "_id": {
                    "user_id": "$user_id",
                    "year": "$year",
                    "elite": "$user_info.elite"
                },
                "tip_count": {"$sum": 1},
                "avg_compliments": {"$avg": "$compliment_count"},
                "total_compliments": {"$sum": "$compliment_count"}
            }
        },
        {
            "$sort": {
                "_id.user_id": 1,
                "_id.year": 1
            }
        }
    ]

    results = tip_collection.aggregate(pipeline)
    query_time = time.time() - start_time

    print("User Tips Analysis by Year and Elite Status:")
    print("-" * 120)
    print(f"{'User ID':<35} {'Year':<6} {'Tips':<8} {'Avg Comp.':<12} {'Total Comp.':<12} {'Elite Status'}")
    print("-" * 120)

    current_user = None
    for result in results:
        user_id = result['_id']['user_id']
        year = result['_id']['year']
        elite_status = result['_id']['elite']

        if current_user and current_user != user_id:
            print()
        current_user = user_id

        print(f"{user_id:<35} {year:<6} {result['tip_count']:<8} "
              f"{result['avg_compliments']:>8.2f}    {result['total_compliments']:>8}      "
              f"{elite_status if elite_status else 'Non-Elite'}")

    print("-" * 120)
    print(f"Query execution time: {query_time:.4f} seconds")

    client.close()


def find_friends_of_friends():
    MONGODB_URI = "mongodb://localhost:27017/"
    DB_NAME = "yelp_db"
    COLLECTION_NAME = "user"

    client = pymongo.MongoClient(MONGODB_URI)
    db = client[DB_NAME]
    user_collection = db[COLLECTION_NAME]

    start_time = time.time()

    pipeline = [
        {
            "$match": {
                "user_id": {"$regex": "^A"}
            }
        },
        {
            "$lookup": {
                "from": "user",
                "localField": "friends",
                "foreignField": "user_id",
                "as": "direct_friends"
            }
        },
        {"$unwind": "$direct_friends"},
        {
            "$lookup": {
                "from": "user",
                "localField": "direct_friends.friends",
                "foreignField": "user_id",
                "as": "friends_of_friends"
            }
        },
        {"$unwind": "$friends_of_friends"},
        {
            "$group": {
                "_id": "$user_id",
                "direct_friends": {"$addToSet": "$direct_friends.user_id"},
                "potential_fof": {"$addToSet": "$friends_of_friends.user_id"}
            }
        },
        {"$sort": {"_id": 1}}
    ]

    results = user_collection.aggregate(pipeline)

    processed_results = []
    for result in results:
        user_id = result["_id"]
        direct_friends = set(result["direct_friends"])
        all_fof = set(result["potential_fof"])

        valid_fof = all_fof - direct_friends - {user_id}

        processed_results.append({
            "PersonID": user_id,
            "FriendsOfFriends": sorted(list(valid_fof))
        })

    end_time = time.time()
    query_time = end_time - start_time

    print(f"Friends of Friends Analysis for Users Starting with 'A':")
    print("-" * 80)

    for result in processed_results:
        print(f"\nUser: {result['PersonID']}")
        print(f"Friends of Friends ({len(result['FriendsOfFriends'])}): "
              f"{', '.join(result['FriendsOfFriends'][:5])}..."
              if len(result['FriendsOfFriends']) > 5 else
              f"{', '.join(result['FriendsOfFriends'])}")

    print(f"\nTotal time: {query_time:.2f} seconds")
    print(f"Total users processed: {len(processed_results)}")

    client.close()


if __name__ == '__main__':
    find_top_rated_restaurant_in_nashville()
    calculate_average_stars_per_city()
    find_user_reviews("___6aix-XvFcQz3GauAPpw")
    calculate_users_review_count()
    find_business_pairs_tipped_by_same_people()
    find_top_users_with_most_tips_and_compliments()

    find_top_business_categories_by_compliments()
    find_businesses_with_tip_compliment_percentages()
    find_top_businesses_in_each_category()
    rank_categories_by_compliments()
    find_user_tips_per_year_with_elite_status()
    find_friends_of_friends()
