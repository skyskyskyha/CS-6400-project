import json

# business
input_file = "yelp_academic_dataset_business.json"
output_file = "yelp_academic_dataset_business.dat"

with open(input_file, "r", encoding="utf-8") as f:
    data = [json.loads(line) for line in f]


fields = [
    "business_id", "name", "address", "city", "state",
    "postal_code", "latitude", "longitude", "stars",
    "review_count", "is_open"
]

with open(output_file, "w", encoding="utf-8") as f:
    for item in data:
        row = "|".join([str(item.get(field, "")).replace('®', '').replace('|', ' ') for field in fields])
        f.write(row + "\n")


# review
review_input_file = "yelp_academic_dataset_review.json"
review_output_file = "yelp_academic_dataset_review.dat"
with open(review_input_file, "r", encoding="utf-8") as f:
    review_data = [json.loads(line) for line in f]

review_fields = [
    "review_id", "user_id", "business_id", "stars",
    "date",
    # "text",
    "useful", "funny", "cool"
]

with open(review_output_file, "w", encoding="utf-8") as f:
    for item in review_data:
        row = "|".join([str(item.get(field, "")).replace('|', ' ') for field in review_fields])
        f.write(row + "\n")

# user
user_input_file = "yelp_academic_dataset_user.json"
user_output_file = "yelp_academic_dataset_user.dat"

with open(user_input_file, "r", encoding="utf-8") as f:
    user_data = [json.loads(line) for line in f]

user_fields = [
    "user_id", "name", "review_count", "yelping_since",
    "useful", "funny", "cool", "fans",
    "average_stars", "compliment_hot", "compliment_more",
    "compliment_profile", "compliment_cute", "compliment_list",
    "compliment_note", "compliment_plain", "compliment_cool",
    "compliment_funny", "compliment_writer", "compliment_photos"
]

with open(user_output_file, "w", encoding="utf-8") as f:
    for item in user_data:
        row = "|".join([str(item.get(field, "")).replace('|', ' ') for field in user_fields])
        f.write(row + "\n")
