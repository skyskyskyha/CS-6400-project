import json
import random
import copy
import re
from typing import Dict, List, Tuple, Any


def clean_json_string(json_str: str) -> str:
    # Remove // comments
    json_str = re.sub(r'//.*$', '', json_str)
    # Remove any trailing commas before closing braces/brackets
    json_str = re.sub(r',(\s*[}\]])', r'\1', json_str)
    return json_str


def load_jsonl_file(filename: str) -> Dict[str, Any]:
    data = {}
    with open(filename, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith('//'):  # Skip empty lines and comment lines
                continue
            try:
                # Clean and parse each line
                cleaned_line = clean_json_string(line)
                obj = json.loads(cleaned_line)

                # Each object should have an ID field
                if 'business_id' in obj:
                    data[obj['business_id']] = obj
                elif 'user_id' in obj:
                    data[obj['user_id']] = obj
                elif 'text' in obj:  # for tips, create an artificial ID
                    tip_id = str(hash(obj['text'] + obj['date']))
                    data[tip_id] = obj

            except json.JSONDecodeError as e:
                print(f"Error parsing line in {filename}: {str(e)}")
                print(f"Problematic line: {line[:100]}...")
                continue
    return data


def save_jsonl_file(data: Dict[str, Any], filename: str):
    with open(filename, 'w', encoding='utf-8') as f:
        for item in data.values():
            f.write(json.dumps(item) + '\n')


def generate_random_compliments() -> Dict[str, int]:
    """Generate random compliment counts, all under 100"""
    compliment_types = [
        'compliment_hot', 'compliment_more', 'compliment_profile',
        'compliment_cute', 'compliment_list', 'compliment_note',
        'compliment_plain', 'compliment_cool', 'compliment_funny',
        'compliment_writer', 'compliment_photos'
    ]
    return {compliment_type: random.randint(0, 99) for compliment_type in compliment_types}


def generate_random_elite_years() -> List[int]:
    """Generate random elite years between 2012-2022"""
    num_elite_years = random.randint(0, 10)  # Can be elite for 0 to 10 years
    if num_elite_years == 0:
        return []
    possible_years = list(range(2012, 2023))  # 2012 to 2022
    selected_years = sorted(random.sample(possible_years, num_elite_years))
    return selected_years

def generate_reduced_dataset(source_users: str, source_businesses: str, source_tips: str,
                             output_users: str, output_businesses: str, output_tips: str,
                             max_users: int, max_businesses: int, max_tips: int,
                             max_friends: int, fixed_friend: bool, random_compliments: bool, random_elite_years: bool,
                             random_tip_compliments: bool):
    print("Loading data files...")
    try:
        users = load_jsonl_file(source_users)
        businesses = load_jsonl_file(source_businesses)
        tips = load_jsonl_file(source_tips)
    except Exception as e:
        print(f"Error loading files: {str(e)}")
        return

    print("Selecting random samples...")
    # Select random samples
    selected_user_items = random.sample(list(users.items()), min(max_users, len(users)))
    selected_business_items = random.sample(list(businesses.items()), min(max_businesses, len(businesses)))
    selected_tip_items = random.sample(list(tips.items()), min(max_tips, len(tips)))

    # Create new datasets
    new_users = {}
    new_businesses = {}
    new_tips = {}

    print("Processing users...")
    # Process users first
    user_ids = []
    for user_id, user_data in selected_user_items:
        user_ids.append(user_id)
        new_user = copy.deepcopy(user_data)

        # Update compliments with random values
        if random_compliments:
            random_compliments = generate_random_compliments()
            for compliment_type, count in random_compliments.items():
                new_user[compliment_type] = count

        # Update elite years
        if random_elite_years:
            new_user['elite'] = generate_random_elite_years()

        new_users[user_id] = new_user

    # Now update friends lists
    print("Generating friend relationships...")
    for user_id in new_users:
        num_friends = min(random.randint(0, max_friends), len(user_ids)) if not fixed_friend else max_friends
        new_users[user_id]['friends'] = random.sample(user_ids, num_friends)

    print("Processing businesses...")
    # Process businesses
    business_ids = []
    for business_id, business_data in selected_business_items:
        business_ids.append(business_id)
        new_businesses[business_id] = business_data

    print("Processing tips...")
    # Process tips
    for tip_id, tip_data in selected_tip_items:
        new_tip = copy.deepcopy(tip_data)
        new_tip['user_id'] = random.choice(user_ids)
        new_tip['business_id'] = random.choice(business_ids)
        # Update tip compliment count
        if random_tip_compliments:
            new_tip['compliment_count'] = random.randint(0, 99)
        new_tips[tip_id] = new_tip

    print("Saving reduced datasets...")
    try:
        save_jsonl_file(new_users, output_users)
        save_jsonl_file(new_businesses, output_businesses)
        save_jsonl_file(new_tips, output_tips)
    except Exception as e:
        print(f"Error saving files: {str(e)}")
        return

    # Print statistics
    print("\nDataset statistics:")
    print(f"Original sizes:")
    print(f"Users: {len(users)}")
    print(f"Businesses: {len(businesses)}")
    print(f"Tips: {len(tips)}")

    print(f"\nReduced sizes:")
    print(f"Users: {len(new_users)}")
    print(f"Businesses: {len(new_businesses)}")
    print(f"Tips: {len(new_tips)}")