USE world;
DROP TABLE IF EXISTS user;
CREATE TABLE user (
    user_id CHAR(22) PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    review_count INT NOT NULL,
    yelping_since DATE NOT NULL,
    useful INT NOT NULL,
    funny INT NOT NULL,
    cool INT NOT NULL,
    fans INT NOT NULL,
    average_stars FLOAT NOT NULL,
    compliment_hot INT NOT NULL,
    compliment_more INT NOT NULL,
    compliment_profile INT NOT NULL,
    compliment_cute INT NOT NULL,
    compliment_list INT NOT NULL,
    compliment_note INT NOT NULL,
    compliment_plain INT NOT NULL,
    compliment_cool INT NOT NULL,
    compliment_funny INT NOT NULL,
    compliment_writer INT NOT NULL,
    compliment_photos INT NOT NULL
);

DROP TABLE IF EXISTS business;

CREATE TABLE business (
    business_id CHAR(22) PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    address VARCHAR(255) NOT NULL,
    city VARCHAR(100) NOT NULL,
    state CHAR(10) NOT NULL,
    postal_code VARCHAR(20) NOT NULL,
    latitude FLOAT NOT NULL,
    longitude FLOAT NOT NULL,
    stars FLOAT NOT NULL,
    review_count INT NOT NULL,
    is_open TINYINT NOT NULL,
    MONDAY CHAR(10) NOT NULL,
    TUESDAY CHAR(10) NOT NULL,
    WEDNESDAY CHAR(10) NOT NULL,
    THURSDAY CHAR(10) NOT NULL,
    FRIDAY CHAR(10) NOT NULL,
    SATURDAY CHAR(10) NOT NULL,
    SUNDAY CHAR(10) NOT NULL
);

DROP TABLE IF EXISTS review;


# CREATE TABLE review (
#     review_id CHAR(22) PRIMARY KEY,
#     user_id CHAR(22) NOT NULL,
#     business_id CHAR(22) NOT NULL,
#     stars INT NOT NULL,
#     date DATE NOT NULL,
#     text TEXT NOT NULL,
#     useful INT DEFAULT 0,
#     funny INT DEFAULT 0,
#     cool INT DEFAULT 0
# );

DROP TABLE IF EXISTS tip;
CREATE TABLE tip (
    user_id VARCHAR(50) NOT NULL,
    business_id VARCHAR(50) NOT NULL,
    text TEXT NOT NULL,
    date DATETIME NOT NULL,
    compliment_count INT NOT NULL,
    PRIMARY KEY (user_id, business_id, date)
);

DROP TABLE IF EXISTS Category;
CREATE TABLE Category (
    business_id VARCHAR(50) NOT NULL,
    category VARCHAR(100) NOT NULL,
    PRIMARY KEY (business_id, category),
    FOREIGN KEY (business_id) REFERENCES Business(business_id)
);

DROP TABLE IF EXISTS Friends;
CREATE TABLE Friends (
    user_id VARCHAR(50) NOT NULL,
    friend_id VARCHAR(50) NOT NULL,
    PRIMARY KEY (user_id, friend_id)
);


SHOW VARIABLES LIKE 'secure_file_priv';


LOAD DATA INFILE 'C:\\ProgramData\\MySQL\\MySQL Server 8.3\\Uploads\\reduced_users.dat'
INTO TABLE user
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
(user_id, name, review_count, yelping_since, useful, funny, cool, fans, average_stars,
 compliment_hot, compliment_more, compliment_profile, compliment_cute,
 compliment_list, compliment_note, compliment_plain, compliment_cool,
 compliment_funny, compliment_writer, compliment_photos);


DELETE FROM business;

LOAD DATA INFILE 'C:\\ProgramData\\MySQL\\MySQL Server 8.3\\Uploads\\reduced_businesses.dat'
IGNORE
INTO TABLE business
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
(business_id, name, address, city, state, postal_code, latitude, longitude, stars, review_count, is_open,
 Monday, Tuesday, Wednesday, Thursday, Friday, Saturday, Sunday);



# LOAD DATA INFILE 'C:\\ProgramData\\MySQL\\MySQL Server 8.3\\Uploads\\yelp_academic_dataset_review.dat'
# IGNORE
# INTO TABLE review
# FIELDS TERMINATED BY '|'
# LINES TERMINATED BY '\n'
# (review_id, user_id, business_id, stars, date, useful, funny, cool);

LOAD DATA INFILE 'C:\\ProgramData\\MySQL\\MySQL Server 8.3\\Uploads\\reduced_tips.dat'
INTO TABLE Tip
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
(user_id, business_id, text, @date, compliment_count)
SET date = STR_TO_DATE(@date, '%Y-%m-%d %H:%i:%s');

LOAD DATA INFILE 'C:\\ProgramData\\MySQL\\MySQL Server 8.3\\Uploads\\reduced_categories.dat'
IGNORE
INTO TABLE Category
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
(business_id, category);

LOAD DATA INFILE 'C:\\ProgramData\\MySQL\\MySQL Server 8.3\\Uploads\\reduced_friends.dat'
IGNORE
INTO TABLE Friends
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
(user_id, friend_id);



#1 Find top-rated (star) restaurant in Nashville
SELECT name, stars
FROM Business
WHERE city = 'Nashville'
ORDER BY stars DESC, review_count DESC
LIMIT 1;

#2 Calculate the average star in every city
SELECT city, AVG(stars) AS average_stars
FROM Business
GROUP BY city;

#3 Find a user's all review
SELECT Tip.*
FROM Tip
WHERE user_id = '___6aix-XvFcQz3GauAPpw';

#4 Calculate users' review by number
SELECT user_id, COUNT(*) AS review_count
FROM Tip
GROUP BY user_id
ORDER BY review_count DESC;

#5 Find businesses pairs that tipped by the same people
SELECT t1.business_id AS business1, t2.business_id AS business2, t1.user_id
FROM Tip t1
JOIN Tip t2 ON t1.user_id = t2.user_id AND t1.business_id < t2.business_id
GROUP BY t1.business_id, t2.business_id, t1.user_id;

#6 Identify the top 10 users who have written the most tips with more than 100 compliments
SELECT user_id, COUNT(*) AS tip_count, SUM(compliment_count) AS total_compliments
FROM Tip
GROUP BY user_id
HAVING total_compliments > 100
ORDER BY tip_count DESC
LIMIT 10;

#7 Find the top 5 most popular business categories based on the sum of compliments their tips received
SELECT c.category, SUM(t.compliment_count) AS total_compliments
FROM Category c
JOIN Tip t ON c.business_id = t.business_id
GROUP BY c.category
ORDER BY total_compliments DESC
LIMIT 5;

#8 Find businesses with at least 50 tips, and calculate the percentage of tips that exceed 10 compliments or less than 10 compliments
SELECT b.business_id, b.name,
       SUM(CASE WHEN t.compliment_count > 10 THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS percent_above_10,
       SUM(CASE WHEN t.compliment_count <= 10 THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS percent_below_or_equal_10
FROM Business b
JOIN Tip t ON b.business_id = t.business_id
GROUP BY b.business_id, b.name
HAVING COUNT(*) >= 50;

#9 Identify the top 5 businesses in each category ranked by the total count of compliments
SELECT c.category, t.business_id, b.name, SUM(t.compliment_count) AS total_compliments
FROM Category c
JOIN Tip t ON c.business_id = t.business_id
JOIN Business b ON c.business_id = b.business_id
GROUP BY c.category, t.business_id
ORDER BY c.category, total_compliments DESC
LIMIT 5;

#10 Rank categories based on the number of compliments received in tips relating to all businesses in that category
SELECT c.category, SUM(t.compliment_count) AS total_compliments
FROM Category c
JOIN Tip t ON c.business_id = t.business_id
GROUP BY c.category
ORDER BY total_compliments DESC;

#11 Number of tips the user writes in each year with elite status
SELECT u.user_id, YEAR(t.date) AS year, COUNT(*) AS tip_count, u.elite
FROM Tip t
JOIN User u ON t.user_id = u.user_id
GROUP BY u.user_id, YEAR(t.date), u.elite;

#12 Find a person’s friends’ friends but not the person’s friends
SELECT DISTINCT f2.friend_id
FROM Friends f1
JOIN Friends f2 ON f1.friend_id = f2.user_id
WHERE f1.user_id = 'USER_ID'
  AND f2.friend_id NOT IN (SELECT friend_id FROM Friends WHERE user_id = '__-YOsZp7ilfYVwD8Wdszg');

SELECT *
from friends
where user_id="KT-Ft32ObIh7J_h_kEdh1g"

SELECT COUNT(*)
FROM friends;

