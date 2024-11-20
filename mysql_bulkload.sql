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
    is_open TINYINT NOT NULL
);

DROP TABLE IF EXISTS review;


CREATE TABLE review (
    review_id CHAR(22) PRIMARY KEY,
    user_id CHAR(22) NOT NULL,
    business_id CHAR(22) NOT NULL,
    stars INT NOT NULL,
    date DATE NOT NULL,
    text TEXT NOT NULL,
    useful INT DEFAULT 0,
    funny INT DEFAULT 0,
    cool INT DEFAULT 0
);


SHOW VARIABLES LIKE 'secure_file_priv';


LOAD DATA INFILE 'C:\\ProgramData\\MySQL\\MySQL Server 8.3\\Uploads\\yelp_academic_dataset_user.dat'
INTO TABLE user
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
(user_id, name, review_count, yelping_since, useful, funny, cool, fans, average_stars,
 compliment_hot, compliment_more, compliment_profile, compliment_cute,
 compliment_list, compliment_note, compliment_plain, compliment_cool,
 compliment_funny, compliment_writer, compliment_photos);


LOAD DATA INFILE 'C:\\ProgramData\\MySQL\\MySQL Server 8.3\\Uploads\\yelp_academic_dataset_business.dat'
INTO TABLE business
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
(business_id, name, address, city, state, postal_code, latitude, longitude,
 stars, review_count, is_open);



LOAD DATA INFILE 'C:\\ProgramData\\MySQL\\MySQL Server 8.3\\Uploads\\yelp_academic_dataset_review.dat'
IGNORE
INTO TABLE review
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
(review_id, user_id, business_id, stars, date, useful, funny, cool);

