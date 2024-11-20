# Find top rated restaurant in one city (74ms)
SELECT
    business_id, name, stars
FROM
    business
WHERE
    city = 'St Louis'
ORDER BY
    stars DESC;


# Calculate average star in every city
SELECT
    city,
    COUNT(business_id) AS total_businesses,
    AVG(stars) AS average_stars
FROM
    business
GROUP BY
    city
ORDER BY
    average_stars DESC;



# Find a user's all review (13s 45ms)
SELECT
    user_id,
    review_id,
    business_id,
    stars,
    date,
    useful,
    funny,
    cool
FROM
    review
WHERE
    user_id = 'U7L73-H42e7uOzNmlFXiRw';

# Calculate users' review by number (>5min)
SELECT
    user_id,
    COUNT(*) AS review_count
FROM
    review
GROUP BY
    user_id
ORDER BY
    review_count DESC;


# Find businesses pairs that reviewed by same people (>5min)
SELECT
    r1.business_id AS business_id_1,
    r2.business_id AS business_id_2,
    COUNT(DISTINCT r1.user_id) AS shared_users
FROM
    review r1
JOIN
    review r2 ON r1.user_id = r2.user_id
WHERE
    r1.business_id != r2.business_id
GROUP BY
    r1.business_id, r2.business_id
ORDER BY
    shared_users DESC;
