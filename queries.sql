-- 1. How many squirrels are there in each Park?
SELECT p.park_name, COUNT(s.squirrel_id) as squirrel_count
FROM parks p
LEFT JOIN squirrels s ON p.park_id = s.park_id
GROUP BY p.park_name;

-- 2. What is the most common primary fur color for Squirrels?
SELECT primary_fur_color, COUNT(*) as color_count
FROM squirrels
GROUP BY primary_fur_color
ORDER BY color_count DESC
LIMIT 1;

-- 3. What is the most common activity for Squirrels?
SELECT activities, COUNT(*) as activity_count
FROM squirrels
GROUP BY activities
ORDER BY activity_count DESC
LIMIT 1;

-- 4. A count of all Primary Fur Colors by Park.
SELECT p.park_name, s.primary_fur_color, COUNT(*) as color_count
FROM parks p
JOIN squirrels s ON p.park_id = s.park_id
GROUP BY p.park_name, s.primary_fur_color
ORDER BY p.park_name, color_count DESC;

-- 5. Total number of squirrels in the dataset
SELECT COUNT(*) as total_squirrels
FROM squirrels;
