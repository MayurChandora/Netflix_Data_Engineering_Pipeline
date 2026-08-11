-- Netflix Data Engineering Project
-- Databricks SQL Dashboard Queries


-- =========================================================
-- 1. OVERVIEW Main Dashboard
-- =========================================================

SELECT
    COUNT(DISTINCT show_id) AS total_titles,

    COUNT(DISTINCT CASE
        WHEN type = 'Movie' THEN show_id
    END) AS total_movies,

    COUNT(DISTINCT CASE
        WHEN type = 'TV Show' THEN show_id
    END) AS total_tv_shows,

    ROUND(
        100.0 * COUNT(DISTINCT CASE
            WHEN type = 'Movie' THEN show_id
        END)
        / COUNT(DISTINCT show_id),
        2
    ) AS movie_percentage,

    ROUND(
        100.0 * COUNT(DISTINCT CASE
            WHEN type = 'TV Show' THEN show_id
        END)
        / COUNT(DISTINCT show_id),
        2
    ) AS tv_show_percentage

FROM netflix_catalog.gold.content_overview;


-- =========================================================
-- 2. MOVIES VS TV SHOWS
-- =========================================================

SELECT
    type,
    COUNT(DISTINCT show_id) AS title_count
FROM netflix_catalog.gold.content_overview
WHERE type IS NOT NULL
GROUP BY type;


-- =========================================================
-- 3. CONTENT ADDED BY YEAR
-- =========================================================

SELECT
    YEAR(date_added) AS added_year,
    COUNT(DISTINCT show_id) AS title_count
FROM netflix_catalog.gold.content_overview
WHERE date_added IS NOT NULL
GROUP BY YEAR(date_added)
ORDER BY added_year;


-- =========================================================
-- 4. TITLES BY RELEASE YEAR
-- =========================================================

SELECT
    release_year,
    COUNT(DISTINCT show_id) AS title_count
FROM netflix_catalog.gold.content_overview
WHERE release_year IS NOT NULL
GROUP BY release_year
ORDER BY release_year;


-- =========================================================
-- 5. TOP 10 CATEGORIES
-- =========================================================

SELECT
    category,
    COUNT(DISTINCT show_id) AS title_count
FROM netflix_catalog.gold.content_by_category
WHERE category IS NOT NULL
GROUP BY category
ORDER BY title_count DESC
LIMIT 10;


-- =========================================================
-- 6. MOVIES VS TV SHOWS BY CATEGORY
-- =========================================================

SELECT
    category,
    type,
    COUNT(DISTINCT show_id) AS title_count
FROM netflix_catalog.gold.content_by_category
WHERE category IS NOT NULL
GROUP BY category, type
ORDER BY title_count DESC;


-- =========================================================
-- 7. CONTENT RATING DISTRIBUTION
-- =========================================================

SELECT
    rating,
    COUNT(DISTINCT show_id) AS title_count
FROM netflix_catalog.gold.content_overview
WHERE rating IS NOT NULL
GROUP BY rating
ORDER BY title_count DESC;


-- =========================================================
-- 8. MOVIE DURATION DISTRIBUTION
-- =========================================================

SELECT
    CASE
        WHEN duration_minutes < 60 THEN '< 60 min'
        WHEN duration_minutes BETWEEN 60 AND 89 THEN '60–89 min'
        WHEN duration_minutes BETWEEN 90 AND 119 THEN '90–119 min'
        WHEN duration_minutes BETWEEN 120 AND 149 THEN '120–149 min'
        WHEN duration_minutes BETWEEN 150 AND 179 THEN '150–179 min'
        WHEN duration_minutes >= 180 THEN '180+ min'
    END AS duration_bucket,

    COUNT(DISTINCT show_id) AS movie_count

FROM netflix_catalog.gold.content_overview

WHERE type = 'Movie'
  AND duration_minutes IS NOT NULL

GROUP BY
    CASE
        WHEN duration_minutes < 60 THEN '< 60 min'
        WHEN duration_minutes BETWEEN 60 AND 89 THEN '60–89 min'
        WHEN duration_minutes BETWEEN 90 AND 119 THEN '90–119 min'
        WHEN duration_minutes BETWEEN 120 AND 149 THEN '120–149 min'
        WHEN duration_minutes BETWEEN 150 AND 179 THEN '150–179 min'
        WHEN duration_minutes >= 180 THEN '180+ min'
    END;


-- =========================================================
-- 9. TOP 10 COUNTRIES
-- =========================================================

SELECT
    country,
    COUNT(DISTINCT show_id) AS title_count
FROM netflix_catalog.gold.content_by_country
WHERE country IS NOT NULL
GROUP BY country
ORDER BY title_count DESC
LIMIT 10;


-- =========================================================
-- 10. MOVIES VS TV SHOWS BY COUNTRY
-- =========================================================

SELECT
    country,
    type,
    COUNT(DISTINCT show_id) AS title_count
FROM netflix_catalog.gold.content_by_country
WHERE country IS NOT NULL
GROUP BY country, type
ORDER BY title_count DESC;


-- =========================================================
-- 11. TOP 10 CAST MEMBERS
-- =========================================================

SELECT
    cast_member,
    total_titles,
    movie_count,
    tv_show_count
FROM netflix_catalog.gold.top_cast_members
ORDER BY total_titles DESC
LIMIT 10;


-- =========================================================
-- 12. TOP 10 DIRECTORS
-- =========================================================

SELECT
    director,
    total_titles,
    movie_count,
    tv_show_count
FROM netflix_catalog.gold.top_directors
ORDER BY total_titles DESC
LIMIT 10;
