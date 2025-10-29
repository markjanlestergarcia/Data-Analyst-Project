-- ==========================================================
-- 🧠 EXPLORATORY DATA ANALYSIS (EDA) — Global Layoffs Dataset
-- ==========================================================
-- Objective: Explore trends, patterns, and anomalies in global layoffs data (2020–2023)
-- Dataset: world_layoffs.layoffs_staging2
-- ----------------------------------------------------------


-- 🔍 1. Inspect the dataset
SELECT * 
FROM world_layoffs.layoffs_staging2
LIMIT 10;


-- 📊 2. Quick Data Overview
-- Find the maximum number of layoffs in a single record
SELECT MAX(total_laid_off) AS max_layoffs
FROM world_layoffs.layoffs_staging2;

-- Check range of percentage layoffs
SELECT 
    MAX(percentage_laid_off) AS max_percentage,
    MIN(percentage_laid_off) AS min_percentage
FROM world_layoffs.layoffs_staging2
WHERE percentage_laid_off IS NOT NULL;


-- 🏢 3. Companies with 100% Layoffs (Closed Down)
SELECT *
FROM world_layoffs.layoffs_staging2
WHERE percentage_laid_off = 1
ORDER BY funds_raised_millions DESC;
-- Insight: Mostly startups that went out of business (e.g., BritishVolt, Quibi)


-- ==========================================================
-- 📈 AGGREGATED INSIGHTS
-- ==========================================================

-- 🔹 4. Top 10 Companies with the Highest Total Layoffs
SELECT company, SUM(total_laid_off) AS total_laid_off
FROM world_layoffs.layoffs_staging2
GROUP BY company
ORDER BY total_laid_off DESC
LIMIT 10;

-- 🔹 5. Locations Most Affected by Layoffs
SELECT location, SUM(total_laid_off) AS total_laid_off
FROM world_layoffs.layoffs_staging2
GROUP BY location
ORDER BY total_laid_off DESC
LIMIT 10;

-- 🔹 6. Total Layoffs by Country
SELECT country, SUM(total_laid_off) AS total_laid_off
FROM world_layoffs.layoffs_staging2
GROUP BY country
ORDER BY total_laid_off DESC;

-- 🔹 7. Yearly Layoff Trends
SELECT YEAR(date) AS year, SUM(total_laid_off) AS total_laid_off
FROM world_layoffs.layoffs_staging2
GROUP BY YEAR(date)
ORDER BY year ASC;

-- 🔹 8. Industry with the Most Layoffs
SELECT industry, SUM(total_laid_off) AS total_laid_off
FROM world_layoffs.layoffs_staging2
GROUP BY industry
ORDER BY total_laid_off DESC;

-- 🔹 9. Layoffs by Company Stage (Startup, Post-IPO, etc.)
SELECT stage, SUM(total_laid_off) AS total_laid_off
FROM world_layoffs.layoffs_staging2
GROUP BY stage
ORDER BY total_laid_off DESC;


-- ==========================================================
-- 🧮 ADVANCED ANALYSIS
-- ==========================================================

-- 🔸 10. Top 3 Companies with Most Layoffs Per Year
WITH Company_Year AS (
  SELECT company, YEAR(date) AS year, SUM(total_laid_off) AS total_laid_off
  FROM world_layoffs.layoffs_staging2
  GROUP BY company, YEAR(date)
),
Company_Year_Rank AS (
  SELECT company, year, total_laid_off,
         DENSE_RANK() OVER (PARTITION BY year ORDER BY total_laid_off DESC) AS rank
  FROM Company_Year
)
SELECT company, year, total_laid_off, rank
FROM Company_Year_Rank
WHERE rank <= 3
AND year IS NOT NULL
ORDER BY year ASC, total_laid_off DESC;


-- 🔸 11. Rolling Monthly Total of Layoffs
WITH Monthly_Layoffs AS (
  SELECT DATE_FORMAT(date, '%Y-%m') AS month, SUM(total_laid_off) AS total_laid_off
  FROM world_layoffs.layoffs_staging2
  GROUP BY month
)
SELECT month, 
       SUM(total_laid_off) OVER (ORDER BY month ASC) AS rolling_total_layoffs
FROM Monthly_Layoffs
ORDER BY month ASC;

-- ==========================================================
-- 📍 END OF EDA SCRIPT
-- ----------------------------------------------------------
-- Insights Generated:
-- - Startups had the highest closure rates (100% layoffs)
-- - Tech and Finance sectors saw peak layoffs in 2022
-- - U.S. and India were the most affected countries
-- - Major layoff waves aligned with global economic slowdowns
-- ==========================================================
