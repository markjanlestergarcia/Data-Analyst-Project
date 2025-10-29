-- ==========================================================
-- 🧹 SQL PROJECT: DATA CLEANING - WORLD LAYOFFS DATASET
-- ==========================================================
-- 📊 Source: https://www.kaggle.com/datasets/swaptr/layoffs-2022
-- Objective:
--   Clean and prepare global layoffs data for analysis by:
--   1. Removing duplicates
--   2. Standardizing data and fixing inconsistencies
--   3. Handling null values
--   4. Removing irrelevant data
-- ==========================================================

-- Step 1: Load and Inspect Raw Data
SELECT * 
FROM world_layoffs.layoffs;

-- Create a staging table to preserve the raw data
CREATE TABLE world_layoffs.layoffs_staging LIKE world_layoffs.layoffs;

INSERT INTO world_layoffs.layoffs_staging
SELECT * FROM world_layoffs.layoffs;


-- ==========================================================
-- 1️⃣ REMOVE DUPLICATES
-- ==========================================================

-- Identify duplicate records
SELECT *
FROM (
    SELECT company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions,
           ROW_NUMBER() OVER (
               PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions
           ) AS row_num
    FROM world_layoffs.layoffs_staging
) duplicates
WHERE row_num > 1;

-- Create a new staging table with row numbers for easier deletion
CREATE TABLE world_layoffs.layoffs_staging2 (
    company TEXT,
    location TEXT,
    industry TEXT,
    total_laid_off INT,
    percentage_laid_off TEXT,
    `date` TEXT,
    stage TEXT,
    country TEXT,
    funds_raised_millions INT,
    row_num INT
);

INSERT INTO world_layoffs.layoffs_staging2
SELECT company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions,
       ROW_NUMBER() OVER (
           PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions
       ) AS row_num
FROM world_layoffs.layoffs_staging;

-- Remove duplicates
DELETE FROM world_layoffs.layoffs_staging2
WHERE row_num > 1;


-- ==========================================================
-- 2️⃣ STANDARDIZE DATA
-- ==========================================================

-- Check for inconsistencies and nulls in 'industry'
SELECT DISTINCT industry
FROM world_layoffs.layoffs_staging2
ORDER BY industry;

-- Replace blank strings with NULLs
UPDATE world_layoffs.layoffs_staging2
SET industry = NULL
WHERE industry = '';

-- Populate missing industries using values from duplicate company names
UPDATE t1
JOIN world_layoffs.layoffs_staging2 t2
ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;

-- Standardize variations (e.g., “Crypto Currency”, “CryptoCurrency” → “Crypto”)
UPDATE world_layoffs.layoffs_staging2
SET industry = 'Crypto'
WHERE industry IN ('Crypto Currency', 'CryptoCurrency');

-- Standardize country names (remove trailing periods)
UPDATE world_layoffs.layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country);

-- Convert date column to proper DATE format
UPDATE world_layoffs.layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

ALTER TABLE world_layoffs.layoffs_staging2
MODIFY COLUMN `date` DATE;


-- ==========================================================
-- 3️⃣ HANDLE NULL VALUES
-- ==========================================================
-- Keep NULLs in numeric fields for accurate analysis
-- (e.g., total_laid_off, percentage_laid_off, funds_raised_millions)
-- No further changes necessary here.


-- ==========================================================
-- 4️⃣ REMOVE IRRELEVANT DATA
-- ==========================================================

-- Delete rows with missing key values (both total and percentage laid off are NULL)
DELETE FROM world_layoffs.layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

-- Drop helper column
ALTER TABLE world_layoffs.layoffs_staging2
DROP COLUMN row_num;


-- ==========================================================
-- ✅ FINAL CLEANED DATA
-- ==========================================================
SELECT * 
FROM world_layoffs.layoffs_staging2;

