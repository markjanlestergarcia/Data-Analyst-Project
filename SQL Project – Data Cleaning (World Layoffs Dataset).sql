-- =====================================================
-- SQL Data Cleaning Project: Global Layoffs Dataset
-- Source: https://www.kaggle.com/datasets/swaptr/layoffs-2022
-- =====================================================

-- STEP 0: Create a staging table (to preserve raw data)
CREATE TABLE world_layoffs.layoffs_staging LIKE world_layoffs.layoffs;
INSERT INTO world_layoffs.layoffs_staging SELECT * FROM world_layoffs.layoffs;

-- =====================================================
-- STEP 1: Remove Duplicates
-- =====================================================

-- Identify duplicates using ROW_NUMBER
CREATE TABLE world_layoffs.layoffs_staging2 AS
SELECT *,
       ROW_NUMBER() OVER (
            PARTITION BY company, location, industry, total_laid_off,
                         percentage_laid_off, `date`, stage, country, funds_raised_millions
            ORDER BY company
       ) AS row_num
FROM world_layoffs.layoffs_staging;

-- Keep only unique rows
DELETE FROM world_layoffs.layoffs_staging2
WHERE row_num > 1;

-- Remove helper column
ALTER TABLE world_layoffs.layoffs_staging2 DROP COLUMN row_num;

-- =====================================================
-- STEP 2: Standardize Data
-- =====================================================

-- Replace blank industries with NULL
UPDATE world_layoffs.layoffs_staging2
SET industry = NULL
WHERE industry = '';

-- Populate missing industries using other rows of the same company
UPDATE t1
JOIN world_layoffs.layoffs_staging2 t2
  ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL
  AND t2.industry IS NOT NULL;

-- Standardize "Crypto" variations
UPDATE world_layoffs.layoffs_staging2
SET industry = 'Crypto'
WHERE industry IN ('Crypto Currency', 'CryptoCurrency');

-- Standardize country names (remove trailing periods)
UPDATE world_layoffs.layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country);

-- Fix date format
UPDATE world_layoffs.layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

ALTER TABLE world_layoffs.layoffs_staging2
MODIFY COLUMN `date` DATE;

-- =====================================================
-- STEP 3: Handle Nulls
-- =====================================================
-- Keep NULLs in numeric columns for accuracy in EDA
-- No changes needed

-- =====================================================
-- STEP 4: Remove Unnecessary Data
-- =====================================================

-- Delete rows with no useful layoff data
DELETE FROM world_layoffs.layoffs_staging2
WHERE total_laid_off IS NULL
  AND percentage_laid_off IS NULL;

-- =====================================================
-- Final Cleaned Data
-- =====================================================
SELECT *
FROM world_layoffs.layoffs_staging2;
