-- Data Cleaning

https://www.kaggle.com/datasets/swaptr/layoffs-2022?resource=download

SELECT *
FROM layoffs;

-- 1. Remove Duplicates - remove any duplicate data in the database
-- 2. Standardize the data - check for any issues like spelling etc. 
-- 3. Look for Null values or Blank values - double check the rows with blank or null values
-- 4. Remove unnecessary columns - removing any columns which are not needed so we can focus on what is only needed for our update.

CREATE TABLE layoffs_staging
LIKE layoffs;
-- We will create a new table so our "layoffs" table will still be our raw data table. In this case, all changes and updates will be made in our new table,
-- which is the layoffs_staging. This is a critical part because in real world update we will be doing updates not in the raw data but in the secondary data.

SELECT *
FROM layoffs_staging;

INSERT layoffs_staging
SELECT * 
FROM layoffs;

-- Start the update by removing duplicates. 

-- Start by creating a unique id for each column

SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, total_laid_off, `date`, percentage_laid_off, industry, `source`, stage, funds_raised, country, date_added) AS row_num
FROM layoffs_staging;

-- Check for columns that have row number greater than 1
WITH duplicate_cte AS 
(
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, total_laid_off, `date`, percentage_laid_off, industry, `source`, stage, funds_raised, country, date_added) AS row_num
FROM layoffs_staging
)
SELECT *
FROM duplicate_cte
WHERE row_num > 1;

-- Spot check on some companies to check if the duplicate values.
SELECT * 
FROM layoffs_staging
WHERE company = 'GupShup';

WITH duplicate_cte AS 
(
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, total_laid_off, `date`, percentage_laid_off, industry, `source`, stage, funds_raised, country, date_added) AS row_num
FROM layoffs_staging
)
DELETE 
FROM duplicate_cte
WHERE row_num > 1;

-- Create another table to make some changes like adding the row number and the deletion of the duplicates
CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `total_laid_off` double DEFAULT NULL,
  `date` text,
  `percentage_laid_off` text,
  `industry` text,
  `source` text,
  `stage` text,
  `funds_raised` text,
  `country` text,
  `date_added` text,
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Check for the duplicates
SELECT * 
FROM layoffs_staging2
WHERE row_num > 1;

-- This query is inserting data into a staging table while adding a row number to help identify duplicates
INSERT INTO layoffs_staging2
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, total_laid_off, `date`, percentage_laid_off, industry, `source`, stage, funds_raised, country, date_added) AS row_num
FROM layoffs_staging;

-- Delete all values that row number equals greater than 1.
DELETE  
FROM layoffs_staging2
WHERE row_num > 1;

-- Check if there are items greater than 1 
SELECT * 
FROM layoffs_staging2
WHERE row_num > 1;

-- Check for the location and if needed standardized any inconsistent data.
SELECT DISTINCT location 
FROM layoffs_staging2
ORDER BY 1;

-- Check for the country and standardized any inconsistent data.
SELECT DISTINCT country 
FROM layoffs_staging2
ORDER BY 1;


SELECT company, TRIM(company)
FROM layoffs_staging2;

-- Next is to update the table by removing any leading or trailing spaces from the company column. This will set consistency across the data.
UPDATE layoffs_staging2
SET company = TRIM(company);

-- Check for the distinct value for the industry. If all is well with the data, we can move on to other updates.
SELECT DISTINCT industry
FROM layoffs_staging2
ORDER BY 1;

SELECT * 
FROM layoffs_staging2;


-- Standardized the date from text to date format by using string to date

SELECT `date`
FROM layoffs_staging2;

-- by using string to date function we would be able to change the date format from text to date. Just make sure to follow below format in changing text values to date.
SELECT `date`,
STR_TO_DATE(`date`, '%m/%d/%Y')
FROM layoffs_staging2;

-- Use the UPDATE function to set the changes in our staging table.
UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

-- Change the data type of date column in the staging table to appear a proper date format. 
-- Note that we can only use the alter function in the staging table and not in raw table.
ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;

-- Check the null and blank values

SELECT *
FROM layoffs_staging2;

SELECT * 
FROM layoffs_staging2
WHERE country = ' ';

SELECT * 
FROM layoffs_staging2
WHERE industry IS NULL;

-- Delete the column row_num using the Drop function

SELECT * 
FROM layoffs_staging2;

-- Remove the row_num column 
ALTER TABLE layoffs_staging2
DROP COLUMN row_num;
