-- Data Cleaning

SELECT *
FROM world_layofss;

--  1. Remove Duplicates if any
--  2. Standardize the Data
--  3. Null Values or Blank Values
--  4. Remove any columns or rows if neccessary

DROP TABLE IF EXISTS layoffs_staging;

CREATE TABLE layoffs_staging
LIKE world_layofss;


INSERT layoffs_staging
SELECT *
FROM world_layofss;

SELECT *
FROM layoffs_staging;

WITH cets AS
(
SELECT *,
ROW_NUMBER() OVER(PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging
) 
SELECT *
FROM cets;

DROP TABLE IF EXISTS layoffs_staging2;

CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

SELECT *
FROM layoffs_staging2;

INSERT INTO layoffs_staging2
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging;

DELETE 
FROM layoffs_staging2
WHERE row_num>1;

SELECT *
FROM layoffs_staging2;
--  Now how to remove the duplicates the duplicats



