CREATE DATABASE world_layoffs;

USE world_layoffs;

SELECT *
FROM layoffs;

-- DUPLICATE REMOVAL
-- STANDARDIZATION
-- NULL VALUES
-- REMOVE COLUMNS

CREATE TABLE layoffs_staging
LIKE layoffs;

SELECT *
FROM layoffs_staging;

INSERT layoffs_staging
SELECT *
FROM Layoffs;

SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`) AS row_num
FROM layoffs_staging;

WITH duplicate_cte AS
(
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging
)
SELECT *
FROM duplicate_cte
WHERE row_num > 1;

SELECT *
FROM layoffs_staging
WHERE company = 'Casper';


CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` text,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` text,
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

SELECT *
FROM layoffs_staging2
WHERE row_num > 1;

INSERT INTO layoffs_staging2
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging;

DELETE
FROM layoffs_staging2
WHERE row_num > 1;

SELECT *
FROM layoffs_staging2;

-- COMPANY
SELECT DISTINCT(company)
FROM layoffs_staging2
ORDER BY 1;

SELECT DISTINCT company, TRIM(company)
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET company = TRIM(company);

-- LOCATION

SELECT DISTINCT(location)
FROM layoffs_staging2
ORDER BY 1;

SELECT DISTINCT location, TRIM(location)
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET location = TRIM(location);

-- INDUSTRY
SELECT DISTINCT(industry)
FROM layoffs_staging2
ORDER BY 1;

SELECT *
FROM layoffs_staging2
WHERE industry LIKE 'Crypto%';

UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

-- COUNTRY

SELECT DISTINCT(country)
FROM layoffs_staging2
ORDER BY 1;

SELECT DISTINCT country, TRIM(TRAILING '.' FROM country)
FROM layoffs_staging2
ORDER BY 1;

UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country);

SELECT DISTINCT country
FROM layoffs_staging2
ORDER BY 1;

-- DATE

SELECT `date`,
STR_TO_DATE(`date`, '%m/%d/%Y')
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET `date` = NULL
WHERE `date` = 'NULL';

UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y')
WHERE `date`;

SELECT DISTINCT `date`
FROM layoffs_staging2;

ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;

-- NULLS

-- TOTAL LAID OFF NULLS

SELECT DISTINCT total_laid_off
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET total_laid_off = NULL
WHERE total_laid_off = 'NULL';

ALTER TABLE layoffs_staging2
MODIFY COLUMN total_laid_off INT;

SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL;

-- PERCENTAGE LAID OFF NULLS

SELECT DISTINCT percentage_laid_off
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET percentage_laid_off = NULL
WHERE percentage_laid_off = 'NULL';

ALTER TABLE layoffs_staging2
MODIFY COLUMN percentage_laid_off DECIMAL(8,4);


-- PERCENT AND TOTAL NULLS

SELECT *
FROM layoffs_staging2
WHERE percentage_laid_off IS NULL
AND total_laid_off IS NULL;

DELETE
FROM layoffs_staging2
WHERE percentage_laid_off IS NULL
AND total_laid_off IS NULL;

SELECT *
FROM layoffs_staging2;

ALTER TABLE layoffs_staging2
DROP COLUMN row_num;

-- INDUSTRY NULLS

SELECT DISTINCT industry
FROM layoffs_staging2;


SELECT *
FROM layoffs_staging2
WHERE industry IS NULL
OR industry = 'NULL'
OR industry = '';

SELECT *
FROM layoffs_staging2
WHERE company = 'Airbnb';

-- JOIN TO RESOLVE EMPTY INDUSTRIES
SELECT t1. company, t1.location, t1.industry, t2.industry
FROM layoffs_staging2 AS t1
JOIN layoffs_staging2 AS t2
	ON t1.company = t2.company
    AND t1.location = t2.location
WHERE (t1.industry = '' OR t1.industry IS NULL)
AND t2.industry IS NOT NULL;


-- UPDATE ALTERNATIVE?

UPDATE layoffs_staging2
SET industry = NULL
WHERE industry IS NULL
OR industry = 'NULL'
OR industry = '';


UPDATE layoffs_staging2 AS t1
JOIN layoffs_staging2 AS t2
	ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;


-- PRINT

SELECT DISTINCT industry
FROM layoffs_staging2;



