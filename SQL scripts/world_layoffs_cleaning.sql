-- Data cleaning, my first project:)
-- 1. check for duplicates and remove any
-- 2. standardize data and fix errors
-- 3. Look at null values  
-- 4. remove any columns and rows that are not necessary 

-- data profile
describe layoffs;
select * from layoffs;

-- create data staging (copy)
create table layoffs_staging like layoffs;
insert layoffs_staging select * from layoffs;
select * from layoffs_staging;

-- 1. check for duplicates and remove any

SELECT *
FROM world_layoffs.layoffs
;

ALTER TABLE layoffs_staging
ADD COLUMN id INT AUTO_INCREMENT PRIMARY KEY;

with duplicate_cte as 
(
select *, row_number() over (
partition by company, location, `date`,total_laid_off, stage, industry, country,funds_raised ORDER BY id) as row_num 
from layoffs_staging) select * from duplicate_cte where row_num > 1;



SELECT id, company, location, `date`, total_laid_off, stage, industry, country, funds_raised, source, date_added, row_num
FROM (
    SELECT id, company, location, `date`, total_laid_off, stage, industry, country, funds_raised, source, date_added,
           ROW_NUMBER() OVER (
             PARTITION BY company, location, `date`, total_laid_off, stage, industry, country, funds_raised
             ORDER BY id
           ) AS row_num
    FROM layoffs_staging
) AS t
WHERE row_num > 1;

DELETE FROM layoffs_staging
WHERE id IN (
    SELECT id
    FROM (
        SELECT id,
               ROW_NUMBER() OVER (
                   PARTITION BY company, location, `date`, total_laid_off, stage, industry, country, funds_raised
                   ORDER BY id
               ) AS row_num
        FROM layoffs_staging
    ) t
    WHERE row_num > 1
);

with duplicate_cte as 
(
select *, row_number() over (
partition by company, location, `date`,total_laid_off, stage, industry, country,funds_raised ORDER BY id) as row_num 
from layoffs_staging) select * from duplicate_cte where row_num > 1;

select * from layoffs_staging;

ALTER TABLE layoffs_staging
DROP COLUMN id;

select * from layoffs_staging;

-- removing duplictes done:)

-- next step is standardize the data 

select company, trim(company) from layoffs_staging;
update layoffs_staging set company = trim(company);

select distinct industry from layoffs_staging order by 1;
select distinct location from layoffs_staging order by 1;
UPDATE layoffs_staging
SET location = TRIM(location);

-- non US added to the location and should be removed 
update layoffs_staging set location = replace(location,',Non-U.S.', '');

SELECT DISTINCT location
FROM layoffs_staging
ORDER BY location;

-- encoding issue like FÃ¸rde,DÃ¼sseldorf, FlorianÃ³polis, MalmÃ¶, WrocÅ‚aw
UPDATE layoffs_staging
SET location = CONVERT(CAST(location AS BINARY) USING utf8mb4);

SELECT DISTINCT location
FROM layoffs_staging
ORDER BY location;

-- i still see the same issue FÃ¸rde,DÃ¼sseldorf, FlorianÃ³polis, MalmÃ¶, WrocÅ‚aw
-- Düsseldorf
UPDATE layoffs_staging
SET location = REPLACE(location, 'DÃ¼sseldorf', 'Düsseldorf');

-- Malmö
UPDATE layoffs_staging
SET location = REPLACE(location, 'MalmÃ¶', 'Malmö');

-- Wrocław
UPDATE layoffs_staging
SET location = REPLACE(location, 'WrocÅ‚aw', 'Wrocław');

-- Florianópolis
UPDATE layoffs_staging
SET location = REPLACE(location, 'FlorianÃ³polis', 'Florianópolis');

-- Førde
UPDATE layoffs_staging
SET location = REPLACE(location, 'FÃ¸rde', 'Førde');

-- some location has two cityes Luxembourg,Raleigh, Melbourne,Victoria, New Delhi,New York City
SELECT location
FROM layoffs_staging
WHERE location LIKE '%,%';

UPDATE layoffs_staging
SET location = SUBSTRING_INDEX(location, ',', 1)
WHERE location LIKE '%,%';

SELECT DISTINCT location
FROM layoffs_staging order by 1;

-- company and location done 

select distinct country from layoffs_staging;

UPDATE layoffs_staging
SET country = 'United Arab Emirates'
WHERE country = 'UAE';

-- country done now i want to change the date becouse the coulmn is text and it will be an issue if i want to create time series analsis later on
select distinct `date` from layoffs_staging;
update layoffs_staging set `date` = str_to_date(date, '%m/%d/%Y');
alter table layoffs_staging modify COLUMN `date` DATE ;

-- date done 

select distinct funds_raised from layoffs_staging;

-- i will reomve the $ icon and change the coulmn to be a number not text 
UPDATE layoffs_staging
SET funds_raised = REPLACE(funds_raised, '$', '');

UPDATE layoffs_staging
SET funds_raised = NULL
WHERE funds_raised = '';

ALTER TABLE layoffs_staging
MODIFY COLUMN funds_raised INT;

-- the null values in all coulmns
SELECT
    SUM(CASE WHEN company IS NULL OR company = '' THEN 1 ELSE 0 END) AS company_missing,
    SUM(CASE WHEN location IS NULL OR location = '' THEN 1 ELSE 0 END) AS location_missing,
    SUM(CASE WHEN industry IS NULL OR industry = '' THEN 1 ELSE 0 END) AS industry_missing,
    SUM(CASE WHEN total_laid_off IS NULL OR total_laid_off = '' THEN 1 ELSE 0 END) AS total_laid_off_missing,
    SUM(CASE WHEN stage IS NULL OR stage = '' THEN 1 ELSE 0 END) AS stage_missing,
    SUM(CASE WHEN country IS NULL OR country = '' THEN 1 ELSE 0 END) AS country_missing,
    SUM(CASE WHEN funds_raised IS NULL OR funds_raised = '' THEN 1 ELSE 0 END) AS funds_raised_missing
FROM layoffs_staging;

-- checking null for coulmn that can be populated by another 
select * from layoffs_staging t1 join layoffs_staging t2 
on t1.company =t2.company 
where (t1.country is null or t1.country ='') and t2.country is not null;
-- Berlin and Mont do not have country it should be Germany and canada 
update layoffs_staging set country ='Germany' where 
(location ='berlin' or location ='Berlin') and (country is null or country ='');
update layoffs_staging set country ='Canada' where 
(location ='Montreal' or location ='montreal') and (country is null or country ='');
-- no null values now in country
-- total laid off and funds raised those can not be populated from another rows so i will keep them null for now

-- now i want to remove the row that will not help my project 
-- i want to know the total laid off and the % laid off so the rows that does not have values will be removed 

select count(*) as missing from layoffs_staging where (total_laid_off is null or total_laid_off = '') 
and (percentage_laid_off is null or percentage_laid_off = '');

-- there is about 887 rows which is alot of data so i'm not 100% sure if i should remove them but in the anylsis phase they are useless to me
Delete from layoffs_staging where (total_laid_off is null or total_laid_off = '') 
and (percentage_laid_off is null or percentage_laid_off = '');

select * from layoffs_staging where (total_laid_off is null or total_laid_off = '') ;

















