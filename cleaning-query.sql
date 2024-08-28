-- CLEAN DATA

SELECT * FROM shrink_data.layoffs;

-- 1. Remove duplicates
-- 2. Standardize data (like fix misspleing)
-- 3. Fix null values or blanck values
-- 4. Remove unnecessary columns or rows

create table data_staging
like layoffs;

select * from data_staging;

insert data_staging
select * 
from layoffs;

select *,
row_number() over(partition by company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, funds_raised_millions) as row_num
from data_staging;

with duplicate_cte as (
select *,
row_number() over(partition by company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, funds_raised_millions) as row_num
from data_staging
)
select *
from duplicate_cte
where row_num > 1;

CREATE TABLE `data_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

select * from data_staging2;

insert into data_staging2
select *,
row_number() over(partition by company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, funds_raised_millions ) as row_num
from data_staging;

delete
from data_staging2
where row_num > 1;

-- 2. Standardize data (like fix misspleing)

select company from data_staging2;

select company, trim(company) from data_staging2;

update data_staging2
set company = trim(company);

select industry
from data_staging2
group by industry
order by industry;

select industry
from data_staging2
where industry like 'Crypto%';

update data_staging2
set industry = 'Crypto'
where industry like 'Crypto%';

select country
from data_staging2
order by 1;

select country
from data_staging2
where country like 'united states%';

select distinct country, trim(trailing '.' from country)
from data_staging2
where country like 'united states%';

update data_staging2
set country = trim(trailing '.' from country);

select `date`
from data_staging2;

select `date`, str_to_date(`date`, '%m/%d/%Y')
from data_staging2;

update data_staging2
set `date` = str_to_date(`date`, '%m/%d/%Y');

alter table data_staging2
modify column `date` date;

-- 3. Fix null values or blanck values

select *
from data_staging2
where industry is null
or industry = '';

update data_staging2
set industry = null
where industry = '';

select industry
from data_staging2;

select t1.industry, t2.industry
from data_staging2 t1
join data_staging2 t2
	on t1.company = t2.company
where (t1.industry is null or t2.industry = '')
	and t2.industry is not null;

update data_staging2 t1
join data_staging2 t2
	on t1.company = t2.company
set t1.industry = t2.industry
where t1.industry is null
	and t2.industry is not null;

-- 4. Remove unnecessary columns or rows

select *
from data_staging2;

alter table data_staging2
drop column row_num;

