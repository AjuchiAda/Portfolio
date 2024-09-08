select * from layoffs;

create table layoffs_staging
like layoffs;

select * from layoffs_staging;

Insert layoffs_staging
select * from layoffs;

select *,
Row_Number() over(
partition by company, industry, total_laid_off, percentage_laid_off, `date`) AS row_num
from layoffs_staging;

with duplicate_cte as 
(
select *,
Row_Number() over(
partition by company, location, industry, total_laid_off, percentage_laid_off, `date`, stage,
country, funds_raised_millions) AS row_num
from layoffs_staging
)  
select *
from duplicate_cte;
-- where row_num > 1;

-- select * 
-- from layoffs_staging
-- where company = 'Casper'; 


create table layoffs_staging2 (
company text,
location text,
industry text,
total_laid_off int default null,
percentage_laid_off text,
`date` text,
stage text,
country text,
funds_raised_millions int default null,
row_num int
) ENGINE = InnoDB Default CHARSET=utf8mb4 Collate=utf8mb4_0900_ai_ci;


select * from layoffs_staging2;

insert into layoffs_staging2
select *,
Row_Number() over(
partition by company, location, industry, total_laid_off, percentage_laid_off, `date`, stage,
country, funds_raised_millions) AS row_num
from layoffs_staging

