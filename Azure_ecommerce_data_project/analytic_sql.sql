-- Create delta table from path
CREATE OR REPLACE TABLE ecom_one_big_table
SELECT *
FROM delta.`/mnt/delta/tables/gold/ecom_one_big_table`;


-- Country-specific seller dominance
SELECT distinct(Country), Countries_Sellers, Countries_TopSellers,
       Round(((Countries_TopSellers / Countries_Sellers) * 100)) AS TopSellerPercentage
FROM ecom_one_big_table
where country is not null
ORDER BY TopSellerPercentage DESC;

-- Gender distribution in sellers
select distinct(Country), countries_femaleSellers, Countries_MaleSellers, countries_topFemaleSellers, countries_topMaleSellers,
ROUND((countries_FemaleSellers/ (Countries_FemaleSellers + Countries_MaleSellers)) * 100) as FemaleSellerPercentage,
ROUND((countries_MaleSellers / (Countries_FemaleSellers + Countries_MaleSellers)) * 100) as MaleSellerPercentage
from ecom_one_big_table
where country is not null
order by Countries_FemaleSellers desc, Countries_MaleSellers desc;

-- top buyers vs general buyers
select distinct(Country), buyers_total, Round((buyers_top/buyers_total) * 100) as TopBuyerPercentage
from ecom_one_big_table
where country is not null
order by TopBuyerPercentage DESC;

-- gender distribution in buyers
select distinct(Country), buyers_male, Buyers_TopFemale, Buyers_TopMale,
ROUND((Buyers_Female/Buyers_Total) * 100) as femaleBuyerPercentage,
ROUND((Buyers_male/Buyers_Total) * 100) as maleBuyerPercentage,
ROUND((Buyers_TopFemale/Buyers_Top) * 100) as topFemaleBuyerPercentage,
ROUND((Buyers_TopFemale/Buyers_Top) * 100) as topMaleBuyerPercentage
from ecom_one_big_table
where country is not null;

-- Product engagement by users
select Country, Round(AVG(Users_productsSold),2) as avgProductsSold, Round(avg(User_productsWished),2) as AvgProductsWished
from ecom_one_big_table
group by country
order by avgProductsSold desc, AvgProductsWished desc;

-- Impact of social media
select country, 
  ROUND(avg(user_socialnbfollowers), 2) as AvgFollowers,
  ROUND(avg(users_productsSold), 2) as AvgProductsSold,
  ROUND(avg(User_productsWished), 2) as AvgProductsWished
from ecom_one_big_table
where country is not null
group by country
HAVING AvgFollowers > 0;

-- Age group buying patterns
select User_account_age_group, 
Round(avg(users_productsSold), 2) as AvgProductsSold,
Round(AVG(User_productsWished), 2) as AvgProductsWished
from ecom_one_big_table
where country is not null
group by User_account_age_group;

-- App Usage Trends
select User_hasanyapp,
  ROUND(avg(Users_productsSold),2) as AvgProdustsSold,
  ROUND(avg(User_productsWished),2) as AvgProductsWished,
  ROUND(avg(User_socialnbfollowers),2) as AvgFollowers
from ecom_one_big_table
where country is not null
group by User_hasanyapp
order by User_hasanyapp desc;

-- average seller performance
select country, 
  ROUND(avg(Sellers_MeanProductsSold), 2) as AvgProductsSold,
  ROUND(avg(Sellers_MeanProductsListed), 2) as AvgProductsListed
from ecom_one_big_table
where country is not null
group by country
order by AvgProductsSold desc, AvgProductsListed desc;

-- user flag impact
select User_flag_long_title, 
ROUND(avg(Users_productsSold), 2) as AvgProductsSold, 
ROUND(avg(User_socialnbfollowers), 2) as AvgFollowers
from ecom_one_big_table
where country is not null
group by user_flag_long_title;