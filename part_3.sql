-- 1 
with best_listing_neighbourhood as 
(
	select 
		listing_neighbourhood,
		sum(avg_estimated_revenue_per_active_listings) as estimated_revenue_per_active_listings
	from datamart.dm_listing_neighbourhood dln 
	group by listing_neighbourhood 
	order by sum (avg_estimated_revenue_per_active_listings) desc
	limit 1
),
worst_listing_neighbourhood as 
(
	select 
		listing_neighbourhood,
		sum(avg_estimated_revenue_per_active_listings) as estimated_revenue_per_active_listings
	from datamart.dm_listing_neighbourhood dln 
	group by listing_neighbourhood 
	order by sum(avg_estimated_revenue_per_active_listings) asc 
	limit 1
),
best_worst_listing_neighbourhood as 
(
	select 
		*
	from 
	best_listing_neighbourhood
	union 
	select 
		*
	from 
	worst_listing_neighbourhood
),
lga_code_best_worst as (
    select 
        lga_code, 
        listing_neighbourhood,
        estimated_revenue_per_active_listings
    from best_worst_listing_neighbourhood as best_worst 
    left join warehouse.dim_lga lga on best_worst.listing_neighbourhood = lga.lga_name
)
select 
    best_worst.lga_code, 
    listing_neighbourhood,
    estimated_revenue_per_active_listings as estimated_revenue,
    total_population,
    (age_0_4_yr_total + age_5_14_yr_total) as age_0_14_pop,
    (age_15_19_yr_total + age_20_24_yr_total) as age_15_24_pop,
    (age_25_34_yr_total + age_35_44_yr_total) as age_25_44_pop, 
    (age_45_54_yr_total + age_55_64_yr_total) as age_45_64_pop,
    (age_65_74_yr_total + age_75_84_yr_total + age_85_over_total) as age_65_over_pop
from lga_code_best_worst as best_worst
left join warehouse.dim_g01 dim_g01 on best_worst.lga_code = dim_g01.lga_code


--2 
with top_5_listing_neighbourhood as 
(
    select listing_neighbourhood
    from datamart.dm_listing_neighbourhood
    group by listing_neighbourhood
    order by sum(avg_estimated_revenue_per_active_listings) desc 
    limit 5
),
partitioned_listings as 
(
    select 
        listing_neighbourhood,
        property_type, 
        room_type, 
        accommodates,
        sum(case when has_availability = TRUE then 30 - availability_30 else null end) as total_number_of_stays,
        row_number() over (partition by listing_neighbourhood order by sum(case when has_availability = TRUE then 30 - availability_30 else null end) desc) as rk
    from warehouse.facts_listings where listing_neighbourhood in (select listing_neighbourhood from top_5_listing_neighbourhood)
    group by 1,2,3,4 
    order by listing_neighbourhood, total_number_of_stays desc 
)
select * 
from partitioned_listings where rk = 1
order by total_number_of_stays desc


-- 3
with listing_host_lga as 
(
    select 
        listing_id,
        listing_neighbourhood_lga_code,
        host_id, 
        host_neighbourhood_lga_code
    from warehouse.facts_listings 
),
hosts_with_multiple_listings as
(
    select 
        host_id,
        count(listing_id) as count_listings
    from listing_host_lga
    group by host_id 
    having count(distinct listing_id)  > 1
),
hosts_listings_same_lga as 
(
    select
        host_id, 
        count(listing_id) as count_listings_host_same_lga
    from listing_host_lga
    where host_neighbourhood_lga_code = listing_neighbourhood_lga_code and host_id in (select host_id from hosts_with_multiple_listings)
    group by host_id
)
select 
    avg(count_listings_host_same_lga / count_listings * 100)
from hosts_with_multiple_listings as all_listings inner join hosts_listings_same_lga as same on all_listings.host_id = same.host_id

-- 4 
with hosts_with_unique_listing as 
(
    select 
        host_id, 
        count(distinct listing_id) as count_listings
    from warehouse.facts_listings
    group by host_id
    having count(distinct listing_id) = 1
),
median_mortgage_repayment_annually_tab as
(
    select 
        lga_code,
        median_mortgage_repay_monthly * 12 as median_mortgage_repayment_annually
    from warehouse.dim_g02
),
estimated_revenue_annually_tab as 
(
    select 
        host_id, 
        listing_neighbourhood_lga_code,
        sum(case when has_availability = TRUE then (30 - availability_30)*price else null end) as estimated_revenue_annually
    from warehouse.facts_listings 
    where host_id in (select host_id from hosts_with_unique_listing)
    group by 1,2 
),
revenue_same_mortgage as 
(
    select 
        host_id, 
        listing_neighbourhood_lga_code,
        estimated_revenue_annually,
        median_mortgage_repayment_annually
    from estimated_revenue_annually_tab as estimated_revenue
    left join median_mortgage_repayment_annually_tab as median_mortgage on estimated_revenue.listing_neighbourhood_lga_code = median_mortgage.lga_code
)

select (
	(
	   select 
	        count(distinct host_id) as count_hosts_cover_mortgage
	   from revenue_same_mortgage
		where estimated_revenue_annually >= median_mortgage_repayment_annually
	) :: numeric /
(
		select 
			count(distinct host_id) as count_hosts_unique_listings
		from hosts_with_unique_listing
	) :: numeric
) *100 as percentage_hosts_cover_mortgage