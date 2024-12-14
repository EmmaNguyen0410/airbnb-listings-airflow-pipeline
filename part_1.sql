-- Create a raw schema
create schema RAW

-- Create a CENSUS_G01_NSW_LGA table
create table RAW.CENSUS_G01_NSW_LGA (
	LGA_CODE_2016 varchar primary key,
	Tot_P_M varchar,
	Tot_P_F varchar,
	Tot_P_P varchar,
	Age_0_4_yr_M varchar,
	Age_0_4_yr_F varchar,
	Age_0_4_yr_P varchar,
	Age_5_14_yr_M varchar,
	Age_5_14_yr_F varchar,
	Age_5_14_yr_P varchar,
	Age_15_19_yr_M varchar,
	Age_15_19_yr_F varchar,
	Age_15_19_yr_P varchar,
	Age_20_24_yr_M varchar,
	Age_20_24_yr_F varchar,
	Age_20_24_yr_P varchar,
	Age_25_34_yr_M varchar,
	Age_25_34_yr_F varchar,
	Age_25_34_yr_P varchar,
	Age_35_44_yr_M varchar,
	Age_35_44_yr_F varchar,
	Age_35_44_yr_P varchar,
	Age_45_54_yr_M varchar,
	Age_45_54_yr_F varchar,
	Age_45_54_yr_P varchar,
	Age_55_64_yr_M varchar,
	Age_55_64_yr_F varchar,
	Age_55_64_yr_P varchar,
	Age_65_74_yr_M varchar,
	Age_65_74_yr_F varchar,
	Age_65_74_yr_P varchar,
	Age_75_84_yr_M varchar,
	Age_75_84_yr_F varchar,
	Age_75_84_yr_P varchar,
	Age_85ov_M varchar,
	Age_85ov_F varchar,
	Age_85ov_P varchar,
	Counted_Census_Night_home_M varchar,
	Counted_Census_Night_home_F varchar,
	Counted_Census_Night_home_P varchar,
	Count_Census_Nt_Ewhere_Aust_M varchar,
	Count_Census_Nt_Ewhere_Aust_F varchar,
	Count_Census_Nt_Ewhere_Aust_P varchar,
	Indigenous_psns_Aboriginal_M varchar,
	Indigenous_psns_Aboriginal_F varchar,
	Indigenous_psns_Aboriginal_P varchar,
	Indig_psns_Torres_Strait_Is_M varchar,
	Indig_psns_Torres_Strait_Is_F varchar,
	Indig_psns_Torres_Strait_Is_P varchar,
	Indig_Bth_Abor_Torres_St_Is_M varchar,
	Indig_Bth_Abor_Torres_St_Is_F varchar,
	Indig_Bth_Abor_Torres_St_Is_P varchar,
	Indigenous_P_Tot_M varchar,
	Indigenous_P_Tot_F varchar,
	Indigenous_P_Tot_P varchar,
	Birthplace_Australia_M varchar,
	Birthplace_Australia_F varchar,
	Birthplace_Australia_P varchar,
	Birthplace_Elsewhere_M varchar,
	Birthplace_Elsewhere_F varchar,
	Birthplace_Elsewhere_P varchar,
	Lang_spoken_home_Eng_only_M varchar,
	Lang_spoken_home_Eng_only_F varchar,
	Lang_spoken_home_Eng_only_P varchar,
	Lang_spoken_home_Oth_Lang_M varchar,
	Lang_spoken_home_Oth_Lang_F varchar,
	Lang_spoken_home_Oth_Lang_P varchar,
	Australian_citizen_M varchar,
	Australian_citizen_F varchar,
	Australian_citizen_P varchar,
	Age_psns_att_educ_inst_0_4_M varchar,
	Age_psns_att_educ_inst_0_4_F varchar,
	Age_psns_att_educ_inst_0_4_P varchar,
	Age_psns_att_educ_inst_5_14_M varchar,
	Age_psns_att_educ_inst_5_14_F varchar,
	Age_psns_att_educ_inst_5_14_P varchar,
	Age_psns_att_edu_inst_15_19_M varchar,
	Age_psns_att_edu_inst_15_19_F varchar,
	Age_psns_att_edu_inst_15_19_P varchar,
	Age_psns_att_edu_inst_20_24_M varchar,
	Age_psns_att_edu_inst_20_24_F varchar,
	Age_psns_att_edu_inst_20_24_P varchar,
	Age_psns_att_edu_inst_25_ov_M varchar,
	Age_psns_att_edu_inst_25_ov_F varchar,
	Age_psns_att_edu_inst_25_ov_P varchar,
	High_yr_schl_comp_Yr_12_eq_M varchar,
	High_yr_schl_comp_Yr_12_eq_F varchar,
	High_yr_schl_comp_Yr_12_eq_P varchar,
	High_yr_schl_comp_Yr_11_eq_M varchar,
	High_yr_schl_comp_Yr_11_eq_F varchar,
	High_yr_schl_comp_Yr_11_eq_P varchar,
	High_yr_schl_comp_Yr_10_eq_M varchar,
	High_yr_schl_comp_Yr_10_eq_F varchar,
	High_yr_schl_comp_Yr_10_eq_P varchar,
	High_yr_schl_comp_Yr_9_eq_M varchar,
	High_yr_schl_comp_Yr_9_eq_F varchar,
	High_yr_schl_comp_Yr_9_eq_P varchar,
	High_yr_schl_comp_Yr_8_belw_M varchar,
	High_yr_schl_comp_Yr_8_belw_F varchar,
	High_yr_schl_comp_Yr_8_belw_P varchar,
	High_yr_schl_comp_D_n_g_sch_M varchar,
	High_yr_schl_comp_D_n_g_sch_F varchar,
	High_yr_schl_comp_D_n_g_sch_P varchar,
	Count_psns_occ_priv_dwgs_M varchar,
	Count_psns_occ_priv_dwgs_F varchar,
	Count_psns_occ_priv_dwgs_P varchar,
	Count_Persons_other_dwgs_M varchar,
	Count_Persons_other_dwgs_F varchar,
	Count_Persons_other_dwgs_P varchar
);

-- Create a CENSUS_G02_NSW_LGA table
create table RAW.CENSUS_G02_NSW_LGA (
	LGA_CODE_2016 varchar primary key,
	Median_age_persons varchar,
	Median_mortgage_repay_monthly varchar,
	Median_tot_prsnl_inc_weekly varchar,
	Median_rent_weekly varchar,
	Median_tot_fam_inc_weekly varchar,
	Average_num_psns_per_bedroom varchar,
	Median_tot_hhd_inc_weekly varchar,
	Average_household_size varchar
);

-- Create a LISTINGS table
create table RAW.LISTINGS (
	LISTING_ID varchar,
	SCRAPE_ID varchar,
	SCRAPED_DATE varchar,
	HOST_ID varchar,
	HOST_NAME varchar,
	HOST_SINCE varchar,
	HOST_IS_SUPERHOST varchar,
	HOST_NEIGHBOURHOOD varchar,
	LISTING_NEIGHBOURHOOD varchar,
	PROPERTY_TYPE varchar,
	ROOM_TYPE varchar,
	ACCOMMODATES varchar,
	PRICE varchar,
	HAS_AVAILABILITY varchar,
	AVAILABILITY_30 varchar,
	NUMBER_OF_REVIEWS varchar,
	REVIEW_SCORES_RATING varchar,
	REVIEW_SCORES_ACCURACY varchar,
	REVIEW_SCORES_CLEANLINESS varchar,
	REVIEW_SCORES_CHECKIN varchar,
	REVIEW_SCORES_COMMUNICATION varchar,
	REVIEW_SCORES_VALUE varchar
);

-- Create a NSW_LGA_CODE table
create table RAW.NSW_LGA_CODE (
	LGA_CODE varchar primary key,
	LGA_NAME varchar
);

-- Create a NSW_LGA_SUBURB table
create table RAW.NSW_LGA_SUBURB (
	LGA_NAME varchar,
	SUBURB_NAME varchar primary key
)

-- Update dbt_valid_to for snapshot_host
update raw.snapshot_host as sh
set dbt_valid_to = next_date
from (
    select 
        host_id,
        scraped_date, 
        lead(scraped_date) over (partition by host_id order by scraped_date) as next_date
    from raw.snapshot_host
) as sh2
where sh.host_id = sh2.host_id and sh.scraped_date = sh2.scraped_date

-- Update dbt_valid_to for snapshot_room 
update raw.snapshot_room as sr
set dbt_valid_to = next_date
from (
    select 
        listing_id,
        scraped_date, 
        lead(scraped_date) over (partition by listing_id order by scraped_date) as next_date
    from raw.snapshot_room
) as sr2
where sr.listing_id = sr2.listing_id and sr.scraped_date = sr2.scraped_date

-- Update dbt_valid_to for snapshot_property
update raw.snapshot_property as sp
set dbt_valid_to = next_date
from (
    select 
        listing_id,
        scraped_date, 
        lead(scraped_date) over (partition by listing_id order by scraped_date) as next_date
    from raw.snapshot_property
) as sp2
where sp.listing_id = sp2.listing_id and sp.scraped_date = sp2.scraped_date