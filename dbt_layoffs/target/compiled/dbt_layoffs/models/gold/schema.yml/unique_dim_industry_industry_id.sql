
    
    

select
    industry_id as unique_field,
    count(*) as n_records

from "tech_layoffs_dw"."public_gold"."dim_industry"
where industry_id is not null
group by industry_id
having count(*) > 1


