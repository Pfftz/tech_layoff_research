
    
    

select
    fact_id as unique_field,
    count(*) as n_records

from "tech_layoffs_dw"."public_gold"."fact_layoffs"
where fact_id is not null
group by fact_id
having count(*) > 1


