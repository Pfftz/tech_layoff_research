
    
    

select
    company_id as unique_field,
    count(*) as n_records

from "tech_layoffs_dw"."public_gold"."dim_company"
where company_id is not null
group by company_id
having count(*) > 1


