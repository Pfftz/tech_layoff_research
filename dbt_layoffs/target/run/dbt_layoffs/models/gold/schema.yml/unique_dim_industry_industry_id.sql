
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

select
    industry_id as unique_field,
    count(*) as n_records

from "tech_layoffs_dw"."public_gold"."dim_industry"
where industry_id is not null
group by industry_id
having count(*) > 1



  
  
      
    ) dbt_internal_test