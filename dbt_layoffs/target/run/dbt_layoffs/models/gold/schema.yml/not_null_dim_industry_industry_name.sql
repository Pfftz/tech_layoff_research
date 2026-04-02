
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select industry_name
from "tech_layoffs_dw"."public_gold"."dim_industry"
where industry_name is null



  
  
      
    ) dbt_internal_test