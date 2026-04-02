
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select industry_id
from "tech_layoffs_dw"."public_gold"."fact_layoffs"
where industry_id is null



  
  
      
    ) dbt_internal_test