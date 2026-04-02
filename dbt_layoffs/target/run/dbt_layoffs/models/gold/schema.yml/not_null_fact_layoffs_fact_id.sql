
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select fact_id
from "tech_layoffs_dw"."public_gold"."fact_layoffs"
where fact_id is null



  
  
      
    ) dbt_internal_test