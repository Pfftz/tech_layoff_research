
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select company_id
from "tech_layoffs_dw"."public_gold"."dim_company"
where company_id is null



  
  
      
    ) dbt_internal_test