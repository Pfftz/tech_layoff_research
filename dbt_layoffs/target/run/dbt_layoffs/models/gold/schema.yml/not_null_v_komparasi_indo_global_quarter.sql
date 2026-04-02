
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select quarter
from "tech_layoffs_dw"."public_gold"."v_komparasi_indo_global"
where quarter is null



  
  
      
    ) dbt_internal_test