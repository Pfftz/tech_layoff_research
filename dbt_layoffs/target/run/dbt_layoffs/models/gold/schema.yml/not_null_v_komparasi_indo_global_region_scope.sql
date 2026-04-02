
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select region_scope
from "tech_layoffs_dw"."public_gold"."v_komparasi_indo_global"
where region_scope is null



  
  
      
    ) dbt_internal_test