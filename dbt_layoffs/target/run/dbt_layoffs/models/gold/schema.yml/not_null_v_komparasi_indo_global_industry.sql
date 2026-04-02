
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select industry
from "tech_layoffs_dw"."public_gold"."v_komparasi_indo_global"
where industry is null



  
  
      
    ) dbt_internal_test