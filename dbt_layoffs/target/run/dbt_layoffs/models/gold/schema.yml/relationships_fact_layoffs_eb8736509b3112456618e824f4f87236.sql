
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with child as (
    select company_id as from_field
    from "tech_layoffs_dw"."public_gold"."fact_layoffs"
    where company_id is not null
),

parent as (
    select company_id as to_field
    from "tech_layoffs_dw"."public_gold"."dim_company"
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null



  
  
      
    ) dbt_internal_test