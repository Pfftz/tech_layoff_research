
    
    

with child as (
    select industry_id as from_field
    from "tech_layoffs_dw"."public_gold"."fact_layoffs"
    where industry_id is not null
),

parent as (
    select industry_id as to_field
    from "tech_layoffs_dw"."public_gold"."dim_industry"
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


