{{ config(materialized='view') }}

select
    -- identifiers
    cast(dispatching_base_num as string) as dispatching_base_num,
    cast(PUlocationid as integer) as  pickup_locationid,
    cast(DOlocationid as integer) as dropoff_locationid,
    
    -- timestamps
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropOff_datetime as timestamp) as dropoff_datetime,
    
    -- trip info
    cast(SR_Flag as decimal) as SR_Flag,
    cast(Affiliated_base_number as string) as Affiliated_base_number
from {{  source('staging', 'fhv_tripdata_partitoned') }}
