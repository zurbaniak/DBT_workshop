select
    rentalorders.carid,
    cars.make,
    cars.model,
    rentalorders.mileagestart,
    rentalorders.mileageend,
    rentalcategories.car_category
from dbt_workshop.car_rental.rentalorders
inner join (
select
cars.carid as car_id,
cars.rentalcategoriesid,
cars.make,
cars.model
from dbt_workshop.car_rental.cars
) cars
on rentalorders.carid = cars.car_id

inner join (
    select
        rentalcategories.rentalcategoriesid as rentalcategories_id,
        rentalcategories.category,
        case when round(rentalcategories.monthlyrentprice,2) = 300.0 then 'car category 1'
        when round(rentalcategories.monthlyrentprice,2) = 400.0 then 'car category 2'
        when round(rentalcategories.monthlyrentprice,2) = 500.0 then 'car category 3'
        else 'car category 4'
        end as car_category,
        round(rentalcategories.monthlyrentprice,2) as monthly_rent_price
    from dbt_workshop.car_rental.rentalcategories
) rentalcategories 
on cars.rentalcategoriesid = rentalcategories.rentalcategories_id


WITH
rentalorders AS (
    SELECT
        rentalorders.rentalorderid AS rentalorder_id,
        rentalorders.customerid AS customer_id,
        rentalorders.carid AS car_id,
        rentalorders.orderstatus,
        rentalorders.mileagestart AS mileage_start,
        rentalorders.mileageend AS mileage_end
    FROM {{ source('car_rental', 'rentalorders') }}
),

cars AS (
    SELECT
        cars.carid AS car_id,
        cars.make AS car_make,
        cars.model AS car_model,
        cars.color,
        cars.rentalcategoriesid AS rentalcategories_id
    FROM {{ source('car_rental', 'cars') }}    
),

rentalcategories AS (
    SELECT
        rentalcategories.rentalcategoriesid AS rentalcategories_id,
        rentalcategories.category,
        CASE 
            WHEN round(rentalcategories.monthlyrentprice,2) = 300.0 
                THEN 'car category 1'
            WHEN round(rentalcategories.monthlyrentprice,2) = 400.0 
                THEN 'car category 2'
            WHEN round(rentalcategories.monthlyrentprice,2) = 500.0 
                THEN 'car category 3'
            ELSE 'car category 4'
        END AS car_category,
        round(rentalcategories.monthlyrentprice,2) AS monthly_rent_price
    FROM {{ source('car_rental', 'rentalcategories') }}
),

final AS (
    SELECT

        cars.car_make,
        cars.car_model,
        rentalorders.mileage_start,
        rentalorders.mileage_end,
        rentalcategories.car_category
    FROM rentalorders
    inner join cars
    ON rentalorders.car_id = cars.car_id
    inner join rentalcategories 
    ON cars.rentalcategories_id = rentalcategories.rentalcategories_id
)

SELECT * FROM final