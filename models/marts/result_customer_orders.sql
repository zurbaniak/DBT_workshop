select
    customers.customerid as customer_id,
    customers.fullname,
    rentalcategories.category as customer_category,
    rentalcategories.monthly_rent_price,
    cars.make,
    cars.model,
    cars.color,
    rentalcategories.car_category
from {{ source('car_rental', 'customers')}} as customers
inner join (
    select
    rentalorders.rentalorderid as rentalorder_id,
    rentalorders.customerid as customer_id,
    rentalorders.carid as car_id,
    rentalorders.orderstatus
    from {{ source('car_rental', 'rentalorders')}} as rentalorders
) rentalorders
on customers.customerid = rentalorders.customer_id

inner join (
select
cars.carid as car_id,
cars.make,
cars.model,
cars.color
from {{ source('car_rental', 'cars')}} as cars 
) cars
on rentalorders.car_id = cars.car_id
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
    from {{ source('car_rental', 'rentalcategories')}} as rentalcategories
) rentalcategories on customers.rentalcategoriesid = rentalcategories.rentalcategories_id

order by customers.customerid

WITH
customers AS (
    SELECT
        customers.customerid as customer_id,
        customers.fullname as full_name,
        customers.rentalcategoriesid as rentalcategories_id
    FROM {{ source('car_rental', 'customers') }}
),
rentalorders AS (
    SELECT
        rentalorders.rentalorderid AS rentalorder_id,
        rentalorders.customerid AS customer_id,
        rentalorders.carid AS car_id,
        rentalorders.orderstatus
    FROM {{ source('car_rental', 'rentalorders') }}
),

cars AS (
    SELECT
        cars.carid AS car_id,
        cars.make,
        cars.model,
        cars.color
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
        customers.customer_id,
        customers.full_name,
        rentalcategories.category AS customer_category,
        rentalcategories.monthly_rent_price,
        cars.make,
        cars.model,
        cars.color,
        rentalcategories.car_category       
    FROM customers

    inner join rentalorders
    ON customers.customer_id = rentalorders.customer_id

    inner join cars
    ON rentalorders.car_id = cars.car_id

    inner join rentalcategories 
    ON customers.rentalcategories_id = rentalcategories.rentalcategories_id

    ORDER BY customers.customer_id
)
SELECT * FROM final
