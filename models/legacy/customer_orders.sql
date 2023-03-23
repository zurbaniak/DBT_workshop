select
    customers.customerid as customer_id,
    customers.fullname,
    rentalcategories.category as customer_category,
    rentalcategories.monthly_rent_price,
    cars.make,
    cars.model,
    cars.color,
    rentalcategories.car_category
from dbt_workshop.car_rental.customers as customers
inner join (
    select
    rentalorders.rentalorderid as rentalorder_id,
    rentalorders.customerid as customer_id,
    rentalorders.carid as car_id,
    rentalorders.orderstatus
    from dbt_workshop.car_rental.rentalorders
) rentalorders
on customers.customerid = rentalorders.customer_id

inner join (
select
cars.carid as car_id,
cars.make,
cars.model,
cars.color
from dbt_workshop.car_rental.cars
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
    from dbt_workshop.car_rental.rentalcategories
) rentalcategories on customers.rentalcategoriesid = rentalcategories.rentalcategories_id

order by customers.customerid