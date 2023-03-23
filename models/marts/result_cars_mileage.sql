select
    rentalorders.carid,
    cars.make,
    cars.model,
    rentalorders.mileagestart,
    rentalorders.mileageend,
    rentalcategories.car_category
from {{ source('car_rental', 'rentalorders')}} as rentalorders
inner join (
select 
cars.carid as car_id,
cars.rentalcategoriesid,
cars.make,
cars.model
from {{ source('car_rental', 'cars')}} as cars 
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
    from {{ source('car_rental', 'rentalcategories')}} as rentalcategories
) rentalcategories 
on cars.rentalcategoriesid = rentalcategories.rentalcategories_id