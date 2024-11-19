SELECT distinct 
    CASE 
        WHEN LOWER(bds.category_home) LIKE '%ban nha mat pho%' THEN 'ban nha mat pho'
        WHEN LOWER(bds.category_home) LIKE '%ban can ho chung cu%' THEN 'ban can ho chung cu'
        WHEN LOWER(bds.category_home) LIKE '%ban dat%' THEN 'ban dat'
        WHEN LOWER(bds.category_home) LIKE '%ban shophouse%' THEN 'ban shophouse'
        WHEN LOWER(bds.category_home) LIKE '%ban kho nha xuong%' THEN 'ban kho nha xuong'
        WHEN LOWER(bds.category_home) LIKE '%ban trang trai khu nghi duong' THEN 'ban trang trai khu nghi duong'
        WHEN LOWER(bds.category_home) LIKE '%ban condotel%' THEN 'ban condotel'
        WHEN LOWER(bds.category_home) LIKE '%ban loai bat dong san khac%' THEN 'ban loai bat dong san khac'
        WHEN LOWER(bds.category_home) LIKE '%ban nha biet thu%' THEN 'ban nha biet thu'
        WHEN LOWER(bds.category_home) LIKE '%ban nha rieng%' THEN 'ban nha biet thu'
        ELSE 'khác'
    END
 as category_home
    , count(*) as quantity_bds_hcm    
FROM `data-engineering-439905`.`batdongsan`.`dim_bds` as bds
where bds.city = "Hồ Chí Minh"
GROUP BY 
    CASE 
        WHEN LOWER(bds.category_home) LIKE '%ban nha mat pho%' THEN 'ban nha mat pho'
        WHEN LOWER(bds.category_home) LIKE '%ban can ho chung cu%' THEN 'ban can ho chung cu'
        WHEN LOWER(bds.category_home) LIKE '%ban dat%' THEN 'ban dat'
        WHEN LOWER(bds.category_home) LIKE '%ban shophouse%' THEN 'ban shophouse'
        WHEN LOWER(bds.category_home) LIKE '%ban kho nha xuong%' THEN 'ban kho nha xuong'
        WHEN LOWER(bds.category_home) LIKE '%ban trang trai khu nghi duong' THEN 'ban trang trai khu nghi duong'
        WHEN LOWER(bds.category_home) LIKE '%ban condotel%' THEN 'ban condotel'
        WHEN LOWER(bds.category_home) LIKE '%ban loai bat dong san khac%' THEN 'ban loai bat dong san khac'
        WHEN LOWER(bds.category_home) LIKE '%ban nha biet thu%' THEN 'ban nha biet thu'
        WHEN LOWER(bds.category_home) LIKE '%ban nha rieng%' THEN 'ban nha biet thu'
        ELSE 'khác'
    END
