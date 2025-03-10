 WITH "t1" AS (
    SELECT
        "InvoiceDate",
        "StockCode",
        "CustomerID",
        COUNT (*) as nb_commandes,
        SUM ("Quantity") as total_sold,
        "UnitPrice",
        (SUM ("Quantity") * "UnitPrice") as earned
        FROM invoices
        where "InvoiceDate" like '2010-12-01%'
        group by "StockCode", "UnitPrice", "CustomerID", "InvoiceDate"
        )
    SELECT 
        DATE("InvoiceDate"),
        count(distinct "CustomerID") as customers_count,
        sum(total_sold) as total_sold,
        sum(earned) as total_rev,
        case
            when sum(total_sold) = 0 then null
            else round((sum(earned) / sum(total_sold))::numeric, 2)
        end as mean_price,
        case
            when count(distinct "CustomerID") = 0 then null
            else round((sum(earned) / count(distinct "CustomerID"))::numeric, 2)
        end as mean_basket
    FROM "t1"
    GROUP BY DATE("InvoiceDate");