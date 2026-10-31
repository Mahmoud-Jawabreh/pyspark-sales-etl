-- Creates the reporting view for the learning environment.
-- Prerequisite: the sales pipeline has created lf_gold_product_sales.

USE CATALOG workspace;
USE SCHEMA de_learning;

CREATE VIEW de_learning.vw_product_sales_reporting (
  product,
  units_sold,
  revenue,
  revenue_per_unit)
COMMENT 'Product-level sales summary from accepted Silver records.
Revenue excludes quarantined records.
Each row represents one product.'
WITH SCHEMA COMPENSATION
AS SELECT
    product,
    units_sold,
    revenue,
    ROUND(
        revenue / NULLIF(units_sold, 0),
        2
    ) AS revenue_per_unit
FROM de_learning.lf_gold_product_sales ;