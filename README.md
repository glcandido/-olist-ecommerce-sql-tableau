# Olist E-commerce Analytics (SQL + Tableau)

End-to-end analytics project using the **Olist Brazilian e-commerce dataset**.  
Built a clean analysis layer in **MySQL**, created reusable **marts** for BI, and published interactive dashboards in **Tableau Public**.

## Live Dashboards (Tableau Public)
- Executive Overview (GMV Proxy):  
  https://public.tableau.com/views/OlistExecutiveOverview/ExecutiveOverviewGMVProxy?:language=en-GB&:sid=&:redirect=auth&:display_count=n&:origin=viz_share_link

- Commercial Insights:  
  https://public.tableau.com/views/OlistExecutiveOverviewCommercialInsights/CommercialInsights?:language=en-GB&:sid=&:redirect=auth&:display_count=n&:origin=viz_share_link

## What this project answers
- How has revenue (GMV proxy) and orders evolved over time?
- What is the **new vs repeat customer** contribution and trend?
- Which **categories** and **products** drive the most revenue?
- What commercial patterns stand out (concentration, top contributors)?

## Data model (high level)
The SQL scripts follow a layered approach:

**1) RAW (immutable imports)**  
Original Olist tables (orders, items, payments, customers, products, reviews, category translation).

**2) CLEAN ("silver") layer**  
Recreated typed, join-friendly tables (`*_clean`), with fixes like:
- Timestamp parsing / typing
- ID trimming
- Category translation cleanup
- Derived delivery metrics (e.g., delivery days, delay flag)

**3) FACT + MARTS ("gold") layer**
Reusable tables for Tableau / reporting, including:
- `fact_orders_enriched` (order-level grain, joins customers + computed revenue proxy)
- `mart_monthly_kpis` (monthly revenue, orders, AOV, items sold)
- `mart_monthly_new_repeat` (new vs repeat customers over time)
- `mart_category_revenue` (revenue by category)
- `mart_top_products` (top products by revenue/orders/items)

- How I Built This (Step-by-step)

1) Data ingestion (raw → database)

1. Downloaded the Olist dataset (multiple CSVs: orders, order_items, payments, customers, products, sellers, etc.).
2. Loaded CSVs into a SQL database (PostgreSQL recommended).
3. Standardized column types (dates/timestamps, numeric fields) during or after import.

2) Data checks + proofing

Before building insights, I ran validation to avoid “pretty dashboards on broken data”:

* Checked for duplicates and key integrity (e.g., order_id uniqueness, joins not inflating rows)
* Null / missing data profiling (especially date fields, customer identifiers, and product/category attributes)
* Sanity checks on totals (orders, revenue proxy, items) across tables

(See: Proofing_commented.sql and dataset_analysis.sql.)

3) Feature engineering + business logic

I created business-friendly fields to make analysis consistent:

* GMV proxy: revenue estimate using item price + freight (or dataset revenue proxy field)
* Time grain: monthly aggregation for KPI trends
* Customer logic: new vs repeat classification by first purchase month
* Product/category rollups: category revenue and top product ranking

4) Build reporting marts (the “semantic layer”)

Instead of querying raw tables in Tableau, I created marts so dashboards are fast and consistent:

* mart_monthly_kpis: monthly KPIs for executive overview charts + KPI tiles
* mart_monthly_new_repeat: customer mix evolution
* mart_category_revenue: category contribution and ranking
* mart_top_products: top products table for exploration

(See: EDA_commented.sql — this is the main script that materializes the marts.)

5) Tableau dashboards

1. Connected Tableau to the marts (not raw tables).
2. Built an Executive Overview:
    * KPI tiles (Revenue/Orders/Customers/AOV proxy)
    * Revenue trend & orders trend
    * Customer mix new vs repeat
3. Built Commercial Insights:
    * Top categories by revenue
    * Top products table (revenue/orders/items)
    * Filters to explore time periods and segments

6) Design choices

* Marts over raw tables to prevent inconsistent definitions across charts
* Clear KPI naming (GMV Proxy) to avoid overstating “true revenue”
* Filters designed for quick executive exploration (time, category/product, customer mix)

## Repo contents
### `/sql`
- `Proofing_commented.sql`  
  Profiling + clean layer build + revenue validation checks (GMV proxy).  
  Creates the `*_clean` tables and foundational analytical tables.

- `EDA_commented.sql`  
  Builds enriched fact tables + marts used by Tableau dashboards.

- `dataset_analysis.sql`  
  Additional sanity checks and validation queries during analysis.

## How to run (MySQL)
1. Import the Olist CSVs into MySQL (as raw tables).
2. Run scripts in this order:
   1) `Proofing_commented.sql`  
   2) `EDA_commented.sql`  
   3) `dataset_analysis.sql` (optional validation)
3. Connect Tableau to the generated mart tables.

> Notes:
> - Scripts are written to be **re-runnable** (DROP/CREATE build artifacts).
> - Revenue is treated as a **GMV proxy** based on available order/item/payment fields.

## Tableau dashboards
Dashboards are built on top of the marts listed above:
- Executive view: KPIs + revenue/orders trends + customer mix
- Commercial insights: top categories and top products, with drilldowns

## Assumptions & limitations
- “Revenue” is a **proxy** (GMV proxy), not accounting profit/margin.
- Some metrics depend on delivered/completed order status depending on filters used in the marts.
- Dataset represents a specific time window and marketplace dynamics.

## Skills demonstrated
- SQL data profiling & quality checks
- Building clean analytical layers and marts (star-schema mindset)
- KPI design (AOV, revenue proxy, new vs repeat)
- Tableau dashboard design + interactive filtering
