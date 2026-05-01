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
