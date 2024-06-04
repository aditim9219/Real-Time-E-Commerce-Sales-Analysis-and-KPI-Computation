# Real-Time-E-Commerce-Sales-Analysis-and-KPI-Computation

This project aims to analyze real-time sales data for RetailCorp Inc. and compute various Key Performance Indicators (KPIs). The data comprises global order invoices, and we will utilize big data tools to derive insights and improve business performance.
Online shopping has become one of the most popular activities worldwide, driven by digitally enabled customers. In 2019, global e-commerce sales reached 3.53 trillion USD and are projected to grow to 6.54 trillion USD by 2022.

# Impact of Big Data on E-commerce
Big data analytics has significantly enhanced business performance across various industries, including e-commerce. These tools help companies analyze trends and consumer behavior, enabling them to offer better and more customized products.

# Project Objective
For this project, we will compute various KPIs for RetailCorp Inc. using real-time sales data. The data includes global order invoices, and the primary KPIs we will calculate are:
- Total Volume of Sales
- Orders Per Minute (OPM)
- Rate of Return
- Average Transaction Size

These KPIs will be calculated both globally and on a per-country basis.

# Project Tasks
## Reading Data from Kafka
The project begins with reading the sales data from a Kafka server.

## Preprocessing Data
Next, we will preprocess the raw stream data to calculate additional derived columns. These include:
- total_cost: Total cost of an order
- total_items: Total number of items in an order
- is_order: Flag for new orders
- is_return: Flag for return orders

## Calculating KPIs
We will calculate various KPIs to analyze business performance:
- Total Volume of Sales: Sum of transaction values of all orders in the given time window, considering returns as negative values.
- Orders Per Minute (OPM): Total number of orders received in a minute.
- Rate of Return: Ratio of returned orders to total orders, indicating customer satisfaction.
- Average Transaction Size: Average amount of money spent per transaction.

## Implementing the Code
We will implement the necessary code to perform these tasks:
- Defining Schema: Code to define the schema of a single order.
- Calculating UDFs: Code to define the User-Defined Functions (UDFs) and utility functions.
- Writing to Console: Code to write the final summarized input values to the console.

## Calculating Time-based KPIs
We will calculate the KPIs using a tumbling window of one minute for orders globally and over a 10-minute interval for evaluation.

## Calculating Country-based KPIs
Similarly, we will calculate KPIs on a per-country basis, using a one-minute tumbling window and over a 10-minute interval for evaluation.

## Writing KPIs to JSON Files
Finally, we will write the calculated KPIs to JSON files for each one-minute window. These files will be archived into separate ZIP files for time-based and country-based KPIs.

Through these tasks, the project aims to derive actionable insights from the sales data to enhance business performance and customer satisfaction.

