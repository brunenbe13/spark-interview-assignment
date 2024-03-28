# PySpark Video Course - Solving a Spark Coding Quiz

This is the accompanying repository to my free video course published on YouTube, which can be found [here](TODO).
We will solve a made-up Spark coding quiz in a professional manner.

**Please Note:**
The guidance provided here is not intended to suggest how one should approach answering a Spark quiz during an actual interview, where brevity and a focus on problem-solving strategies within the allotted time are paramount.
Rather, this is an illustration of my approach to tackling a task with the level of thoroughness and professionalism I would apply in a real-world professional context.

## Domain description: Customer Behaviour Analysis

You are working with a retail dataset containing information about customer transactions, product details, and store information.
Your task is to analyze customer behavior over time, focusing on customer loyalty and product preferences.
This involves calculating metrics like the frequency of purchases, average spending per visit over time, and identifying preferred products for each customer.

### Transactions DataFrame

Schema:

- `transaction_id` (string)
- `customer_id` (string)
- `product_id` (string)
- `amount` (double)
- `transaction_date` (date)

Example Data:

| transaction_id | customer_id | product_id | amount | transaction_date |
|----------------|-------------|------------|--------|------------------|
| T100           | C1          | P1         | 150.0  | 2024-01-10       |
| T101           | C2          | P2         | 75.0   | 2024-01-15       |
| T102           | C1          | P3         | 200.0  | 2024-02-05       |

### Products DataFrame

Schema:

- `product_id` (string)
- `product_name` (string)
- `category` (string)

Example Data:

| product_id | product_name | category    |
|------------|--------------|-------------|
| P1         | Laptop       | Electronics |
| P2         | Headphones   | Electronics |
| P3         | Coffee Maker | Appliances  |

### Customers DataFrame

Schema:

- `customer_id` (string)
- `customer_name` (string)
- `join_date` (date)

Example Data:

| customer_id | customer_name | join_date  |
|-------------|---------------|------------|
| C1          | John Doe      | 2023-01-01 |
| C2          | Jane Smith    | 2023-06-15 |

## Task description

1. Calculate the monthly frequency of purchases and average spending per customer.
2. Identify the most preferred category for each customer based on the total amount spent per category.
3. Combine the above metrics with customer details and present a comprehensive analysis that includes the customer's name, join date, average monthly spending, purchase frequency, and preferred category.
4. Optimize the query or DataFrame transformations for performance considering data skewness.

Expected Output Schema:

- `customer_name` (string)
- `join_date` (date)
- `average_monthly_spending` (double)
- `purchase_frequency` (integer, monthly average)
- `preferred_category` (string)

## Instructions

1. Write the PySpark SQL or DataFrame API code to accomplish the above tasks.
2. Consider using window functions for ranking and aggregations.
3. Think about how to handle data skewness, especially if you have customers or products that significantly dominate the dataset.

This quiz is designed to test advanced Spark skills, including data manipulation, aggregation, optimization for data skew, and the ability to join and analyze data from multiple sources.