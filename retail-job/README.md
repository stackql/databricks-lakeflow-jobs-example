# Retail Data Processing Pipeline

This project demonstrates a comprehensive Databricks Lakeflow pipeline using Databricks Asset Bundles (DAB) for retail data processing. The pipeline includes data ingestion, transformation, conditional logic, and state-based processing.

## Pipeline Architecture

### 1. Data Ingestion (`01_data_ingestion/`)
- **orders_table_creation.py**: Creates dummy orders data with order information
- **sales_table_creation.py**: Creates dummy sales data with product information

### 2. Data Loading (`02_data_loading/`)
- **customers_table_creation.py**: Creates dummy customer data with configurable catalog/schema

### 3. Data Processing (`03_data_processing/`)
- **join_customers_sales.py**: Joins customer and sales data, checks for duplicates

### 4. Data Transformation (`04_data_transformation/`)
- **join_customers_orders.py**: Creates comprehensive order dataset
- **remove_duplicates.py**: IF condition - removes duplicates if detected
- **clean_and_transform.py**: ELSE condition - performs additional transformations

### 5. State Processing (`05_state_processing/`)
- **process_orders_by_state.py**: FOR EACH loop - processes orders by state

## Key Lakeflow Features Demonstrated

1. **Conditional Logic**: IF-ELSE tasks based on duplicate detection
2. **For Each Tasks**: Parallel processing by state
3. **Task Dependencies**: Proper dependency management
4. **Parameter Passing**: Configuration through job parameters
5. **Task Values**: Data sharing between tasks

## Dummy Data

The pipeline uses realistic dummy data including:
- 10 orders across different customers
- 15 sales records with various products
- 10 customers across 5 states (CA, NY, TX, FL, WA)
- Product categories: Electronics, Furniture, Home, Office

## Deployment

Deploy using Databricks CLI:
```bash
databricks bundle deploy -t development
databricks bundle run retail_data_processing_pipeline -t development
```

## GitHub Actions Integration

This repository includes GitHub Actions workflows for CI/CD deployment to Databricks workspaces using DAB.