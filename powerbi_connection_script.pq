
// Power Query M Script for PostgreSQL Connection
// Generated on: 2025-05-26 03:57:25
// Database: airflow
// Host: localhost:5432

let
    // Connect to PostgreSQL database
    Source = PostgreSQL.Database("localhost:5432", "airflow"),
    
    // Get powerbi schema
    powerbi_schema = Source{[Schema="powerbi"]}[Data],
    
    // Stock dimension with sectors and categories
    dim_stocks = Source{[Schema="powerbi",Item="dim_stocks"]}[Data]

in
    // Return the main fact table
    fact_daily_trading
