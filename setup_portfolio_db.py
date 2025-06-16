#!/usr/bin/env python3
"""
Portfolio Database Setup Script
Jalankan: python setup_portfolio_db.py
"""

import psycopg2
import os

def setup_portfolio_database():
    """Setup portfolio database schema"""
    
    # Database config
    db_config = {
        'host': os.getenv('DB_HOST', 'postgres'),
        'database': os.getenv('DB_NAME', 'airflow'),
        'user': os.getenv('DB_USER', 'airflow'),
        'password': os.getenv('DB_PASSWORD', 'airflow'),
        'port': int(os.getenv('DB_PORT', '5432'))
    }
    
    print("🔧 Setting up Portfolio Database...")
    print(f"📡 Connecting to {db_config['host']}:{db_config['port']}")
    
    try:
        # Connect to database
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()
        
        print("✅ Connected to database")
        
        # Read and execute schema
        schema_sql = """
        -- Create portfolio schema
        CREATE SCHEMA IF NOT EXISTS portfolio;
        
        -- Create transactions table
        CREATE TABLE IF NOT EXISTS portfolio.transactions (
            id SERIAL PRIMARY KEY,
            user_id VARCHAR(50) DEFAULT 'default_user',
            trade_date DATE NOT NULL,
            settlement_date DATE,
            symbol VARCHAR(10) NOT NULL,
            company_name VARCHAR(255),
            transaction_type VARCHAR(10) NOT NULL CHECK (transaction_type IN ('BUY', 'SELL')),
            quantity INTEGER NOT NULL,
            price DECIMAL(15,2) NOT NULL,
            total_value DECIMAL(20,2) NOT NULL,
            fees DECIMAL(10,2) DEFAULT 0,
            ref_number VARCHAR(50),
            source_file VARCHAR(255),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        CREATE TABLE IF NOT EXISTS portfolio.holdings (
            id SERIAL PRIMARY KEY,
            user_id VARCHAR(50) DEFAULT 'default_user',
            symbol VARCHAR(10) NOT NULL,
            company_name VARCHAR(255),
            total_quantity INTEGER NOT NULL DEFAULT 0,
            average_price DECIMAL(15,2) NOT NULL DEFAULT 0,
            total_cost DECIMAL(20,2) NOT NULL DEFAULT 0,
            current_price DECIMAL(15,2),
            current_value DECIMAL(20,2),
            unrealized_pnl DECIMAL(20,2),
            unrealized_pnl_pct DECIMAL(8,4),
            last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(user_id, symbol)
        );
        
        CREATE TABLE IF NOT EXISTS portfolio.portfolio_summary (
            id SERIAL PRIMARY KEY,
            user_id VARCHAR(50) DEFAULT 'default_user',
            summary_date DATE NOT NULL,
            total_invested DECIMAL(20,2) NOT NULL DEFAULT 0,
            current_value DECIMAL(20,2) NOT NULL DEFAULT 0,
            total_pnl DECIMAL(20,2) NOT NULL DEFAULT 0,
            total_pnl_pct DECIMAL(8,4) NOT NULL DEFAULT 0,
            realized_pnl DECIMAL(20,2) NOT NULL DEFAULT 0,
            unrealized_pnl DECIMAL(20,2) NOT NULL DEFAULT 0,
            total_stocks INTEGER NOT NULL DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(user_id, summary_date)
        );
        
        CREATE INDEX IF NOT EXISTS idx_transactions_user_symbol ON portfolio.transactions(user_id, symbol);
        CREATE INDEX IF NOT EXISTS idx_transactions_date ON portfolio.transactions(trade_date);
        CREATE INDEX IF NOT EXISTS idx_holdings_user ON portfolio.holdings(user_id);
        """
        
        # Execute schema creation
        cursor.execute(schema_sql)
        conn.commit()
        
        print("✅ Portfolio schema created")
        
        # Create trigger function
        trigger_sql = """
        CREATE OR REPLACE FUNCTION portfolio.update_holdings_after_transaction()
        RETURNS TRIGGER AS $$
        BEGIN
            IF TG_OP = 'INSERT' THEN
                INSERT INTO portfolio.holdings (user_id, symbol, company_name, total_quantity, average_price, total_cost)
                VALUES (NEW.user_id, NEW.symbol, NEW.company_name, 
                        CASE WHEN NEW.transaction_type = 'BUY' THEN NEW.quantity ELSE -NEW.quantity END,
                        CASE WHEN NEW.transaction_type = 'BUY' THEN NEW.price ELSE 0 END,
                        CASE WHEN NEW.transaction_type = 'BUY' THEN NEW.total_value ELSE -NEW.total_value END)
                ON CONFLICT (user_id, symbol)
                DO UPDATE SET
                    total_quantity = holdings.total_quantity + 
                        CASE WHEN NEW.transaction_type = 'BUY' THEN NEW.quantity ELSE -NEW.quantity END,
                    total_cost = holdings.total_cost + 
                        CASE WHEN NEW.transaction_type = 'BUY' THEN NEW.total_value ELSE -NEW.total_value END,
                    average_price = CASE 
                        WHEN (holdings.total_quantity + CASE WHEN NEW.transaction_type = 'BUY' THEN NEW.quantity ELSE -NEW.quantity END) > 0
                        THEN (holdings.total_cost + CASE WHEN NEW.transaction_type = 'BUY' THEN NEW.total_value ELSE -NEW.total_value END) / 
                             (holdings.total_quantity + CASE WHEN NEW.transaction_type = 'BUY' THEN NEW.quantity ELSE -NEW.quantity END)
                        ELSE 0
                    END,
                    company_name = COALESCE(holdings.company_name, NEW.company_name),
                    last_updated = CURRENT_TIMESTAMP;
                
                DELETE FROM portfolio.holdings 
                WHERE user_id = NEW.user_id AND symbol = NEW.symbol AND total_quantity <= 0;
                
                RETURN NEW;
            END IF;
            RETURN NULL;
        END;
        $$ LANGUAGE plpgsql;
        
        DROP TRIGGER IF EXISTS trigger_update_holdings ON portfolio.transactions;
        CREATE TRIGGER trigger_update_holdings
            AFTER INSERT ON portfolio.transactions
            FOR EACH ROW
            EXECUTE FUNCTION portfolio.update_holdings_after_transaction();
        """
        
        cursor.execute(trigger_sql)
        conn.commit()
        
        print("✅ Database triggers created")
        
        # Test the setup
        cursor.execute("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'portfolio'")
        table_count = cursor.fetchone()[0]
        
        print(f"✅ Portfolio setup complete! {table_count} tables created")
        
        cursor.close()
        conn.close()
        
        print("🎉 Portfolio Database ready!")
        print("📊 You can now use Portfolio Tracker in the dashboard")
        
    except Exception as e:
        print(f"❌ Error setting up database: {e}")
        print("🔧 Make sure PostgreSQL is running and accessible")

if __name__ == "__main__":
    setup_portfolio_database()
