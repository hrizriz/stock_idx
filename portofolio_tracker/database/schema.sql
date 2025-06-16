-- ============================================================================
-- PORTFOLIO TRACKER DATABASE SCHEMA
-- File: portofolio_tracker/database/schema.sql
-- ============================================================================

-- Create portfolio schema
CREATE SCHEMA IF NOT EXISTS portfolio;

-- Table: portfolio.transactions
-- Stores all buy/sell transactions
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

-- Table: portfolio.holdings  
-- Current portfolio positions
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

-- Table: portfolio.portfolio_summary
-- Daily portfolio summary metrics
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

-- Table: portfolio.price_history
-- Historical price data for portfolio stocks
CREATE TABLE IF NOT EXISTS portfolio.price_history (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(10) NOT NULL,
    date DATE NOT NULL,
    close_price DECIMAL(15,2) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(symbol, date)
);

-- Performance Indexes
CREATE INDEX IF NOT EXISTS idx_transactions_user_symbol ON portfolio.transactions(user_id, symbol);
CREATE INDEX IF NOT EXISTS idx_transactions_date ON portfolio.transactions(trade_date);
CREATE INDEX IF NOT EXISTS idx_holdings_user ON portfolio.holdings(user_id);
CREATE INDEX IF NOT EXISTS idx_price_history_symbol_date ON portfolio.price_history(symbol, date);

-- Function: Auto-update holdings after transaction
CREATE OR REPLACE FUNCTION portfolio.update_holdings_after_transaction()
RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        -- Update or insert holding
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
        
        -- Remove holdings with zero quantity
        DELETE FROM portfolio.holdings 
        WHERE user_id = NEW.user_id AND symbol = NEW.symbol AND total_quantity <= 0;
        
        RETURN NEW;
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

-- Trigger: Automatically update holdings when transaction added
DROP TRIGGER IF EXISTS trigger_update_holdings ON portfolio.transactions;
CREATE TRIGGER trigger_update_holdings
    AFTER INSERT ON portfolio.transactions
    FOR EACH ROW
    EXECUTE FUNCTION portfolio.update_holdings_after_transaction();

-- Insert sample data for testing (optional)
-- INSERT INTO portfolio.transactions (user_id, trade_date, symbol, company_name, transaction_type, quantity, price, total_value)
-- VALUES ('default_user', '2025-06-13', 'BBCA', 'Bank Central Asia Tbk.', 'BUY', 1000, 10000, 10000000);

COMMENT ON SCHEMA portfolio IS 'Portfolio tracking system for Indonesian stocks';
COMMENT ON TABLE portfolio.transactions IS 'All buy/sell transactions from trade confirmations';
COMMENT ON TABLE portfolio.holdings IS 'Current portfolio positions with P&L calculations';
COMMENT ON TABLE portfolio.portfolio_summary IS 'Daily portfolio performance summary';
COMMENT ON TABLE portfolio.price_history IS 'Historical price data for portfolio analysis';