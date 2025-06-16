def format_currency(value):
    """Format number as currency"""
    if pd.isna(value):
        return "N/A"
    return f"Rp {value:,.0f}"

def format_percentage(value):
    """Format number as percentage"""
    if pd.isna(value):
        return "N/A"
    return f"{value:.2f}%"

def calculate_metrics(df):
    """Calculate basic metrics from dataframe"""
    if df.empty:
        return {}
    
    metrics = {
        'mean': df.mean(),
        'std': df.std(),
        'min': df.min(),
        'max': df.max()
    }
    return metrics 