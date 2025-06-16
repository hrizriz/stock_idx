# Import database utilities
try:
    from .database import PortfolioDatabase
except ImportError:
    # Fallback for relative import issues
    from portofolio_tracker.utils.database import PortfolioDatabase 