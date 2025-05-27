# Save this as: airflow/dags/utils/elliott_config.py

"""
Elliott Wave Analysis Configuration Module
Berisi konfigurasi untuk Elliott Wave analysis dan trading signals
"""

import logging
from typing import Dict, List, Any
import os

# Logging configuration
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ElliottWaveConfig:
    """
    Konfigurasi untuk Elliott Wave Analysis
    """
    
    # Fibonacci levels yang digunakan dalam Elliott Wave
    FIBONACCI_LEVELS = {
        'retracement': [0.236, 0.382, 0.5, 0.618, 0.786, 0.886],
        'extension': [1.272, 1.382, 1.5, 1.618, 2.0, 2.618, 3.618, 4.236],
        'projection': [0.618, 1.0, 1.272, 1.618, 2.618]
    }
    
    # Minimum requirements untuk wave detection
    WAVE_DETECTION = {
        'min_wave_size_pct': 1.5,      # Minimum 1.5% price movement
        'min_duration_days': 1,         # Minimum 1 day duration
        'window_size': 5,               # Swing point detection window
        'prominence_factor': 0.1,       # Peak prominence factor
        'max_retracement_pct': 99,      # Max Wave 2 retracement
        'min_wave3_factor': 1.01        # Wave 3 minimum vs shortest wave
    }
    
    # Elliott Wave pattern scoring
    PATTERN_SCORING = {
        'rule_1_weight': 25,            # Wave 2 retracement rule
        'rule_2_weight': 25,            # Wave 3 not shortest rule
        'rule_3_weight': 20,            # Wave 4 overlap rule
        'fibonacci_weight': 25,         # Fibonacci adherence
        'time_volume_weight': 5,        # Time and volume relationships
        'min_confidence': 60            # Minimum pattern confidence
    }
    
    # Trading signal configuration
    TRADING_SIGNALS = {
        'strong_buy_confidence': 80,
        'buy_confidence': 65,
        'hold_confidence': 50,
        'sell_confidence': 65,
        'strong_sell_confidence': 80,
        'position_sizes': {
            'Large': 3.0,      # 3% of portfolio
            'Standard': 2.0,   # 2% of portfolio
            'Small': 1.0,      # 1% of portfolio
            'Micro': 0.5       # 0.5% of portfolio
        }
    }
    
    # Screening parameters
    SCREENING_CONFIG = {
        'max_symbols': 50,
        'min_data_days': 100,
        'min_liquidity_volume': 500000,     # Minimum daily volume
        'min_price_range_pct': 15,          # Minimum price volatility
        'lookback_days': 150,
        'max_analysis_time_hours': 2
    }
    
    # Indonesian stock market specific settings
    IDX_MARKET = {
        'trading_hours': {
            'session_1_start': '09:00',
            'session_1_end': '12:00',
            'session_2_start': '13:30',
            'session_2_end': '16:00'
        },
        'trading_days': ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday'],
        'market_timezone': 'Asia/Jakarta',
        'currency': 'IDR',
        'tick_size': 1,  # Minimum price movement
        'lot_size': 100  # Standard lot size
    }
    
    # Top Indonesian stocks untuk priority analysis
    TOP_STOCKS = {
        'tier_1': [  # Blue chips dengan likuiditas tinggi
            'BBCA', 'BBRI', 'BMRI', 'BBNI',  # Banking
            'ASII', 'UNTR', 'AUTO',           # Automotive
            'TLKM', 'EXCL', 'ISAT',          # Telecommunications
            'UNVR', 'ICBP', 'INDF'           # Consumer goods
        ],
        'tier_2': [  # Large caps
            'PGAS', 'ADRO', 'ITMG', 'PTBA',  # Energy & mining
            'GGRM', 'HMSP', 'WIIM',          # Tobacco & consumer
            'KLBF', 'KAEF', 'SIDO',          # Healthcare
            'CPIN', 'JPFA', 'MAIN'           # Agriculture & livestock
        ],
        'tier_3': [  # Mid caps dengan potensi
            'MYOR', 'ULTJ', 'DVLA',          # Consumer discretionary
            'AKRA', 'SMSM', 'TAXI',          # Services & technology
            'ANTM', 'INCO', 'TINS',          # Mining
            'JSMR', 'WIKA', 'WSKT'           # Infrastructure
        ]
    }
    
    # Risk management parameters
    RISK_MANAGEMENT = {
        'max_portfolio_risk': 0.02,      # 2% maximum portfolio risk per trade
        'max_position_size': 0.05,       # 5% maximum position size
        'stop_loss_levels': {
            'wave_2': 0.786,             # 78.6% Fibonacci for Wave 2 entries
            'wave_4': 0.618,             # 61.8% Fibonacci for Wave 4 entries
            'wave_5': 0.382,             # 38.2% Fibonacci for Wave 5 exits
            'breakdown': 0.05            # 5% below breakdown level
        },
        'take_profit_levels': {
            'conservative': 1.618,        # 161.8% Fibonacci extension
            'aggressive': 2.618,          # 261.8% Fibonacci extension
            'target_1': 1.272,           # First target at 127.2%
            'target_2': 1.618,           # Second target at 161.8%
            'target_3': 2.618            # Third target at 261.8%
        }
    }
    
    # Database configuration
    DATABASE_CONFIG = {
        'table_names': {
            'elliott_analysis': 'public_analytics.elliott_wave_analysis',
            'trading_signals': 'public_analytics.elliott_trading_signals',
            'wave_patterns': 'public_analytics.elliott_wave_patterns',
            'fibonacci_levels': 'public_analytics.elliott_fibonacci_levels'
        },
        'retention_days': 90,  # Keep analysis data for 90 days
        'batch_size': 100,     # Batch size for database operations
        'connection_timeout': 30
    }
    
    # Telegram notification settings
    TELEGRAM_CONFIG = {
        'max_message_length': 4000,     # Telegram message length limit
        'report_schedule': {
            'daily': '16:45',           # After market close
            'weekly': 'Friday 17:00',   # Weekly summary
            'alerts': 'immediate'       # Real-time alerts
        },
        'message_templates': {
            'signal_strength': {
                'STRONG_BUY': '🚀🟢',
                'BUY': '📈🟢',
                'HOLD': '⏸️🟡',
                'SELL': '📉🔴',
                'STRONG_SELL': '⬇️🔴'
            },
            'wave_labels': {
                '1': '1️⃣', '2': '2️⃣', '3': '3️⃣', '4': '4️⃣', '5': '5️⃣',
                'A': '🅰️', 'B': '🅱️', 'C': '©️'
            }
        }
    }
    
    # Performance monitoring
    MONITORING = {
        'performance_metrics': [
            'pattern_accuracy',
            'signal_success_rate',
            'average_holding_period',
            'risk_adjusted_returns',
            'maximum_drawdown'
        ],
        'alert_thresholds': {
            'low_confidence_pattern': 40,
            'high_failure_rate': 0.6,
            'system_error_rate': 0.05
        },
        'reporting_intervals': {
            'real_time': 1,      # minutes
            'daily': 1440,       # minutes (24 hours)
            'weekly': 10080      # minutes (7 days)
        }
    }

class ElliottWaveSignalConfig:
    """
    Konfigurasi untuk Elliott Wave trading signals
    """
    
    # Signal generation rules berdasarkan wave position
    WAVE_SIGNAL_RULES = {
        '1': {
            'signal': 'BUY',
            'confidence_base': 70,
            'position_size': 'Standard',
            'time_horizon': 'Medium',
            'description': 'Initial impulse wave - Trend beginning'
        },
        '2': {
            'signal': 'STRONG_BUY',
            'confidence_base': 85,
            'position_size': 'Large',
            'time_horizon': 'Medium',
            'description': 'Wave 2 completion - Strongest setup for Wave 3'
        },
        '3': {
            'signal': 'HOLD',
            'confidence_base': 60,
            'position_size': 'Standard',
            'time_horizon': 'Short',
            'description': 'Wave 3 in progress - Ride the momentum'
        },
        '4': {
            'signal': 'BUY',
            'confidence_base': 75,
            'position_size': 'Standard',
            'time_horizon': 'Short',
            'description': 'Wave 4 completion - Final rally Wave 5 expected'
        },
        '5': {
            'signal': 'SELL',
            'confidence_base': 80,
            'position_size': 'Large',
            'time_horizon': 'Medium',
            'description': 'Wave 5 completion - Major correction expected'
        },
        'A': {
            'signal': 'HOLD',
            'confidence_base': 50,
            'position_size': 'Small',
            'time_horizon': 'Short',
            'description': 'Wave A correction - Wait for Wave B to sell'
        },
        'B': {
            'signal': 'SELL',
            'confidence_base': 70,
            'position_size': 'Standard',
            'time_horizon': 'Short',
            'description': 'Wave B bounce - Final opportunity before Wave C'
        },
        'C': {
            'signal': 'STRONG_BUY',
            'confidence_base': 90,
            'position_size': 'Large',
            'time_horizon': 'Long',
            'description': 'Wave C completion - New bullish cycle beginning'
        }
    }
    
    # Risk-reward ratios berdasarkan wave position
    RISK_REWARD_TARGETS = {
        'wave_2_entry': {
            'stop_loss': 0.786,      # Below 78.6% Fibonacci
            'target_1': 1.618,       # 161.8% extension
            'target_2': 2.618,       # 261.8% extension
            'min_rr_ratio': 2.0
        },
        'wave_4_entry': {
            'stop_loss': 0.618,      # Below 61.8% Fibonacci
            'target_1': 1.272,       # 127.2% extension
            'target_2': 1.618,       # 161.8% extension
            'min_rr_ratio': 1.5
        },
        'wave_5_exit': {
            'target_1': 0.382,       # 38.2% retracement
            'target_2': 0.618,       # 61.8% retracement
            'min_rr_ratio': 1.0
        },
        'wave_c_entry': {
            'stop_loss': 0.1,        # 10% below Wave C low
            'target_1': 1.618,       # New Wave 1 target
            'target_2': 2.618,       # Extended Wave 1 target
            'min_rr_ratio': 3.0
        }
    }

def get_config() -> Dict[str, Any]:
    """
    Get complete Elliott Wave configuration
    
    Returns:
    Dict: Complete configuration dictionary
    """
    return {
        'elliott_wave': ElliottWaveConfig(),
        'signals': ElliottWaveSignalConfig(),
        'environment': os.getenv('ENVIRONMENT', 'production'),
        'debug_mode': os.getenv('DEBUG_MODE', 'false').lower() == 'true'
    }

def validate_config() -> bool:
    """
    Validate Elliott Wave configuration
    
    Returns:
    bool: True if configuration is valid
    """
    try:
        config = get_config()
        
        # Validate required configuration sections
        required_sections = [
            'elliott_wave',
            'signals'
        ]
        
        for section in required_sections:
            if section not in config:
                logger.error(f"Missing required configuration section: {section}")
                return False
        
        # Validate Fibonacci levels
        fib_levels = config['elliott_wave'].FIBONACCI_LEVELS
        if not all(isinstance(levels, list) for levels in fib_levels.values()):
            logger.error("Invalid Fibonacci levels configuration")
            return False
        
        # Validate trading signal thresholds
        signal_config = config['elliott_wave'].TRADING_SIGNALS
        confidence_levels = [
            signal_config['strong_buy_confidence'],
            signal_config['buy_confidence'],
            signal_config['sell_confidence'],
            signal_config['strong_sell_confidence']
        ]
        
        if not all(0 <= level <= 100 for level in confidence_levels):
            logger.error("Invalid confidence level configuration")
            return False
        
        logger.info("Elliott Wave configuration validation passed")
        return True
        
    except Exception as e:
        logger.error(f"Configuration validation failed: {str(e)}")
        return False

# Environment-specific configurations
ENVIRONMENTS = {
    'development': {
        'screening_max_symbols': 10,
        'telegram_enabled': False,
        'database_retention_days': 7,
        'debug_logging': True
    },
    'staging': {
        'screening_max_symbols': 25,
        'telegram_enabled': True,
        'database_retention_days': 30,
        'debug_logging': True
    },
    'production': {
        'screening_max_symbols': 50,
        'telegram_enabled': True,
        'database_retention_days': 90,
        'debug_logging': False
    }
}

def get_environment_config(env: str = None) -> Dict[str, Any]:
    """
    Get environment-specific configuration
    
    Parameters:
    env (str): Environment name (development, staging, production)
    
    Returns:
    Dict: Environment configuration
    """
    if env is None:
        env = os.getenv('ENVIRONMENT', 'production')
    
    return ENVIRONMENTS.get(env, ENVIRONMENTS['production'])

# Export main configurations
__all__ = [
    'ElliottWaveConfig',
    'ElliottWaveSignalConfig',
    'get_config',
    'validate_config',
    'get_environment_config'
]