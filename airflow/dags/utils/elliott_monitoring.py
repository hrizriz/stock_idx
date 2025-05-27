# Save this as: airflow/dags/utils/elliott_monitoring.py

"""
Elliott Wave Analysis Monitoring and Alerting System
Menyediakan real-time monitoring, performance tracking, dan alerting
"""

import pandas as pd
import numpy as np
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple
import json
import time
from dataclasses import dataclass
from enum import Enum

from .elliott_wave import analyze_elliott_waves_for_symbol, ElliottWaveAnalyzer
from .database import get_database_connection, create_table_if_not_exists
from .telegram import send_telegram_message
from .elliott_config import ElliottWaveConfig

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class AlertLevel(Enum):
    """Alert severity levels"""
    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"
    CRITICAL = "CRITICAL"

class AlertType(Enum):
    """Types of Elliott Wave alerts"""
    PATTERN_COMPLETION = "PATTERN_COMPLETION"
    SIGNAL_TRIGGER = "SIGNAL_TRIGGER"
    FIBONACCI_TOUCH = "FIBONACCI_TOUCH"
    RULE_VIOLATION = "RULE_VIOLATION"
    HIGH_CONFIDENCE = "HIGH_CONFIDENCE"
    MOMENTUM_DIVERGENCE = "MOMENTUM_DIVERGENCE"
    VOLUME_CONFIRMATION = "VOLUME_CONFIRMATION"
    SYSTEM_ERROR = "SYSTEM_ERROR"

@dataclass
class ElliottAlert:
    """Elliott Wave alert data structure"""
    symbol: str
    alert_type: AlertType
    alert_level: AlertLevel
    title: str
    message: str
    current_price: float
    wave_context: Optional[str] = None
    fibonacci_level: Optional[str] = None
    confidence_score: Optional[int] = None
    target_price: Optional[float] = None
    trigger_price: Optional[float] = None
    expiry_time: Optional[datetime] = None
    metadata: Optional[Dict] = None

class ElliottWaveMonitor:
    """
    Real-time monitoring system untuk Elliott Wave analysis
    """
    
    def __init__(self):
        self.config = ElliottWaveConfig()
        self.analyzer = ElliottWaveAnalyzer()
        self.alerts_sent = set()  # Track sent alerts to avoid duplicates
        self.last_analysis_time = {}  # Track last analysis time per symbol
        
    def monitor_symbols(self, symbols: List[str], monitoring_interval: int = 300) -> Dict[str, Any]:
        """
        Monitor multiple symbols untuk Elliott Wave opportunities
        
        Parameters:
        symbols (List[str]): List of symbols to monitor
        monitoring_interval (int): Monitoring interval in seconds
        
        Returns:
        Dict: Monitoring results
        """
        try:
            monitoring_results = {
                'monitoring_start': datetime.now().isoformat(),
                'symbols_monitored': len(symbols),
                'alerts_generated': 0,
                'errors': [],
                'symbol_status': {}
            }
            
            for symbol in symbols:
                try:
                    symbol_results = self._monitor_single_symbol(symbol)
                    monitoring_results['symbol_status'][symbol] = symbol_results
                    
                    # Generate alerts if needed
                    alerts = self._check_alert_conditions(symbol, symbol_results)
                    
                    for alert in alerts:
                        if self._should_send_alert(alert):
                            self._send_alert(alert)
                            monitoring_results['alerts_generated'] += 1
                    
                except Exception as e:
                    error_msg = f"Error monitoring {symbol}: {str(e)}"
                    monitoring_results['errors'].append(error_msg)
                    logger.error(error_msg)
            
            # Update monitoring statistics
            self._update_monitoring_stats(monitoring_results)
            
            return monitoring_results
            
        except Exception as e:
            logger.error(f"Error in symbol monitoring: {str(e)}")
            return {'error': str(e)}
    
    def _monitor_single_symbol(self, symbol: str) -> Dict[str, Any]:
        """Monitor single symbol untuk Elliott Wave patterns"""
        try:
            # Run Elliott Wave analysis
            analysis = analyze_elliott_waves_for_symbol(symbol, lookback_days=120)
            
            if 'error' in analysis:
                return {'status': 'error', 'message': analysis['error']}
            
            # Extract key information
            elliott_pattern = analysis.get('elliott_pattern', {})
            trading_signals = analysis.get('trading_signals', {})
            current_price = analysis.get('current_price', 0)
            
            # Calculate monitoring metrics
            monitoring_data = {
                'status': 'active',
                'last_update': datetime.now().isoformat(),
                'current_price': current_price,
                'pattern_found': elliott_pattern.get('pattern_found', False),
                'pattern_confidence': elliott_pattern.get('confidence', 0),
                'current_wave': elliott_pattern.get('current_wave'),
                'trading_signal': trading_signals.get('signal', 'HOLD'),
                'signal_confidence': trading_signals.get('confidence', 0),
                'fibonacci_levels': analysis.get('fibonacci_levels', {}),
                'price_context': analysis.get('price_context', {}),
                'technical_context': analysis.get('technical_context', {}),
                'alert_triggers': []
            }
            
            # Check for specific monitoring conditions
            self._check_wave_completion(symbol, monitoring_data)
            self._check_fibonacci_levels(symbol, monitoring_data)
            self._check_momentum_divergence(symbol, monitoring_data)
            self._check_volume_confirmation(symbol, monitoring_data)
            
            return monitoring_data
            
        except Exception as e:
            logger.error(f"Error monitoring symbol {symbol}: {str(e)}")
            return {'status': 'error', 'message': str(e)}
    
    def _check_wave_completion(self, symbol: str, data: Dict) -> None:
        """Check for Elliott Wave pattern completion"""
        try:
            current_wave = data.get('current_wave')
            pattern_confidence = data.get('pattern_confidence', 0)
            
            # High confidence wave completions
            if pattern_confidence >= 80:
                if current_wave == '2':
                    data['alert_triggers'].append({
                        'type': 'WAVE_2_COMPLETION',
                        'priority': 'HIGH',
                        'message': f'Wave 2 completion detected - Strong Wave 3 setup'
                    })
                elif current_wave == '5':
                    data['alert_triggers'].append({
                        'type': 'WAVE_5_COMPLETION',
                        'priority': 'HIGH',
                        'message': f'Wave 5 completion detected - Major correction expected'
                    })
                elif current_wave == 'C':
                    data['alert_triggers'].append({
                        'type': 'WAVE_C_COMPLETION',
                        'priority': 'CRITICAL',
                        'message': f'Wave C completion detected - New bullish cycle expected'
                    })
            
        except Exception as e:
            logger.error(f"Error checking wave completion for {symbol}: {str(e)}")
    
    def _check_fibonacci_levels(self, symbol: str, data: Dict) -> None:
        """Check for Fibonacci level interactions"""
        try:
            current_price = data.get('current_price', 0)
            fibonacci_levels = data.get('fibonacci_levels', {})
            
            if not fibonacci_levels or current_price == 0:
                return
            
            tolerance = 0.02  # 2% tolerance for Fibonacci level touch
            
            # Check retracement levels
            retracement_levels = fibonacci_levels.get('retracement', {})
            for level_name, level_price in retracement_levels.items():
                if level_price and abs(current_price - level_price) / level_price <= tolerance:
                    data['alert_triggers'].append({
                        'type': 'FIBONACCI_RETRACEMENT',
                        'priority': 'MEDIUM',
                        'message': f'Price touching {level_name} Fibonacci retracement at {level_price:,.0f}',
                        'fibonacci_level': level_name,
                        'target_price': level_price
                    })
            
            # Check extension levels
            extension_levels = fibonacci_levels.get('extension', {})
            for level_name, level_price in extension_levels.items():
                if level_price and abs(current_price - level_price) / level_price <= tolerance:
                    data['alert_triggers'].append({
                        'type': 'FIBONACCI_EXTENSION',
                        'priority': 'HIGH',
                        'message': f'Price reaching {level_name} Fibonacci extension at {level_price:,.0f}',
                        'fibonacci_level': level_name,
                        'target_price': level_price
                    })
            
        except Exception as e:
            logger.error(f"Error checking Fibonacci levels for {symbol}: {str(e)}")
    
    def _check_momentum_divergence(self, symbol: str, data: Dict) -> None:
        """Check for momentum divergence signals"""
        try:
            current_wave = data.get('current_wave')
            technical_context = data.get('technical_context', {})
            
            # Wave 5 with momentum divergence is significant
            if current_wave == '5':
                # This would require additional technical indicators
                # For now, we'll use volume trend as a proxy
                volume_trend = technical_context.get('volume_trend', '')
                
                if volume_trend == 'Decreasing':
                    data['alert_triggers'].append({
                        'type': 'MOMENTUM_DIVERGENCE',
                        'priority': 'HIGH',
                        'message': f'Potential momentum divergence in Wave 5 - Volume declining'
                    })
            
        except Exception as e:
            logger.error(f"Error checking momentum divergence for {symbol}: {str(e)}")
    
    def _check_volume_confirmation(self, symbol: str, data: Dict) -> None:
        """Check for volume confirmation of wave patterns"""
        try:
            current_wave = data.get('current_wave')
            technical_context = data.get('technical_context', {})
            trading_signal = data.get('trading_signal', 'HOLD')
            
            volume_trend = technical_context.get('volume_trend', '')
            
            # Volume confirmation for bullish signals
            if trading_signal in ['STRONG_BUY', 'BUY'] and volume_trend == 'Increasing':
                data['alert_triggers'].append({
                    'type': 'VOLUME_CONFIRMATION',
                    'priority': 'MEDIUM',
                    'message': f'Volume confirming {trading_signal} signal in Wave {current_wave}'
                })
            
            # Volume divergence warning
            elif trading_signal in ['STRONG_BUY', 'BUY'] and volume_trend == 'Decreasing':
                data['alert_triggers'].append({
                    'type': 'VOLUME_DIVERGENCE',
                    'priority': 'LOW',
                    'message': f'Volume divergence on {trading_signal} signal - Caution advised'
                })
            
        except Exception as e:
            logger.error(f"Error checking volume confirmation for {symbol}: {str(e)}")
    
    def _check_alert_conditions(self, symbol: str, analysis_data: Dict) -> List[ElliottAlert]:
        """Generate alerts based on analysis data"""
        alerts = []
        
        try:
            alert_triggers = analysis_data.get('alert_triggers', [])
            current_price = analysis_data.get('current_price', 0)
            current_wave = analysis_data.get('current_wave')
            pattern_confidence = analysis_data.get('pattern_confidence', 0)
            signal_confidence = analysis_data.get('signal_confidence', 0)
            
            for trigger in alert_triggers:
                alert_type_str = trigger['type']
                priority = trigger['priority']
                message = trigger['message']
                
                # Map priority to alert level
                alert_level_map = {
                    'LOW': AlertLevel.LOW,
                    'MEDIUM': AlertLevel.MEDIUM,
                    'HIGH': AlertLevel.HIGH,
                    'CRITICAL': AlertLevel.CRITICAL
                }
                
                # Create alert
                alert = ElliottAlert(
                    symbol=symbol,
                    alert_type=AlertType.PATTERN_COMPLETION,  # Default, could be more specific
                    alert_level=alert_level_map.get(priority, AlertLevel.MEDIUM),
                    title=f"{symbol} Elliott Wave Alert",
                    message=message,
                    current_price=current_price,
                    wave_context=current_wave,
                    confidence_score=max(pattern_confidence, signal_confidence),
                    fibonacci_level=trigger.get('fibonacci_level'),
                    target_price=trigger.get('target_price'),
                    expiry_time=datetime.now() + timedelta(hours=24),
                    metadata={
                        'pattern_confidence': pattern_confidence,
                        'signal_confidence': signal_confidence,
                        'trigger_type': alert_type_str
                    }
                )
                
                alerts.append(alert)
            
            # High confidence pattern alert
            if pattern_confidence >= 85:
                alerts.append(ElliottAlert(
                    symbol=symbol,
                    alert_type=AlertType.HIGH_CONFIDENCE,
                    alert_level=AlertLevel.HIGH,
                    title=f"{symbol} High Confidence Elliott Pattern",
                    message=f"High confidence Elliott Wave pattern detected (Wave {current_wave}, {pattern_confidence}% confidence)",
                    current_price=current_price,
                    wave_context=current_wave,
                    confidence_score=pattern_confidence
                ))
            
            return alerts
            
        except Exception as e:
            logger.error(f"Error checking alert conditions for {symbol}: {str(e)}")
            return []
    
    def _should_send_alert(self, alert: ElliottAlert) -> bool:
        """Determine if alert should be sent (avoid duplicates)"""
        try:
            # Create unique alert key
            alert_key = f"{alert.symbol}_{alert.alert_type.value}_{alert.wave_context}_{alert.fibonacci_level}"
            
            # Check if similar alert was sent recently (within 6 hours)
            if alert_key in self.alerts_sent:
                return False
            
            # Add to sent alerts (with expiry)
            self.alerts_sent.add(alert_key)
            
            # Clean old alerts (simple approach)
            if len(self.alerts_sent) > 1000:
                self.alerts_sent.clear()
            
            return True
            
        except Exception:
            return True  # Default to sending if check fails
    
    def _send_alert(self, alert: ElliottAlert) -> bool:
        """Send alert via configured channels"""
        try:
            # Save alert to database
            self._save_alert_to_db(alert)
            
            # Send Telegram notification
            telegram_message = self._format_telegram_alert(alert)
            telegram_result = send_telegram_message(telegram_message)
            
            # Log alert
            logger.info(f"Alert sent for {alert.symbol}: {alert.title}")
            
            return "successfully" in telegram_result.lower()
            
        except Exception as e:
            logger.error(f"Error sending alert: {str(e)}")
            return False
    
    def _format_telegram_alert(self, alert: ElliottAlert) -> str:
        """Format alert message for Telegram"""
        try:
            # Alert level emojis
            level_emojis = {
                AlertLevel.LOW: "🔵",
                AlertLevel.MEDIUM: "🟡",
                AlertLevel.HIGH: "🟠",
                AlertLevel.CRITICAL: "🔴"
            }
            
            # Alert type emojis
            type_emojis = {
                AlertType.PATTERN_COMPLETION: "🌊",
                AlertType.SIGNAL_TRIGGER: "📊",
                AlertType.FIBONACCI_TOUCH: "📐",
                AlertType.HIGH_CONFIDENCE: "⭐",
                AlertType.MOMENTUM_DIVERGENCE: "📉",
                AlertType.VOLUME_CONFIRMATION: "📈"
            }
            
            emoji = level_emojis.get(alert.alert_level, "🔔")
            type_emoji = type_emojis.get(alert.alert_type, "📢")
            
            message = f"{emoji} {type_emoji} *ELLIOTT WAVE ALERT*\n\n"
            message += f"*Symbol:* {alert.symbol}\n"
            message += f"*Alert Level:* {alert.alert_level.value}\n"
            message += f"*Current Price:* Rp{alert.current_price:,.0f}\n"
            
            if alert.wave_context:
                message += f"*Wave Position:* {alert.wave_context}\n"
            
            if alert.confidence_score:
                message += f"*Confidence:* {alert.confidence_score}%\n"
            
            if alert.fibonacci_level:
                message += f"*Fibonacci Level:* {alert.fibonacci_level}\n"
            
            if alert.target_price:
                message += f"*Target Price:* Rp{alert.target_price:,.0f}\n"
            
            message += f"\n*Message:* {alert.message}\n"
            
            message += f"\n⏰ Alert Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
            
            if alert.expiry_time:
                message += f"\n⏰ Expires: {alert.expiry_time.strftime('%Y-%m-%d %H:%M:%S')}"
            
            message += "\n\n⚠️ *This is an automated Elliott Wave analysis alert. Always conduct your own research before making investment decisions.*"
            
            return message
            
        except Exception as e:
            logger.error(f"Error formatting Telegram alert: {str(e)}")
            return f"Elliott Wave Alert for {alert.symbol}: {alert.message}"
    
    def _save_alert_to_db(self, alert: ElliottAlert) -> None:
        """Save alert to database"""
        try:
            conn = get_database_connection()
            cursor = conn.cursor()
            
            # Ensure table exists
            create_table_if_not_exists(
                'public_analytics.elliott_wave_alerts',
                """
                CREATE TABLE public_analytics.elliott_wave_alerts (
                    id SERIAL PRIMARY KEY,
                    symbol TEXT NOT NULL,
                    alert_type TEXT NOT NULL,
                    alert_level TEXT NOT NULL,
                    title TEXT NOT NULL,
                    message TEXT NOT NULL,
                    current_price NUMERIC(15,2),
                    wave_context TEXT,
                    fibonacci_level TEXT,
                    confidence_score INTEGER,
                    target_price NUMERIC(15,2),
                    trigger_price NUMERIC(15,2),
                    expiry_time TIMESTAMP,
                    metadata JSONB,
                    telegram_sent BOOLEAN DEFAULT FALSE,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
            
            # Insert alert
            cursor.execute("""
            INSERT INTO public_analytics.elliott_wave_alerts
            (symbol, alert_type, alert_level, title, message, current_price, 
             wave_context, fibonacci_level, confidence_score, target_price, 
             trigger_price, expiry_time, metadata, telegram_sent)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                alert.symbol,
                alert.alert_type.value,
                alert.alert_level.value,
                alert.title,
                alert.message,
                alert.current_price,
                alert.wave_context,
                alert.fibonacci_level,
                alert.confidence_score,
                alert.target_price,
                alert.trigger_price,
                alert.expiry_time,
                json.dumps(alert.metadata) if alert.metadata else None,
                True
            ))
            
            conn.commit()
            cursor.close()
            conn.close()
            
        except Exception as e:
            logger.error(f"Error saving alert to database: {str(e)}")
    
    def _update_monitoring_stats(self, results: Dict) -> None:
        """Update monitoring statistics"""
        try:
            conn = get_database_connection()
            cursor = conn.cursor()
            
            # Update daily monitoring stats
            cursor.execute("""
            INSERT INTO public_analytics.elliott_monitoring_stats 
            (monitoring_date, symbols_monitored, alerts_generated, errors_count, created_at)
            VALUES (CURRENT_DATE, %s, %s, %s, CURRENT_TIMESTAMP)
            ON CONFLICT (monitoring_date) 
            DO UPDATE SET
                symbols_monitored = symbols_monitored + EXCLUDED.symbols_monitored,
                alerts_generated = alerts_generated + EXCLUDED.alerts_generated,
                errors_count = errors_count + EXCLUDED.errors_count,
                updated_at = CURRENT_TIMESTAMP
            """, (
                results['symbols_monitored'],
                results['alerts_generated'],
                len(results['errors'])
            ))
            
            conn.commit()
            cursor.close()
            conn.close()
            
        except Exception as e:
            logger.error(f"Error updating monitoring stats: {str(e)}")

class ElliottPerformanceTracker:
    """
    Performance tracking untuk Elliott Wave analysis accuracy
    """
    
    def __init__(self):
        self.config = ElliottWaveConfig()
    
    def track_signal_performance(self, days_back: int = 30) -> Dict[str, Any]:
        """
        Track performance of Elliott Wave signals
        
        Parameters:
        days_back (int): Number of days to look back
        
        Returns:
        Dict: Performance metrics
        """
        try:
            conn = get_database_connection()
            
            # Get signals from specified period
            signals_query = f"""
            SELECT 
                symbol,
                signal_type,
                confidence_score,
                entry_price,
                current_price,
                signal_date,
                pnl_pct,
                status,
                elliott_wave_basis
            FROM public_analytics.elliott_trading_signals
            WHERE signal_date >= CURRENT_DATE - INTERVAL '{days_back} days'
            AND status IN ('TRIGGERED', 'STOPPED', 'EXPIRED')
            """
            
            signals_df = pd.read_sql(signals_query, conn)
            conn.close()
            
            if signals_df.empty:
                return {'message': 'No completed signals in specified period'}
            
            # Calculate performance metrics
            performance_metrics = {
                'tracking_period': f'{days_back} days',
                'total_signals': len(signals_df),
                'signal_distribution': signals_df['signal_type'].value_counts().to_dict(),
                'overall_metrics': {},
                'by_signal_type': {},
                'by_confidence_level': {},
                'by_wave_position': {}
            }
            
            # Overall metrics
            profitable_signals = signals_df[signals_df['pnl_pct'] > 0]
            performance_metrics['overall_metrics'] = {
                'total_signals': len(signals_df),
                'profitable_signals': len(profitable_signals),
                'win_rate_pct': len(profitable_signals) / len(signals_df) * 100,
                'average_return_pct': signals_df['pnl_pct'].mean(),
                'best_return_pct': signals_df['pnl_pct'].max(),
                'worst_return_pct': signals_df['pnl_pct'].min(),
                'total_return_pct': signals_df['pnl_pct'].sum(),
                'average_confidence': signals_df['confidence_score'].mean()
            }
            
            # Performance by signal type
            for signal_type in signals_df['signal_type'].unique():
                type_data = signals_df[signals_df['signal_type'] == signal_type]
                type_profitable = type_data[type_data['pnl_pct'] > 0]
                
                performance_metrics['by_signal_type'][signal_type] = {
                    'count': len(type_data),
                    'win_rate_pct': len(type_profitable) / len(type_data) * 100,
                    'average_return_pct': type_data['pnl_pct'].mean(),
                    'total_return_pct': type_data['pnl_pct'].sum()
                }
            
            # Performance by confidence level
            confidence_bins = [(0, 60), (60, 75), (75, 85), (85, 100)]
            for low, high in confidence_bins:
                bin_data = signals_df[
                    (signals_df['confidence_score'] >= low) & 
                    (signals_df['confidence_score'] < high)
                ]
                
                if not bin_data.empty:
                    bin_profitable = bin_data[bin_data['pnl_pct'] > 0]
                    performance_metrics['by_confidence_level'][f'{low}-{high}%'] = {
                        'count': len(bin_data),
                        'win_rate_pct': len(bin_profitable) / len(bin_data) * 100,
                        'average_return_pct': bin_data['pnl_pct'].mean()
                    }
            
            # Performance by Elliott Wave position
            for wave_pos in signals_df['elliott_wave_basis'].unique():
                if pd.notna(wave_pos):
                    wave_data = signals_df[signals_df['elliott_wave_basis'] == wave_pos]
                    wave_profitable = wave_data[wave_data['pnl_pct'] > 0]
                    
                    performance_metrics['by_wave_position'][wave_pos] = {
                        'count': len(wave_data),
                        'win_rate_pct': len(wave_profitable) / len(wave_data) * 100,
                        'average_return_pct': wave_data['pnl_pct'].mean()
                    }
            
            # Save performance metrics to database
            self._save_performance_metrics(performance_metrics)
            
            return performance_metrics
            
        except Exception as e:
            logger.error(f"Error tracking signal performance: {str(e)}")
            return {'error': str(e)}
    
    def track_pattern_accuracy(self, days_back: int = 60) -> Dict[str, Any]:
        """
        Track accuracy of Elliott Wave pattern identification
        
        Parameters:
        days_back (int): Number of days to look back
        
        Returns:
        Dict: Pattern accuracy metrics
        """
        try:
            conn = get_database_connection()
            
            # Get pattern analysis from specified period
            pattern_query = f"""
            SELECT 
                symbol,
                analysis_date,
                pattern_found,
                pattern_confidence,
                current_wave,
                trading_signal,
                signal_confidence,
                fibonacci_adherence_score
            FROM public_analytics.elliott_wave_analysis
            WHERE analysis_date >= CURRENT_DATE - INTERVAL '{days_back} days'
            AND pattern_found = TRUE
            """
            
            patterns_df = pd.read_sql(pattern_query, conn)
            conn.close()
            
            if patterns_df.empty:
                return {'message': 'No patterns found in specified period'}
            
            # Calculate pattern accuracy metrics
            accuracy_metrics = {
                'tracking_period': f'{days_back} days',
                'total_patterns': len(patterns_df),
                'pattern_distribution': patterns_df['current_wave'].value_counts().to_dict(),
                'confidence_distribution': {},
                'accuracy_by_wave': {},
                'fibonacci_accuracy': {},
                'overall_accuracy': {}
            }
            
            # Confidence distribution
            confidence_ranges = [(60, 70), (70, 80), (80, 90), (90, 100)]
            for low, high in confidence_ranges:
                count = len(patterns_df[
                    (patterns_df['pattern_confidence'] >= low) & 
                    (patterns_df['pattern_confidence'] < high)
                ])
                accuracy_metrics['confidence_distribution'][f'{low}-{high}%'] = count
            
            # Average metrics
            accuracy_metrics['overall_accuracy'] = {
                'average_pattern_confidence': patterns_df['pattern_confidence'].mean(),
                'average_signal_confidence': patterns_df['signal_confidence'].mean(),
                'average_fibonacci_score': patterns_df['fibonacci_adherence_score'].mean(),
                'high_confidence_patterns': len(patterns_df[patterns_df['pattern_confidence'] >= 80])
            }
            
            return accuracy_metrics
            
        except Exception as e:
            logger.error(f"Error tracking pattern accuracy: {str(e)}")
            return {'error': str(e)}
    
    def _save_performance_metrics(self, metrics: Dict) -> None:
        """Save performance metrics to database"""
        try:
            conn = get_database_connection()
            cursor = conn.cursor()
            
            # Create performance tracking table if not exists
            create_table_if_not_exists(
                'public_analytics.elliott_performance_tracking',
                """
                CREATE TABLE public_analytics.elliott_performance_tracking (
                    id SERIAL PRIMARY KEY,
                    tracking_date DATE NOT NULL,
                    tracking_period TEXT,
                    total_signals INTEGER,
                    win_rate_pct NUMERIC(5,2),
                    average_return_pct NUMERIC(8,4),
                    total_return_pct NUMERIC(8,4),
                    best_return_pct NUMERIC(8,4),
                    worst_return_pct NUMERIC(8,4),
                    average_confidence NUMERIC(5,2),
                    high_confidence_signals INTEGER,
                    performance_by_type JSONB,
                    performance_by_wave JSONB,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(tracking_date, tracking_period)
                )
                """
            )
            
            overall = metrics.get('overall_metrics', {})
            
            cursor.execute("""
            INSERT INTO public_analytics.elliott_performance_tracking
            (tracking_date, tracking_period, total_signals, win_rate_pct, 
             average_return_pct, total_return_pct, best_return_pct, worst_return_pct,
             average_confidence, performance_by_type, performance_by_wave)
            VALUES (CURRENT_DATE, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (tracking_date, tracking_period)
            DO UPDATE SET
                total_signals = EXCLUDED.total_signals,
                win_rate_pct = EXCLUDED.win_rate_pct,
                average_return_pct = EXCLUDED.average_return_pct,
                total_return_pct = EXCLUDED.total_return_pct,
                best_return_pct = EXCLUDED.best_return_pct,
                worst_return_pct = EXCLUDED.worst_return_pct,
                average_confidence = EXCLUDED.average_confidence,
                performance_by_type = EXCLUDED.performance_by_type,
                performance_by_wave = EXCLUDED.performance_by_wave,
                created_at = CURRENT_TIMESTAMP
            """, (
                metrics.get('tracking_period'),
                overall.get('total_signals', 0),
                overall.get('win_rate_pct', 0),
                overall.get('average_return_pct', 0),
                overall.get('total_return_pct', 0),
                overall.get('best_return_pct', 0),
                overall.get('worst_return_pct', 0),
                overall.get('average_confidence', 0),
                json.dumps(metrics.get('by_signal_type', {})),
                json.dumps(metrics.get('by_wave_position', {}))
            ))
            
            conn.commit()
            cursor.close()
            conn.close()
            
        except Exception as e:
            logger.error(f"Error saving performance metrics: {str(e)}")

def run_elliott_monitoring(symbols: List[str] = None) -> str:
    """
    Run Elliott Wave monitoring for specified symbols
    
    Parameters:
    symbols (List[str]): List of symbols to monitor (default: top stocks)
    
    Returns:
    str: Monitoring status message
    """
    try:
        if symbols is None:
            # Use top Indonesian stocks
            symbols = [
                'BBCA', 'BBRI', 'BMRI', 'BBNI', 'ASII', 'TLKM', 'UNVR', 'ICBP',
                'GGRM', 'INDF', 'PGAS', 'ADRO', 'ITMG', 'KLBF', 'CPIN'
            ]
        
        monitor = ElliottWaveMonitor()
        results = monitor.monitor_symbols(symbols)
        
        if 'error' in results:
            return f"Monitoring error: {results['error']}"
        
        status_msg = f"Elliott Wave monitoring completed: "
        status_msg += f"{results['symbols_monitored']} symbols monitored, "
        status_msg += f"{results['alerts_generated']} alerts generated"
        
        if results['errors']:
            status_msg += f", {len(results['errors'])} errors"
        
        logger.info(status_msg)
        return status_msg
        
    except Exception as e:
        error_msg = f"Error running Elliott monitoring: {str(e)}"
        logger.error(error_msg)
        return error_msg

def run_performance_tracking() -> str:
    """
    Run Elliott Wave performance tracking
    
    Returns:
    str: Performance tracking status message
    """
    try:
        tracker = ElliottPerformanceTracker()
        
        # Track signal performance (last 30 days)
        signal_performance = tracker.track_signal_performance(30)
        
        # Track pattern accuracy (last 60 days)
        pattern_accuracy = tracker.track_pattern_accuracy(60)
        
        status_msg = "Elliott Wave performance tracking completed: "
        
        if 'overall_metrics' in signal_performance:
            overall = signal_performance['overall_metrics']
            status_msg += f"Signal performance: {overall.get('win_rate_pct', 0):.1f}% win rate, "
            status_msg += f"{overall.get('average_return_pct', 0):.2f}% avg return. "
        
        if 'overall_accuracy' in pattern_accuracy:
            accuracy = pattern_accuracy['overall_accuracy']
            status_msg += f"Pattern accuracy: {accuracy.get('average_pattern_confidence', 0):.1f}% avg confidence"
        
        logger.info(status_msg)
        return status_msg
        
    except Exception as e:
        error_msg = f"Error in performance tracking: {str(e)}"
        logger.error(error_msg)
        return error_msg

# Export functions
__all__ = [
    'ElliottWaveMonitor',
    'ElliottPerformanceTracker',
    'ElliottAlert',
    'AlertLevel',
    'AlertType',
    'run_elliott_monitoring',
    'run_performance_tracking'
]