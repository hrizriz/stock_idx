# Save this as: airflow/dags/utils/elliott_testing.py

"""
Elliott Wave Analysis Testing and Validation Module
Menyediakan fungsi untuk testing, validation, dan backtesting Elliott Wave analysis
"""

import pandas as pd
import numpy as np
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Any, Optional
import unittest
import json

from .elliott_wave import (
    ElliottWaveAnalyzer, 
    analyze_elliott_waves_for_symbol,
    elliott_wave_screening
)
from .elliott_config import ElliottWaveConfig, get_config, validate_config
from .database import get_database_connection, fetch_data

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ElliottWaveValidator:
    """
    Validator untuk memastikan kualitas dan akurasi Elliott Wave analysis
    """
    
    def __init__(self):
        self.config = ElliottWaveConfig()
        self.tolerance = 0.1  # 10% tolerance untuk Fibonacci levels
        
    def validate_wave_sequence(self, waves: List[Dict]) -> Dict[str, Any]:
        """
        Validate Elliott Wave sequence berdasarkan aturan teoritis
        
        Parameters:
        waves (List[Dict]): List of wave data
        
        Returns:
        Dict: Validation results
        """
        try:
            validation_results = {
                'is_valid': True,
                'errors': [],
                'warnings': [],
                'confidence_score': 100,
                'rule_checks': {}
            }
            
            if len(waves) < 5:
                validation_results['errors'].append('Insufficient waves for complete Elliott pattern')
                validation_results['is_valid'] = False
                return validation_results
            
            # Take last 5 waves for impulse pattern validation
            impulse_waves = waves[-5:]
            
            # Rule 1: Wave 2 never retraces more than 100% of Wave 1
            wave1 = impulse_waves[0]
            wave2 = impulse_waves[1]
            
            if wave1['direction'] == 'up' and wave2['direction'] == 'down':
                retracement = abs(wave2['end_price'] - wave2['start_price']) / abs(wave1['end_price'] - wave1['start_price'])
                if retracement > 1.0:
                    validation_results['errors'].append(f'Rule 1 violation: Wave 2 retraces {retracement:.1%} of Wave 1')
                    validation_results['confidence_score'] -= 30
                elif retracement > 0.9:
                    validation_results['warnings'].append(f'Rule 1 warning: Wave 2 deep retracement {retracement:.1%}')
                    validation_results['confidence_score'] -= 10
                
                validation_results['rule_checks']['rule_1'] = {
                    'passed': retracement <= 1.0,
                    'retracement_ratio': retracement,
                    'threshold': 1.0
                }
            
            # Rule 2: Wave 3 is never the shortest among waves 1, 3, and 5
            wave3 = impulse_waves[2]
            wave5 = impulse_waves[4] if len(impulse_waves) >= 5 else None
            
            magnitudes = [wave1['magnitude'], wave3['magnitude']]
            if wave5:
                magnitudes.append(wave5['magnitude'])
            
            if wave3['magnitude'] == min(magnitudes):
                validation_results['errors'].append('Rule 2 violation: Wave 3 is the shortest impulse wave')
                validation_results['confidence_score'] -= 25
            
            validation_results['rule_checks']['rule_2'] = {
                'passed': wave3['magnitude'] != min(magnitudes),
                'wave_magnitudes': {
                    'wave_1': wave1['magnitude'],
                    'wave_3': wave3['magnitude'],
                    'wave_5': wave5['magnitude'] if wave5 else None
                }
            }
            
            # Rule 3: Wave 4 never overlaps with Wave 1
            if len(impulse_waves) >= 4:
                wave4 = impulse_waves[3]
                
                if wave1['direction'] == 'up':
                    wave1_high = wave1['end_price']
                    wave4_low = min(wave4['start_price'], wave4['end_price'])
                    overlap = wave4_low < wave1_high
                else:
                    wave1_low = wave1['end_price']
                    wave4_high = max(wave4['start_price'], wave4['end_price'])
                    overlap = wave4_high > wave1_low
                
                if overlap:
                    validation_results['errors'].append('Rule 3 violation: Wave 4 overlaps with Wave 1')
                    validation_results['confidence_score'] -= 20
                
                validation_results['rule_checks']['rule_3'] = {
                    'passed': not overlap,
                    'wave_1_level': wave1_high if wave1['direction'] == 'up' else wave1_low,
                    'wave_4_level': wave4_low if wave1['direction'] == 'up' else wave4_high,
                    'overlap_detected': overlap
                }
            
            # Fibonacci validation
            fib_score = self._validate_fibonacci_relationships(impulse_waves)
            validation_results['fibonacci_score'] = fib_score
            validation_results['confidence_score'] += fib_score
            
            # Time relationships validation
            time_score = self._validate_time_relationships(impulse_waves)
            validation_results['time_score'] = time_score
            validation_results['confidence_score'] += time_score
            
            # Ensure confidence score bounds
            validation_results['confidence_score'] = max(0, min(100, validation_results['confidence_score']))
            
            # Overall validation
            if len(validation_results['errors']) == 0:
                validation_results['is_valid'] = True
            else:
                validation_results['is_valid'] = False
            
            return validation_results
            
        except Exception as e:
            logger.error(f"Error validating wave sequence: {str(e)}")
            return {
                'is_valid': False,
                'errors': [f'Validation error: {str(e)}'],
                'confidence_score': 0
            }
    
    def _validate_fibonacci_relationships(self, waves: List[Dict]) -> int:
        """Validate adherence to Fibonacci ratios"""
        try:
            score = 0
            
            if len(waves) < 3:
                return score
            
            # Common Fibonacci ratios
            fib_ratios = [0.236, 0.382, 0.5, 0.618, 0.786, 1.0, 1.272, 1.618, 2.618]
            
            # Check Wave 2 retracement
            if len(waves) >= 2:
                wave1_size = waves[0]['magnitude']
                wave2_size = waves[1]['magnitude']
                if wave1_size > 0:
                    ratio = wave2_size / wave1_size
                    for fib_ratio in [0.382, 0.5, 0.618]:
                        if abs(ratio - fib_ratio) <= self.tolerance:
                            score += 5
                            break
            
            # Check Wave 3 extension
            if len(waves) >= 3:
                wave1_size = waves[0]['magnitude']
                wave3_size = waves[2]['magnitude']
                if wave1_size > 0:
                    ratio = wave3_size / wave1_size
                    for fib_ratio in [1.618, 2.618]:
                        if abs(ratio - fib_ratio) <= self.tolerance:
                            score += 10
                            break
            
            # Check Wave 4 retracement
            if len(waves) >= 4:
                wave3_size = waves[2]['magnitude']
                wave4_size = waves[3]['magnitude']
                if wave3_size > 0:
                    ratio = wave4_size / wave3_size
                    for fib_ratio in [0.236, 0.382]:
                        if abs(ratio - fib_ratio) <= self.tolerance:
                            score += 5
                            break
            
            return min(score, 20)  # Cap at 20 points
            
        except Exception:
            return 0
    
    def _validate_time_relationships(self, waves: List[Dict]) -> int:
        """Validate time relationships between waves"""
        try:
            score = 0
            
            if len(waves) < 4:
                return score
            
            # Wave 4 typically takes longer than Wave 2
            wave2_duration = waves[1].get('duration_days', 0)
            wave4_duration = waves[3].get('duration_days', 0)
            
            if wave4_duration >= wave2_duration:
                score += 5
            
            # Wave 3 is often the longest in time
            if len(waves) >= 5:
                durations = [w.get('duration_days', 0) for w in waves[:5]]
                if waves[2].get('duration_days', 0) == max(durations):
                    score += 5
            
            return score
            
        except Exception:
            return 0

class ElliottWaveBacktester:
    """
    Backtesting engine untuk Elliott Wave analysis
    """
    
    def __init__(self, start_date: str, end_date: str):
        self.start_date = pd.to_datetime(start_date)
        self.end_date = pd.to_datetime(end_date)
        self.results = []
        
    def backtest_symbol(self, symbol: str, initial_capital: float = 100000) -> Dict[str, Any]:
        """
        Backtest Elliott Wave strategy untuk specific symbol
        
        Parameters:
        symbol (str): Stock symbol to backtest
        initial_capital (float): Initial capital amount
        
        Returns:
        Dict: Backtest results
        """
        try:
            logger.info(f"Starting backtest for {symbol} from {self.start_date} to {self.end_date}")
            
            # Get historical data
            conn = get_database_connection()
            query = f"""
            SELECT date, open_price, high, low, close, volume
            FROM public.daily_stock_summary
            WHERE symbol = '{symbol}'
            AND date BETWEEN '{self.start_date.date()}' AND '{self.end_date.date()}'
            ORDER BY date
            """
            
            df = pd.read_sql(query, conn)
            conn.close()
            
            if df.empty:
                return {'error': f'No data available for {symbol}'}
            
            # Initialize backtest variables
            capital = initial_capital
            position = 0
            trades = []
            daily_returns = []
            
            # Run analysis on rolling windows
            window_size = 60  # 60-day analysis window
            
            for i in range(window_size, len(df) - 5):  # Leave 5 days for trade execution
                # Get data window for analysis
                window_data = df.iloc[i-window_size:i].copy()
                current_date = df.iloc[i]['date']
                current_price = df.iloc[i]['close']
                
                # Run Elliott Wave analysis
                analyzer = ElliottWaveAnalyzer()
                window_data_with_swings = analyzer.identify_swing_points(window_data)
                waves = analyzer.extract_waves(window_data_with_swings)
                
                if len(waves) >= 5:
                    elliott_analysis = analyzer.identify_elliott_pattern(waves)
                    
                    if elliott_analysis.get('pattern_found', False):
                        trading_signals = analyzer.generate_trading_signals(
                            elliott_analysis, current_price, symbol
                        )
                        
                        signal = trading_signals.get('signal', 'HOLD')
                        confidence = trading_signals.get('confidence', 0)
                        
                        # Execute trades based on signals
                        if signal in ['STRONG_BUY', 'BUY'] and confidence >= 70 and position == 0:
                            # Enter long position
                            shares = int(capital * 0.95 / current_price)  # Use 95% of capital
                            if shares > 0:
                                position = shares
                                entry_price = current_price
                                entry_date = current_date
                                capital -= shares * current_price
                                
                                logger.info(f"BUY: {shares} shares of {symbol} at {current_price} on {current_date}")
                        
                        elif signal in ['STRONG_SELL', 'SELL'] and confidence >= 70 and position > 0:
                            # Exit long position
                            exit_price = current_price
                            exit_date = current_date
                            capital += position * current_price
                            
                            # Calculate trade performance
                            trade_return = (exit_price - entry_price) / entry_price
                            holding_days = (exit_date - entry_date).days
                            
                            trade = {
                                'symbol': symbol,
                                'entry_date': entry_date,
                                'exit_date': exit_date,
                                'entry_price': entry_price,
                                'exit_price': exit_price,
                                'shares': position,
                                'return_pct': trade_return * 100,
                                'holding_days': holding_days,
                                'signal_confidence': confidence,
                                'elliott_wave': elliott_analysis.get('current_wave'),
                                'pattern_confidence': elliott_analysis.get('confidence', 0)
                            }
                            
                            trades.append(trade)
                            position = 0
                            
                            logger.info(f"SELL: {symbol} at {current_price} on {current_date}, Return: {trade_return:.2%}")
                
                # Calculate daily portfolio value
                portfolio_value = capital + (position * current_price if position > 0 else 0)
                daily_return = (portfolio_value / initial_capital - 1) * 100
                daily_returns.append({
                    'date': current_date,
                    'portfolio_value': portfolio_value,
                    'daily_return_pct': daily_return
                })
            
            # Calculate final results
            final_capital = capital + (position * df.iloc[-1]['close'] if position > 0 else 0)
            total_return = (final_capital / initial_capital - 1) * 100
            
            # Performance metrics
            if trades:
                winning_trades = [t for t in trades if t['return_pct'] > 0]
                losing_trades = [t for t in trades if t['return_pct'] <= 0]
                
                win_rate = len(winning_trades) / len(trades) * 100
                avg_win = np.mean([t['return_pct'] for t in winning_trades]) if winning_trades else 0
                avg_loss = np.mean([t['return_pct'] for t in losing_trades]) if losing_trades else 0
                profit_factor = abs(sum([t['return_pct'] for t in winning_trades]) / 
                                  sum([t['return_pct'] for t in losing_trades])) if losing_trades else float('inf')
                
                # Calculate Sharpe ratio
                returns = [d['daily_return_pct'] for d in daily_returns]
                if len(returns) > 1:
                    sharpe_ratio = np.mean(returns) / np.std(returns) * np.sqrt(252)
                else:
                    sharpe_ratio = 0
                
                # Maximum drawdown
                portfolio_values = [d['portfolio_value'] for d in daily_returns]
                peak = np.maximum.accumulate(portfolio_values)
                drawdown = (portfolio_values - peak) / peak
                max_drawdown = np.min(drawdown) * 100
            else:
                win_rate = avg_win = avg_loss = profit_factor = sharpe_ratio = max_drawdown = 0
            
            backtest_results = {
                'symbol': symbol,
                'start_date': self.start_date.strftime('%Y-%m-%d'),
                'end_date': self.end_date.strftime('%Y-%m-%d'),
                'initial_capital': initial_capital,
                'final_capital': final_capital,
                'total_return_pct': total_return,
                'total_trades': len(trades),
                'winning_trades': len(winning_trades) if trades else 0,
                'losing_trades': len(losing_trades) if trades else 0,
                'win_rate_pct': win_rate,
                'avg_win_pct': avg_win,
                'avg_loss_pct': avg_loss,
                'profit_factor': profit_factor,
                'sharpe_ratio': sharpe_ratio,
                'max_drawdown_pct': max_drawdown,
                'trades': trades,
                'daily_performance': daily_returns
            }
            
            logger.info(f"Backtest completed for {symbol}: {total_return:.2f}% return, {win_rate:.1f}% win rate")
            
            return backtest_results
            
        except Exception as e:
            logger.error(f"Error in backtesting {symbol}: {str(e)}")
            return {'error': str(e), 'symbol': symbol}
    
    def run_portfolio_backtest(self, symbols: List[str], equal_weight: bool = True) -> Dict[str, Any]:
        """
        Run backtest pada portfolio multiple symbols
        
        Parameters:
        symbols (List[str]): List of symbols to backtest
        equal_weight (bool): Whether to use equal weighting
        
        Returns:
        Dict: Portfolio backtest results
        """
        try:
            individual_results = []
            
            for symbol in symbols:
                result = self.backtest_symbol(symbol)
                if 'error' not in result:
                    individual_results.append(result)
            
            if not individual_results:
                return {'error': 'No successful backtests'}
            
            # Calculate portfolio metrics
            total_capital = sum([r['initial_capital'] for r in individual_results])
            final_capital = sum([r['final_capital'] for r in individual_results])
            portfolio_return = (final_capital / total_capital - 1) * 100
            
            # Aggregate trade statistics
            all_trades = []
            for result in individual_results:
                all_trades.extend(result['trades'])
            
            if all_trades:
                winning_trades = [t for t in all_trades if t['return_pct'] > 0]
                portfolio_win_rate = len(winning_trades) / len(all_trades) * 100
                portfolio_avg_return = np.mean([t['return_pct'] for t in all_trades])
            else:
                portfolio_win_rate = portfolio_avg_return = 0
            
            portfolio_results = {
                'portfolio_symbols': symbols,
                'portfolio_return_pct': portfolio_return,
                'portfolio_win_rate_pct': portfolio_win_rate,
                'portfolio_avg_return_pct': portfolio_avg_return,
                'total_trades': len(all_trades),
                'individual_results': individual_results,
                'best_performer': max(individual_results, key=lambda x: x['total_return_pct'])['symbol'],
                'worst_performer': min(individual_results, key=lambda x: x['total_return_pct'])['symbol'],
                'avg_sharpe_ratio': np.mean([r['sharpe_ratio'] for r in individual_results]),
                'avg_max_drawdown': np.mean([r['max_drawdown_pct'] for r in individual_results])
            }
            
            return portfolio_results
            
        except Exception as e:
            logger.error(f"Error in portfolio backtest: {str(e)}")
            return {'error': str(e)}

class ElliottWaveTestSuite(unittest.TestCase):
    """
    Unit tests untuk Elliott Wave Analysis
    """
    
    def setUp(self):
        """Setup test environment"""
        self.analyzer = ElliottWaveAnalyzer()
        self.validator = ElliottWaveValidator()
        self.config = ElliottWaveConfig()
        
        # Sample wave data for testing
        self.sample_waves = [
            {
                'wave_number': 1, 'direction': 'up', 'magnitude': 0.10,
                'start_price': 1000, 'end_price': 1100, 'duration_days': 5
            },
            {
                'wave_number': 2, 'direction': 'down', 'magnitude': 0.06,
                'start_price': 1100, 'end_price': 1034, 'duration_days': 3
            },
            {
                'wave_number': 3, 'direction': 'up', 'magnitude': 0.16,
                'start_price': 1034, 'end_price': 1200, 'duration_days': 8
            },
            {
                'wave_number': 4, 'direction': 'down', 'magnitude': 0.04,
                'start_price': 1200, 'end_price': 1152, 'duration_days': 4
            },
            {
                'wave_number': 5, 'direction': 'up', 'magnitude': 0.08,
                'start_price': 1152, 'end_price': 1244, 'duration_days': 6
            }
        ]
    
    def test_config_validation(self):
        """Test configuration validation"""
        self.assertTrue(validate_config())
    
    def test_fibonacci_levels(self):
        """Test Fibonacci level calculations"""
        fib_levels = self.config.FIBONACCI_LEVELS
        
        # Test that Fibonacci levels are properly defined
        self.assertIn('retracement', fib_levels)
        self.assertIn('extension', fib_levels)
        self.assertIn(0.618, fib_levels['retracement'])
        self.assertIn(1.618, fib_levels['extension'])
    
    def test_wave_sequence_validation(self):
        """Test Elliott Wave sequence validation"""
        validation = self.validator.validate_wave_sequence(self.sample_waves)
        
        self.assertIsInstance(validation, dict)
        self.assertIn('is_valid', validation)
        self.assertIn('confidence_score', validation)
        self.assertIn('rule_checks', validation)
    
    def test_elliott_rules(self):
        """Test specific Elliott Wave rules"""
        validation = self.validator.validate_wave_sequence(self.sample_waves)
        
        # Check that all rules are tested
        self.assertIn('rule_1', validation['rule_checks'])
        self.assertIn('rule_2', validation['rule_checks'])
        self.assertIn('rule_3', validation['rule_checks'])
    
    def test_trading_signal_generation(self):
        """Test trading signal generation"""
        elliott_analysis = {
            'pattern_found': True,
            'current_wave': '2',
            'confidence': 80,
            'fibonacci_levels': {
                'retracement': {'61.8%': 1000},
                'extension': {'161.8%': 1200}
            }
        }
        
        signals = self.analyzer.generate_trading_signals(elliott_analysis, 1050)
        
        self.assertIsInstance(signals, dict)
        self.assertIn('signal', signals)
        self.assertIn('confidence', signals)
        self.assertIn('reasoning', signals)
    
    def test_data_quality_checks(self):
        """Test data quality validation"""
        # Test with insufficient data
        short_waves = self.sample_waves[:2]
        validation = self.validator.validate_wave_sequence(short_waves)
        
        self.assertFalse(validation['is_valid'])
        self.assertIn('Insufficient waves', validation['errors'][0])
    
    def test_edge_cases(self):
        """Test edge cases and error handling"""
        # Test with empty data
        empty_validation = self.validator.validate_wave_sequence([])
        self.assertFalse(empty_validation['is_valid'])
        
        # Test with invalid wave data
        invalid_waves = [{'invalid': 'data'}]
        try:
            self.analyzer.generate_trading_signals({}, 1000)
        except Exception as e:
            self.assertIsInstance(e, Exception)

def run_elliott_tests():
    """
    Run complete Elliott Wave test suite
    
    Returns:
    Dict: Test results summary
    """
    try:
        # Run unit tests
        test_suite = unittest.TestLoader().loadTestsFromTestCase(ElliottWaveTestSuite)
        test_runner = unittest.TextTestRunner(verbosity=2)
        test_result = test_runner.run(test_suite)
        
        # Configuration validation
        config_valid = validate_config()
        
        # Performance test dengan sample data
        analyzer = ElliottWaveAnalyzer()
        validator = ElliottWaveValidator()
        
        # Create sample market data
        dates = pd.date_range(start='2024-01-01', end='2024-03-01', freq='D')
        sample_data = pd.DataFrame({
            'date': dates,
            'high': np.random.normal(1000, 50, len(dates)).cumsum() + 1000,
            'low': np.random.normal(1000, 50, len(dates)).cumsum() + 950,
            'close': np.random.normal(1000, 50, len(dates)).cumsum() + 980,
            'volume': np.random.randint(100000, 1000000, len(dates))
        })
        
        # Test swing point detection
        df_with_swings = analyzer.identify_swing_points(sample_data)
        waves = analyzer.extract_waves(df_with_swings)
        
        performance_test = {
            'swing_points_detected': len(df_with_swings[df_with_swings['pivot_type'] != 'none']),
            'waves_extracted': len(waves),
            'data_processing_successful': len(waves) > 0
        }
        
        # Summarize results
        test_summary = {
            'timestamp': datetime.now().isoformat(),
            'unit_tests': {
                'tests_run': test_result.testsRun,
                'failures': len(test_result.failures),
                'errors': len(test_result.errors),
                'success_rate': (test_result.testsRun - len(test_result.failures) - len(test_result.errors)) / test_result.testsRun * 100
            },
            'configuration_validation': config_valid,
            'performance_test': performance_test,
            'overall_status': 'PASS' if (test_result.testsRun > 0 and 
                                       len(test_result.failures) == 0 and 
                                       len(test_result.errors) == 0 and 
                                       config_valid) else 'FAIL'
        }
        
        logger.info(f"Elliott Wave testing completed: {test_summary['overall_status']}")
        
        return test_summary
        
    except Exception as e:
        logger.error(f"Error running Elliott Wave tests: {str(e)}")
        return {
            'timestamp': datetime.now().isoformat(),
            'error': str(e),
            'overall_status': 'ERROR'
        }

def run_validation_report() -> str:
    """
    Generate comprehensive validation report
    
    Returns:
    str: Validation report
    """
    try:
        test_results = run_elliott_tests()
        
        report = "ELLIOTT WAVE ANALYSIS VALIDATION REPORT\n"
        report += "=" * 50 + "\n\n"
        
        report += f"Validation Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"
        
        # Unit Tests Results
        unit_tests = test_results.get('unit_tests', {})
        report += "UNIT TESTS RESULTS:\n"
        report += f"- Tests Run: {unit_tests.get('tests_run', 0)}\n"
        report += f"- Failures: {unit_tests.get('failures', 0)}\n"
        report += f"- Errors: {unit_tests.get('errors', 0)}\n"
        report += f"- Success Rate: {unit_tests.get('success_rate', 0):.1f}%\n\n"
        
        # Configuration Validation
        config_status = "PASS" if test_results.get('configuration_validation', False) else "FAIL"
        report += f"CONFIGURATION VALIDATION: {config_status}\n\n"
        
        # Performance Tests
        perf_test = test_results.get('performance_test', {})
        report += "PERFORMANCE TESTS:\n"
        report += f"- Swing Points Detection: {'PASS' if perf_test.get('swing_points_detected', 0) > 0 else 'FAIL'}\n"
        report += f"- Wave Extraction: {'PASS' if perf_test.get('waves_extracted', 0) > 0 else 'FAIL'}\n"
        report += f"- Data Processing: {'PASS' if perf_test.get('data_processing_successful', False) else 'FAIL'}\n\n"
        
        # Overall Status
        overall_status = test_results.get('overall_status', 'UNKNOWN')
        report += f"OVERALL STATUS: {overall_status}\n\n"
        
        if overall_status == 'PASS':
            report += "✅ Elliott Wave Analysis system is ready for production use.\n"
        else:
            report += "❌ Elliott Wave Analysis system requires attention before production use.\n"
        
        report += "\nRecommendations:\n"
        if unit_tests.get('success_rate', 0) < 100:
            report += "- Review and fix failing unit tests\n"
        if not test_results.get('configuration_validation', False):
            report += "- Fix configuration validation issues\n"
        if perf_test.get('waves_extracted', 0) == 0:
            report += "- Check wave extraction algorithm\n"
        
        report += "\nFor detailed logs, check the application log files.\n"
        
        return report
        
    except Exception as e:
        return f"Error generating validation report: {str(e)}"

# Quick validation function for DAG testing
def quick_elliott_validation() -> bool:
    """
    Quick validation untuk DAG health check
    
    Returns:
    bool: True if validation passes
    """
    try:
        # Test basic configuration
        config_valid = validate_config()
        
        # Test basic analyzer functionality
        analyzer = ElliottWaveAnalyzer()
        
        # Test with minimal sample data
        sample_data = pd.DataFrame({
            'date': pd.date_range('2024-01-01', '2024-01-10'),
            'high': [1000, 1050, 1020, 1080, 1070, 1100, 1090, 1120, 1110, 1130],
            'low': [980, 1020, 1000, 1060, 1050, 1080, 1070, 1100, 1090, 1110],
            'close': [990, 1040, 1010, 1070, 1060, 1090, 1080, 1110, 1100, 1125],
            'volume': [100000] * 10
        })
        
        df_with_swings = analyzer.identify_swing_points(sample_data)
        basic_functionality = len(df_with_swings) > 0
        
        return config_valid and basic_functionality
        
    except Exception as e:
        logger.error(f"Quick validation failed: {str(e)}")
        return False

# Export functions
__all__ = [
    'ElliottWaveValidator',
    'ElliottWaveBacktester',
    'ElliottWaveTestSuite',
    'run_elliott_tests',
    'run_validation_report',
    'quick_elliott_validation'
]