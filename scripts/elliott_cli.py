#!/usr/bin/env python3
# Save this as: scripts/elliott_cli.py

"""
Elliott Wave Analysis Command Line Interface
Tool untuk menjalankan Elliott Wave analysis secara manual
"""

import sys
import os
import argparse
import json
import pandas as pd
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
import logging
from tabulate import tabulate
import colorama
from colorama import Fore, Back, Style

# Add the parent directory to Python path untuk import modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import Elliott Wave modules
try:
    from airflow.dags.utils.elliott_wave import (
        analyze_elliott_waves_for_symbol,
        elliott_wave_screening,
        ElliottWaveAnalyzer
    )
    from airflow.dags.utils.elliott_data_quality import (
        ElliottDataQualityChecker,
        run_data_quality_check
    )
    from airflow.dags.utils.elliott_testing import (
        run_elliott_tests,
        run_validation_report,
        ElliottWaveBacktester
    )
    from airflow.dags.utils.elliott_monitoring import (
        run_elliott_monitoring,
        run_performance_tracking
    )
    from airflow.dags.utils.elliott_config import validate_config
except ImportError as e:
    print(f"Error importing Elliott Wave modules: {e}")
    print("Please ensure you're running from the correct directory and all dependencies are installed")
    sys.exit(1)

# Initialize colorama
colorama.init()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('elliott_cli.log')
    ]
)
logger = logging.getLogger(__name__)

class ElliottWaveCLI:
    """
    Command Line Interface untuk Elliott Wave Analysis
    """
    
    def __init__(self):
        self.analyzer = ElliottWaveAnalyzer()
        self.quality_checker = ElliottDataQualityChecker()
        
    def print_banner(self):
        """Print CLI banner"""
        banner = f"""
{Fore.CYAN}
╔══════════════════════════════════════════════════════════════════════════════╗
║                          ELLIOTT WAVE ANALYSIS CLI                          ║
║                     Indonesian Stock Market Technical Analysis              ║
╚══════════════════════════════════════════════════════════════════════════════╝
{Style.RESET_ALL}
        """
        print(banner)
    
    def analyze_single_symbol(self, symbol: str, lookback_days: int = 150, save_results: bool = False) -> Dict[str, Any]:
        """
        Analyze single symbol dengan Elliott Wave
        
        Parameters:
        symbol (str): Stock symbol
        lookback_days (int): Days of historical data
        save_results (bool): Save results to file
        
        Returns:
        Dict: Analysis results
        """
        try:
            print(f"\n{Fore.YELLOW}🌊 Analyzing Elliott Wave patterns for {symbol}...{Style.RESET_ALL}")
            
            # Run analysis
            analysis = analyze_elliott_waves_for_symbol(symbol, lookback_days)
            
            if 'error' in analysis:
                print(f"{Fore.RED}❌ Error analyzing {symbol}: {analysis['error']}{Style.RESET_ALL}")
                return analysis
            
            # Display results
            self._display_analysis_results(analysis)
            
            # Save results if requested
            if save_results:
                filename = f"elliott_analysis_{symbol}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
                with open(filename, 'w') as f:
                    json.dump(analysis, f, indent=2, default=str)
                print(f"\n{Fore.GREEN}💾 Results saved to {filename}{Style.RESET_ALL}")
            
            return analysis
            
        except Exception as e:
            error_msg = f"Error analyzing {symbol}: {str(e)}"
            print(f"{Fore.RED}❌ {error_msg}{Style.RESET_ALL}")
            logger.error(error_msg)
            return {'error': error_msg}
    
    def run_screening(self, max_symbols: int = 30, min_confidence: int = 60, save_results: bool = False) -> List[Dict[str, Any]]:
        """
        Run Elliott Wave screening
        
        Parameters:
        max_symbols (int): Maximum symbols to screen
        min_confidence (int): Minimum confidence threshold
        save_results (bool): Save results to file
        
        Returns:
        List: Screening results
        """
        try:
            print(f"\n{Fore.YELLOW}🔍 Running Elliott Wave screening on {max_symbols} symbols...{Style.RESET_ALL}")
            
            # Run screening
            opportunities = elliott_wave_screening(max_symbols=max_symbols)
            
            if not opportunities:
                print(f"{Fore.YELLOW}⚠️  No Elliott Wave opportunities found{Style.RESET_ALL}")
                return []
            
            # Filter by confidence
            filtered_opportunities = [
                opp for opp in opportunities 
                if opp.get('signal_confidence', 0) >= min_confidence
            ]
            
            if not filtered_opportunities:
                print(f"{Fore.YELLOW}⚠️  No opportunities found with confidence >= {min_confidence}%{Style.RESET_ALL}")
                return []
            
            # Display results
            self._display_screening_results(filtered_opportunities)
            
            # Save results if requested
            if save_results:
                filename = f"elliott_screening_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
                with open(filename, 'w') as f:
                    json.dump(filtered_opportunities, f, indent=2, default=str)
                print(f"\n{Fore.GREEN}💾 Screening results saved to {filename}{Style.RESET_ALL}")
            
            return filtered_opportunities
            
        except Exception as e:
            error_msg = f"Error running screening: {str(e)}"
            print(f"{Fore.RED}❌ {error_msg}{Style.RESET_ALL}")
            logger.error(error_msg)
            return []
    
    def check_data_quality(self, symbols: List[str], save_results: bool = False) -> Dict[str, Any]:
        """
        Check data quality for symbols
        
        Parameters:
        symbols (List[str]): List of symbols to check
        save_results (bool): Save results to file
        
        Returns:
        Dict: Quality check results
        """
        try:
            print(f"\n{Fore.YELLOW}🔍 Checking data quality for {len(symbols)} symbols...{Style.RESET_ALL}")
            
            # Run quality check
            results = run_data_quality_check(symbols, save_to_db=False)
            
            if 'error' in results:
                print(f"{Fore.RED}❌ Error in data quality check: {results['error']}{Style.RESET_ALL}")
                return results
            
            # Display results
            self._display_quality_results(results)
            
            # Save results if requested
            if save_results:
                filename = f"data_quality_check_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
                with open(filename, 'w') as f:
                    json.dump(results, f, indent=2, default=str)
                print(f"\n{Fore.GREEN}💾 Quality check results saved to {filename}{Style.RESET_ALL}")
            
            return results
            
        except Exception as e:
            error_msg = f"Error checking data quality: {str(e)}"
            print(f"{Fore.RED}❌ {error_msg}{Style.RESET_ALL}")
            logger.error(error_msg)
            return {'error': error_msg}
    
    def run_backtest(self, symbol: str, start_date: str, end_date: str, initial_capital: float = 100000) -> Dict[str, Any]:
        """
        Run Elliott Wave backtest
        
        Parameters:
        symbol (str): Symbol to backtest
        start_date (str): Start date (YYYY-MM-DD)
        end_date (str): End date (YYYY-MM-DD)
        initial_capital (float): Initial capital
        
        Returns:
        Dict: Backtest results
        """
        try:
            print(f"\n{Fore.YELLOW}📈 Running Elliott Wave backtest for {symbol} ({start_date} to {end_date})...{Style.RESET_ALL}")
            
            # Run backtest
            backtester = ElliottWaveBacktester(start_date, end_date)
            results = backtester.backtest_symbol(symbol, initial_capital)
            
            if 'error' in results:
                print(f"{Fore.RED}❌ Backtest error: {results['error']}{Style.RESET_ALL}")
                return results
            
            # Display results
            self._display_backtest_results(results)
            
            return results
            
        except Exception as e:
            error_msg = f"Error running backtest: {str(e)}"
            print(f"{Fore.RED}❌ {error_msg}{Style.RESET_ALL}")
            logger.error(error_msg)
            return {'error': error_msg}
    
    def run_validation(self) -> str:
        """
        Run system validation
        
        Returns:
        str: Validation report
        """
        try:
            print(f"\n{Fore.YELLOW}🔧 Running Elliott Wave system validation...{Style.RESET_ALL}")
            
            # Run validation
            report = run_validation_report()
            
            # Display report
            print(f"\n{Fore.CYAN}=== VALIDATION REPORT ==={Style.RESET_ALL}")
            print(report)
            
            # Save report
            filename = f"validation_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
            with open(filename, 'w') as f:
                f.write(report)
            print(f"\n{Fore.GREEN}💾 Validation report saved to {filename}{Style.RESET_ALL}")
            
            return report
            
        except Exception as e:
            error_msg = f"Error running validation: {str(e)}"
            print(f"{Fore.RED}❌ {error_msg}{Style.RESET_ALL}")
            logger.error(error_msg)
            return error_msg
    
    def _display_analysis_results(self, analysis: Dict[str, Any]) -> None:
        """Display Elliott Wave analysis results"""
        try:
            symbol = analysis['symbol']
            current_price = analysis['current_price']
            elliott_pattern = analysis.get('elliott_pattern', {})
            trading_signals = analysis.get('trading_signals', {})
            
            print(f"\n{Fore.CYAN}=== ELLIOTT WAVE ANALYSIS: {symbol} ==={Style.RESET_ALL}")
            print(f"Current Price: Rp{current_price:,.0f}")
            print(f"Analysis Date: {analysis.get('analysis_date', 'N/A')}")
            
            # Pattern Information
            if elliott_pattern.get('pattern_found', False):
                print(f"\n{Fore.GREEN}📊 PATTERN DETECTED{Style.RESET_ALL}")
                print(f"Pattern Type: {elliott_pattern.get('pattern_type', 'N/A')}")
                print(f"Current Wave: {elliott_pattern.get('current_wave', 'N/A')}")
                print(f"Next Expected: {elliott_pattern.get('next_expected', 'N/A')}")
                print(f"Pattern Confidence: {elliott_pattern.get('confidence', 0)}%")
                
                # Rules validation
                rules = elliott_pattern.get('rules_validation', {})
                if rules:
                    passed = rules.get('passed', [])
                    failed = rules.get('failed', [])
                    
                    if passed:
                        print(f"\n{Fore.GREEN}✅ Rules Passed:{Style.RESET_ALL}")
                        for rule in passed:
                            print(f"  • {rule}")
                    
                    if failed:
                        print(f"\n{Fore.RED}❌ Rules Failed:{Style.RESET_ALL}")
                        for rule in failed:
                            print(f"  • {rule}")
            else:
                print(f"\n{Fore.YELLOW}⚠️  No clear Elliott Wave pattern detected{Style.RESET_ALL}")
            
            # Trading Signals
            signal = trading_signals.get('signal', 'HOLD')
            confidence = trading_signals.get('confidence', 0)
            
            signal_color = Fore.GREEN if signal in ['STRONG_BUY', 'BUY'] else Fore.RED if signal in ['STRONG_SELL', 'SELL'] else Fore.YELLOW
            
            print(f"\n{signal_color}🎯 TRADING SIGNAL: {signal}{Style.RESET_ALL}")
            print(f"Signal Confidence: {confidence}%")
            
            if trading_signals.get('entry_price'):
                print(f"Entry Price: Rp{trading_signals['entry_price']:,.0f}")
            if trading_signals.get('stop_loss'):
                print(f"Stop Loss: Rp{trading_signals['stop_loss']:,.0f}")
            if trading_signals.get('take_profit_1'):
                print(f"Take Profit 1: Rp{trading_signals['take_profit_1']:,.0f}")
            if trading_signals.get('take_profit_2'):
                print(f"Take Profit 2: Rp{trading_signals['take_profit_2']:,.0f}")
            if trading_signals.get('risk_reward_ratio'):
                print(f"Risk/Reward Ratio: 1:{trading_signals['risk_reward_ratio']:.1f}")
            
            print(f"Position Size: {trading_signals.get('position_size_recommendation', 'Standard')}")
            print(f"Time Horizon: {trading_signals.get('time_horizon', 'Medium')}")
            
            # Reasoning
            reasoning = trading_signals.get('reasoning', [])
            if reasoning:
                print(f"\n{Fore.CYAN}💡 Analysis Reasoning:{Style.RESET_ALL}")
                for reason in reasoning:
                    print(f"  • {reason}")
            
            # Risk factors
            risk_factors = trading_signals.get('risk_factors', [])
            if risk_factors:
                print(f"\n{Fore.YELLOW}⚠️  Risk Factors:{Style.RESET_ALL}")
                for risk in risk_factors:
                    print(f"  • {risk}")
            
        except Exception as e:
            print(f"{Fore.RED}Error displaying analysis results: {str(e)}{Style.RESET_ALL}")
    
    def _display_screening_results(self, opportunities: List[Dict[str, Any]]) -> None:
        """Display screening results in table format"""
        try:
            print(f"\n{Fore.CYAN}=== ELLIOTT WAVE SCREENING RESULTS ==={Style.RESET_ALL}")
            print(f"Found {len(opportunities)} high-quality opportunities")
            
            # Prepare table data
            table_data = []
            headers = ["Symbol", "Signal", "Signal Conf.", "Pattern Conf.", "Current Wave", "Next Expected", "Current Price", "Entry Price", "R/R Ratio"]
            
            for opp in opportunities[:15]:  # Show top 15
                signal_emoji = "🟢" if opp.get('signal') in ['STRONG_BUY', 'BUY'] else "🔴" if opp.get('signal') in ['STRONG_SELL', 'SELL'] else "🟡"
                
                table_data.append([
                    f"{signal_emoji} {opp['symbol']}",
                    opp.get('signal', 'HOLD'),
                    f"{opp.get('signal_confidence', 0)}%",
                    f"{opp.get('pattern_confidence', 0)}%",
                    opp.get('current_wave', 'N/A'),
                    opp.get('next_expected', 'N/A'),
                    f"Rp{opp.get('current_price', 0):,.0f}",
                    f"Rp{opp.get('entry_price', 0):,.0f}" if opp.get('entry_price') else 'N/A',
                    f"1:{opp.get('risk_reward_ratio', 0):.1f}" if opp.get('risk_reward_ratio') else 'N/A'
                ])
            
            print(tabulate(table_data, headers=headers, tablefmt="grid"))
            
            # Summary statistics
            buy_signals = len([o for o in opportunities if o.get('signal') in ['STRONG_BUY', 'BUY']])
            sell_signals = len([o for o in opportunities if o.get('signal') in ['STRONG_SELL', 'SELL']])
            avg_confidence = sum([o.get('signal_confidence', 0) for o in opportunities]) / len(opportunities)
            
            print(f"\n{Fore.CYAN}📊 Summary:{Style.RESET_ALL}")
            print(f"  • Buy Signals: {buy_signals}")
            print(f"  • Sell Signals: {sell_signals}")
            print(f"  • Average Confidence: {avg_confidence:.1f}%")
            print(f"  • High Confidence (>80%): {len([o for o in opportunities if o.get('signal_confidence', 0) > 80])}")
            
        except Exception as e:
            print(f"{Fore.RED}Error displaying screening results: {str(e)}{Style.RESET_ALL}")
    
    def _display_quality_results(self, results: Dict[str, Any]) -> None:
        """Display data quality results"""
        try:
            print(f"\n{Fore.CYAN}=== DATA QUALITY CHECK RESULTS ==={Style.RESET_ALL}")
            
            summary = results.get('quality_summary', {})
            statistics = results.get('overall_statistics', {})
            
            print(f"Symbols Checked: {summary.get('total_symbols', 0)}")
            print(f"Elliott Wave Ready: {summary.get('elliott_ready', 0)}")
            print(f"Average Quality Score: {summary.get('average_score', 0):.1f}%")
            
            # Quality distribution
            print(f"\n{Fore.CYAN}📊 Quality Distribution:{Style.RESET_ALL}")
            print(f"  • Excellent (95-100%): {statistics.get('symbols_with_excellent_quality', 0)}")
            print(f"  • Good (85-94%): {statistics.get('symbols_with_good_quality', 0)}")
            print(f"  • Fair (70-84%): {statistics.get('symbols_with_fair_quality', 0)}")
            print(f"  • Poor (50-69%): {statistics.get('symbols_with_poor_quality', 0)}")
            print(f"  • Unusable (<50%): {statistics.get('symbols_unusable', 0)}")
            
            # Elliott Wave ready symbols
            ready_symbols = results.get('elliott_ready_symbols', [])
            if ready_symbols:
                print(f"\n{Fore.GREEN}✅ Elliott Wave Ready Symbols:{Style.RESET_ALL}")
                for symbol in ready_symbols[:10]:  # Show first 10
                    print(f"  • {symbol}")
                if len(ready_symbols) > 10:
                    print(f"  ... and {len(ready_symbols) - 10} more")
            
            # Symbols with issues
            symbols_with_issues = results.get('symbols_with_issues', [])
            if symbols_with_issues:
                print(f"\n{Fore.YELLOW}⚠️  Symbols Needing Attention:{Style.RESET_ALL}")
                for item in symbols_with_issues[:5]:  # Show first 5
                    symbol = item['symbol']
                    issues_count = item['issues_count']
                    print(f"  • {symbol}: {issues_count} issues")
            
        except Exception as e:
            print(f"{Fore.RED}Error displaying quality results: {str(e)}{Style.RESET_ALL}")
    
    def _display_backtest_results(self, results: Dict[str, Any]) -> None:
        """Display backtest results"""
        try:
            print(f"\n{Fore.CYAN}=== BACKTEST RESULTS ==={Style.RESET_ALL}")
            
            symbol = results['symbol']
            initial_capital = results['initial_capital']
            final_capital = results['final_capital']
            total_return = results['total_return_pct']
            
            print(f"Symbol: {symbol}")
            print(f"Period: {results['start_date']} to {results['end_date']}")
            print(f"Initial Capital: Rp{initial_capital:,.0f}")
            print(f"Final Capital: Rp{final_capital:,.0f}")
            
            return_color = Fore.GREEN if total_return > 0 else Fore.RED
            print(f"Total Return: {return_color}{total_return:.2f}%{Style.RESET_ALL}")
            
            # Trading statistics
            print(f"\n{Fore.CYAN}📊 Trading Statistics:{Style.RESET_ALL}")
            print(f"Total Trades: {results['total_trades']}")
            print(f"Winning Trades: {results['winning_trades']}")
            print(f"Losing Trades: {results['losing_trades']}")
            print(f"Win Rate: {results['win_rate_pct']:.1f}%")
            print(f"Average Win: {results['avg_win_pct']:.2f}%")
            print(f"Average Loss: {results['avg_loss_pct']:.2f}%")
            print(f"Profit Factor: {results['profit_factor']:.2f}")
            print(f"Sharpe Ratio: {results['sharpe_ratio']:.2f}")
            print(f"Max Drawdown: {results['max_drawdown_pct']:.2f}%")
            
            # Show recent trades
            trades = results.get('trades', [])
            if trades:
                print(f"\n{Fore.CYAN}🔄 Recent Trades:{Style.RESET_ALL}")
                for trade in trades[-5:]:  # Show last 5 trades
                    return_pct = trade['return_pct']
                    trade_color = Fore.GREEN if return_pct > 0 else Fore.RED
                    print(f"  • {trade['entry_date']} → {trade['exit_date']}: {trade_color}{return_pct:.2f}%{Style.RESET_ALL}")
            
        except Exception as e:
            print(f"{Fore.RED}Error displaying backtest results: {str(e)}{Style.RESET_ALL}")

def create_parser() -> argparse.ArgumentParser:
    """Create argument parser"""
    parser = argparse.ArgumentParser(
        description='Elliott Wave Analysis CLI Tool',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Analyze single symbol
  python elliott_cli.py analyze BBCA
  
  # Run screening
  python elliott_cli.py screen --max-symbols 50 --min-confidence 70
  
  # Check data quality
  python elliott_cli.py quality --symbols BBCA,BBRI,BMRI
  
  # Run backtest
  python elliott_cli.py backtest BBCA --start-date 2024-01-01 --end-date 2024-06-01
  
  # Run system validation
  python elliott_cli.py validate
        """
    )
    
    subparsers = parser.add_subparsers(dest='command', help='Available commands')
    
    # Analyze command
    analyze_parser = subparsers.add_parser('analyze', help='Analyze single symbol')
    analyze_parser.add_argument('symbol', help='Stock symbol to analyze')
    analyze_parser.add_argument('--lookback-days', type=int, default=150, help='Days of historical data')
    analyze_parser.add_argument('--save', action='store_true', help='Save results to file')
    
    # Screening command
    screen_parser = subparsers.add_parser('screen', help='Run Elliott Wave screening')
    screen_parser.add_argument('--max-symbols', type=int, default=30, help='Maximum symbols to screen')
    screen_parser.add_argument('--min-confidence', type=int, default=60, help='Minimum confidence threshold')
    screen_parser.add_argument('--save', action='store_true', help='Save results to file')
    
    # Quality check command
    quality_parser = subparsers.add_parser('quality', help='Check data quality')
    quality_parser.add_argument('--symbols', help='Comma-separated list of symbols (default: top stocks)')
    quality_parser.add_argument('--save', action='store_true', help='Save results to file')
    
    # Backtest command
    backtest_parser = subparsers.add_parser('backtest', help='Run backtest')
    backtest_parser.add_argument('symbol', help='Symbol to backtest')
    backtest_parser.add_argument('--start-date', required=True, help='Start date (YYYY-MM-DD)')
    backtest_parser.add_argument('--end-date', required=True, help='End date (YYYY-MM-DD)')
    backtest_parser.add_argument('--initial-capital', type=float, default=100000, help='Initial capital')
    
    # Validation command
    subparsers.add_parser('validate', help='Run system validation')
    
    return parser

def main():
    """Main CLI function"""
    parser = create_parser()
    args = parser.parse_args()
    
    # Initialize CLI
    cli = ElliottWaveCLI()
    cli.print_banner()
    
    if not args.command:
        parser.print_help()
        return
    
    try:
        if args.command == 'analyze':
            cli.analyze_single_symbol(
                symbol=args.symbol.upper(),
                lookback_days=args.lookback_days,
                save_results=args.save
            )
        
        elif args.command == 'screen':
            cli.run_screening(
                max_symbols=args.max_symbols,
                min_confidence=args.min_confidence,
                save_results=args.save
            )
        
        elif args.command == 'quality':
            symbols = None
            if args.symbols:
                symbols = [s.strip().upper() for s in args.symbols.split(',')]
            
            cli.check_data_quality(
                symbols=symbols,
                save_results=args.save
            )
        
        elif args.command == 'backtest':
            cli.run_backtest(
                symbol=args.symbol.upper(),
                start_date=args.start_date,
                end_date=args.end_date,
                initial_capital=args.initial_capital
            )
        
        elif args.command == 'validate':
            cli.run_validation()
        
        print(f"\n{Fore.GREEN}✅ Command completed successfully!{Style.RESET_ALL}")
        
    except KeyboardInterrupt:
        print(f"\n{Fore.YELLOW}⚠️  Command interrupted by user{Style.RESET_ALL}")
    except Exception as e:
        print(f"\n{Fore.RED}❌ Error executing command: {str(e)}{Style.RESET_ALL}")
        logger.error(f"CLI error: {str(e)}", exc_info=True)
        sys.exit(1)

if __name__ == '__main__':
    main()