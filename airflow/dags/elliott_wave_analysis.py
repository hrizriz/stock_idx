# Save this as: airflow/dags/elliott_wave_analysis.py

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.sensors.sql_sensor import SqlSensor
from airflow.models import Variable
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup

from datetime import datetime, timedelta
import logging
import json
import pandas as pd
from typing import List, Dict, Any

# Import Elliott Wave utilities
from utils.elliott_wave import (
    analyze_elliott_waves_for_symbol,
    elliott_wave_screening,
    save_elliott_analysis_to_db,
    send_elliott_wave_report,
    ElliottWaveAnalyzer
)
from utils.database import get_database_connection, fetch_data
from utils.telegram import send_telegram_message

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# DAG configuration
default_args = {
    'owner': 'trading_team',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=15),
    'execution_timeout': timedelta(hours=2),
    'sla': timedelta(hours=3)
}

# Elliott Wave Analysis DAG
dag = DAG(
    'elliott_wave_analysis',
    default_args=default_args,
    description='Comprehensive Elliott Wave Analysis and Trading Signal Generation',
    schedule_interval='45 16 * * 1-5',  # Run at 4:45 PM on weekdays (after market close)
    catchup=False,
    max_active_runs=1,
    tags=['elliott_wave', 'technical_analysis', 'trading_signals']
)

# Configuration variables
TOP_STOCKS = [
    'BBCA', 'BBRI', 'BMRI', 'BBNI', 'ASII', 'TLKM', 'UNVR', 'ICBP', 'GGRM', 'INDF',
    'PGAS', 'ITMG', 'ADRO', 'PTBA', 'ANTM', 'INCO', 'TINS', 'JPFA', 'CPIN', 'SIDO',
    'KLBF', 'KAEF', 'DVLA', 'MYOR', 'ULTJ', 'AKRA', 'UNTR', 'AUTO', 'SMSM', 'TAXI'
]

SCREENING_CONFIG = {
    'max_symbols': 50,
    'min_data_days': 100,
    'min_liquidity': 500000,
    'min_volatility_pct': 15
}

def check_market_data_availability(**context):
    """
    Check if latest market data is available before running analysis
    """
    try:
        conn = get_database_connection()
        
        # Check if today's data is available
        query = """
        SELECT COUNT(*) as count 
        FROM public.daily_stock_summary 
        WHERE date = CURRENT_DATE
        """
        
        result = pd.read_sql(query, conn)
        conn.close()
        
        data_count = result.iloc[0]['count']
        
        if data_count < 100:  # Minimum stocks with today's data
            raise ValueError(f"Insufficient market data for today: {data_count} stocks")
        
        logger.info(f"Market data check passed: {data_count} stocks with current data")
        return data_count
        
    except Exception as e:
        logger.error(f"Market data check failed: {str(e)}")
        raise

def run_elliott_wave_screening(**context):
    """
    Run comprehensive Elliott Wave screening on multiple stocks
    """
    try:
        config = SCREENING_CONFIG
        
        logger.info(f"Starting Elliott Wave screening with config: {config}")
        
        # Run screening
        opportunities = elliott_wave_screening(
            max_symbols=config['max_symbols'],
            min_data_days=config['min_data_days']
        )
        
        if not opportunities:
            logger.warning("No Elliott Wave opportunities found in screening")
            return {"opportunities_found": 0, "message": "No opportunities found"}
        
        # Filter and rank opportunities
        high_confidence = [o for o in opportunities if o['signal_confidence'] >= 75]
        buy_signals = [o for o in opportunities if o['signal'] in ['STRONG_BUY', 'BUY']]
        sell_signals = [o for o in opportunities if o['signal'] in ['STRONG_SELL', 'SELL']]
        
        # Save results to XCom for downstream tasks
        screening_summary = {
            'total_opportunities': len(opportunities),
            'high_confidence_count': len(high_confidence),
            'buy_signals_count': len(buy_signals),
            'sell_signals_count': len(sell_signals),
            'screening_date': datetime.now().isoformat(),
            'opportunities': opportunities
        }
        
        # Push to XCom
        context['ti'].xcom_push(key='screening_results', value=screening_summary)
        
        logger.info(f"Elliott Wave screening completed: {len(opportunities)} opportunities found")
        
        return screening_summary
        
    except Exception as e:
        logger.error(f"Error in Elliott Wave screening: {str(e)}")
        raise

def analyze_top_stocks(**context):
    """
    Detailed Elliott Wave analysis for top Indonesian stocks
    """
    try:
        top_stocks_analysis = []
        
        for symbol in TOP_STOCKS[:15]:  # Analyze top 15 stocks
            try:
                logger.info(f"Analyzing Elliott Waves for {symbol}")
                
                analysis = analyze_elliott_waves_for_symbol(symbol, lookback_days=180)
                
                if 'error' not in analysis:
                    # Extract key metrics
                    elliott_pattern = analysis.get('elliott_pattern', {})
                    trading_signals = analysis.get('trading_signals', {})
                    
                    top_stocks_analysis.append({
                        'symbol': symbol,
                        'current_price': analysis['current_price'],
                        'pattern_found': elliott_pattern.get('pattern_found', False),
                        'pattern_confidence': elliott_pattern.get('confidence', 0),
                        'current_wave': elliott_pattern.get('current_wave'),
                        'trading_signal': trading_signals.get('signal', 'HOLD'),
                        'signal_confidence': trading_signals.get('confidence', 0),
                        'risk_reward_ratio': trading_signals.get('risk_reward_ratio'),
                        'analysis_timestamp': datetime.now().isoformat()
                    })
                else:
                    logger.warning(f"Analysis failed for {symbol}: {analysis.get('error', 'Unknown error')}")
                    
            except Exception as e:
                logger.error(f"Error analyzing {symbol}: {str(e)}")
                continue
        
        # Push results to XCom
        context['ti'].xcom_push(key='top_stocks_analysis', value=top_stocks_analysis)
        
        logger.info(f"Top stocks Elliott Wave analysis completed for {len(top_stocks_analysis)} stocks")
        
        return top_stocks_analysis
        
    except Exception as e:
        logger.error(f"Error in top stocks analysis: {str(e)}")
        raise

def save_analysis_results(**context):
    """
    Save all Elliott Wave analysis results to database
    """
    try:
        # Get screening results from XCom
        screening_results = context['ti'].xcom_pull(
            task_ids='elliott_wave_screening',
            key='screening_results'
        )
        
        # Get top stocks analysis from XCom
        top_stocks_results = context['ti'].xcom_pull(
            task_ids='analyze_top_stocks',
            key='top_stocks_analysis'
        )
        
        # Combine all results for database saving
        all_results = []
        
        if screening_results and screening_results.get('opportunities'):
            all_results.extend(screening_results['opportunities'])
        
        if top_stocks_results:
            # Convert top stocks format to match screening format
            for stock in top_stocks_results:
                if stock.get('pattern_found') and stock.get('signal_confidence', 0) >= 50:
                    all_results.append({
                        'symbol': stock['symbol'],
                        'signal': stock.get('trading_signal', 'HOLD'),
                        'signal_confidence': stock.get('signal_confidence', 0),
                        'pattern_confidence': stock.get('pattern_confidence', 0),
                        'current_wave': stock.get('current_wave'),
                        'current_price': stock.get('current_price'),
                        'risk_reward_ratio': stock.get('risk_reward_ratio')
                    })
        
        if all_results:
            # Save to database
            save_elliott_analysis_to_db(all_results)
            
            logger.info(f"Successfully saved {len(all_results)} Elliott Wave analyses to database")
            
            return {
                'saved_count': len(all_results),
                'timestamp': datetime.now().isoformat()
            }
        else:
            logger.warning("No analysis results to save to database")
            return {'saved_count': 0, 'message': 'No results to save'}
            
    except Exception as e:
        logger.error(f"Error saving analysis results: {str(e)}")
        raise

def generate_detailed_report(**context):
    """
    Generate detailed Elliott Wave analysis report
    """
    try:
        # Get analysis results from XCom
        screening_results = context['ti'].xcom_pull(
            task_ids='elliott_wave_screening',
            key='screening_results'
        )
        
        top_stocks_results = context['ti'].xcom_pull(
            task_ids='analyze_top_stocks',
            key='top_stocks_analysis'
        )
        
        # Generate comprehensive report
        report = {
            'report_date': datetime.now().isoformat(),
            'market_summary': {},
            'elliott_opportunities': [],
            'top_stocks_analysis': [],
            'trading_recommendations': [],
            'market_outlook': {}
        }
        
        # Process screening results
        if screening_results:
            opportunities = screening_results.get('opportunities', [])
            
            # Market summary
            report['market_summary'] = {
                'total_stocks_screened': screening_results.get('total_opportunities', 0),
                'high_confidence_signals': screening_results.get('high_confidence_count', 0),
                'buy_signals': screening_results.get('buy_signals_count', 0),
                'sell_signals': screening_results.get('sell_signals_count', 0)
            }
            
            # Top Elliott Wave opportunities
            sorted_opportunities = sorted(
                opportunities, 
                key=lambda x: (x.get('signal_confidence', 0) + x.get('pattern_confidence', 0)) / 2,
                reverse=True
            )
            
            report['elliott_opportunities'] = sorted_opportunities[:10]  # Top 10
            
            # Trading recommendations
            strong_buys = [o for o in opportunities if o.get('signal') == 'STRONG_BUY' and o.get('signal_confidence', 0) >= 75]
            strong_sells = [o for o in opportunities if o.get('signal') in ['STRONG_SELL', 'SELL'] and o.get('signal_confidence', 0) >= 70]
            
            report['trading_recommendations'] = {
                'immediate_buy_opportunities': strong_buys[:5],
                'profit_taking_candidates': strong_sells[:3],
                'watch_list': [o for o in opportunities if o.get('signal_confidence', 0) >= 60 and o.get('signal') in ['BUY', 'SELL']][:5]
            }
        
        # Process top stocks analysis
        if top_stocks_results:
            # Filter for significant patterns
            significant_patterns = [
                stock for stock in top_stocks_results 
                if stock.get('pattern_found') and stock.get('pattern_confidence', 0) >= 60
            ]
            
            report['top_stocks_analysis'] = significant_patterns
            
            # Market outlook based on top stocks
            wave_distribution = {}
            for stock in significant_patterns:
                wave = stock.get('current_wave', 'Unknown')
                wave_distribution[wave] = wave_distribution.get(wave, 0) + 1
            
            # Determine market phase
            if wave_distribution.get('3', 0) > wave_distribution.get('5', 0):
                market_phase = 'Mid-impulse (Bullish acceleration expected)'
            elif wave_distribution.get('5', 0) > 3:
                market_phase = 'Late-impulse (Correction expected)'
            elif wave_distribution.get('C', 0) > 2:
                market_phase = 'Correction completion (New cycle expected)'
            else:
                market_phase = 'Mixed signals (Selective opportunities)'
            
            report['market_outlook'] = {
                'dominant_phase': market_phase,
                'wave_distribution': wave_distribution,
                'bullish_stocks': len([s for s in significant_patterns if s.get('trading_signal') in ['STRONG_BUY', 'BUY']]),
                'bearish_stocks': len([s for s in significant_patterns if s.get('trading_signal') in ['STRONG_SELL', 'SELL']])
            }
        
        # Save report to XCom for telegram task
        context['ti'].xcom_push(key='detailed_report', value=report)
        
        logger.info("Detailed Elliott Wave report generated successfully")
        
        return report
        
    except Exception as e:
        logger.error(f"Error generating detailed report: {str(e)}")
        raise

def send_telegram_report(**context):
    """
    Send Elliott Wave analysis report via Telegram
    """
    try:
        # Get detailed report from XCom
        detailed_report = context['ti'].xcom_pull(
            task_ids='generate_detailed_report',
            key='detailed_report'
        )
        
        if not detailed_report:
            # Fallback to simple report
            result = send_elliott_wave_report()
            return result
        
        # Format comprehensive Telegram message
        message = "🌊 *ELLIOTT WAVE DAILY ANALYSIS* 🌊\n\n"
        
        # Market summary
        market_summary = detailed_report.get('market_summary', {})
        message += "📊 *MARKET SUMMARY:*\n"
        message += f"• Stocks Analyzed: {market_summary.get('total_stocks_screened', 0)}\n"
        message += f"• High Confidence Signals: {market_summary.get('high_confidence_signals', 0)}\n"
        message += f"• Buy Signals: {market_summary.get('buy_signals', 0)}\n"
        message += f"• Sell Signals: {market_summary.get('sell_signals', 0)}\n\n"
        
        # Market outlook
        market_outlook = detailed_report.get('market_outlook', {})
        if market_outlook:
            message += "🔮 *MARKET OUTLOOK:*\n"
            message += f"• Phase: {market_outlook.get('dominant_phase', 'Mixed')}\n"
            message += f"• Bullish Stocks: {market_outlook.get('bullish_stocks', 0)}\n"
            message += f"• Bearish Stocks: {market_outlook.get('bearish_stocks', 0)}\n\n"
        
        # Top opportunities
        opportunities = detailed_report.get('elliott_opportunities', [])
        if opportunities:
            message += "🚀 *TOP ELLIOTT WAVE OPPORTUNITIES:*\n\n"
            
            for i, opp in enumerate(opportunities[:5], 1):
                signal_emoji = "🟢" if opp.get('signal') in ['STRONG_BUY', 'BUY'] else "🔴" if opp.get('signal') in ['STRONG_SELL', 'SELL'] else "🟡"
                
                message += f"{signal_emoji} *{i}. {opp['symbol']}* - Wave {opp.get('current_wave', 'Unknown')}\n"
                message += f"   💰 Price: Rp{opp['current_price']:,.0f}\n"
                message += f"   📊 Signal: {opp.get('signal', 'HOLD')} ({opp.get('signal_confidence', 0)}%)\n"
                message += f"   🎯 Pattern Confidence: {opp.get('pattern_confidence', 0)}%\n"
                
                if opp.get('risk_reward_ratio'):
                    message += f"   ⚖️ Risk/Reward: 1:{opp['risk_reward_ratio']:.1f}\n"
                
                message += f"   📈 Next Expected: {opp.get('next_expected', 'Unknown')}\n\n"
        
        # Trading recommendations
        recommendations = detailed_report.get('trading_recommendations', {})
        immediate_buys = recommendations.get('immediate_buy_opportunities', [])
        
        if immediate_buys:
            message += "⚡ *IMMEDIATE ACTION REQUIRED:*\n\n"
            for buy in immediate_buys[:3]:
                message += f"🟢 *{buy['symbol']}* - Wave {buy.get('current_wave', 'Unknown')}\n"
                message += f"   Signal: {buy.get('signal', 'BUY')} ({buy.get('signal_confidence', 0)}%)\n"
                message += f"   Current: Rp{buy['current_price']:,.0f}\n"
                if buy.get('entry_price'):
                    message += f"   Entry: Rp{buy['entry_price']:,.0f}\n"
                if buy.get('take_profit_1'):
                    message += f"   Target: Rp{buy['take_profit_1']:,.0f}\n"
                message += "\n"
        
        # Educational content
        message += "🎓 *ELLIOTT WAVE EDUCATION:*\n\n"
        message += "*Current Market Phases:*\n"
        message += "• Wave 2 endings: Best buying opportunities\n"
        message += "• Wave 3: Strongest trends, ride the momentum\n"
        message += "• Wave 4: Prepare for final push (Wave 5)\n"
        message += "• Wave 5: Take profits, expect correction\n"
        message += "• Wave C endings: New bull market begins\n\n"
        
        message += "*Risk Management:*\n"
        message += "• Always use stop losses below key Fibonacci levels\n"
        message += "• Position size according to confidence level\n"
        message += "• Take partial profits at Fibonacci extensions\n"
        message += "• Watch for momentum divergences in Wave 5\n\n"
        
        message += f"📅 Analysis Date: {datetime.now().strftime('%Y-%m-%d %H:%M')}\n"
        message += "⚠️ *Disclaimer: For educational purposes. Always do your own research and manage risk appropriately.*"
        
        # Send the message
        result = send_telegram_message(message)
        
        if "successfully" in result:
            logger.info("Comprehensive Elliott Wave report sent via Telegram")
            return "Telegram report sent successfully"
        else:
            logger.error(f"Failed to send Telegram report: {result}")
            return f"Failed to send Telegram report: {result}"
            
    except Exception as e:
        logger.error(f"Error sending Telegram report: {str(e)}")
        # Try fallback simple report
        try:
            return send_elliott_wave_report()
        except:
            raise

def cleanup_old_analysis(**context):
    """
    Clean up old Elliott Wave analysis data to maintain database performance
    """
    try:
        conn = get_database_connection()
        cursor = conn.cursor()
        
        # Delete analysis older than 30 days
        cursor.execute("""
        DELETE FROM public_analytics.elliott_wave_analysis 
        WHERE analysis_date < CURRENT_DATE - INTERVAL '30 days'
        """)
        
        deleted_count = cursor.rowcount
        
        conn.commit()
        cursor.close()
        conn.close()
        
        logger.info(f"Cleaned up {deleted_count} old Elliott Wave analysis records")
        
        return deleted_count
        
    except Exception as e:
        logger.error(f"Error cleaning up old analysis: {str(e)}")
        raise

# Task definitions with Task Groups for better organization

# Data validation group
with TaskGroup("data_validation", dag=dag) as data_validation_group:
    
    # Check if market data is available
    check_data_task = PythonOperator(
        task_id='check_market_data',
        python_callable=check_market_data_availability,
        dag=dag
    )
    
    # SQL sensor to ensure data freshness
    data_freshness_sensor = SqlSensor(
        task_id='ensure_data_freshness',
        conn_id='postgres_default',
        sql="""
        SELECT COUNT(*) FROM public.daily_stock_summary 
        WHERE date >= CURRENT_DATE - INTERVAL '2 days'
        """,
        dag=dag,
        timeout=300,
        poke_interval=60
    )

# Main analysis group
with TaskGroup("elliott_analysis", dag=dag) as elliott_analysis_group:
    
    # Elliott Wave screening task
    screening_task = PythonOperator(
        task_id='elliott_wave_screening',
        python_callable=run_elliott_wave_screening,
        dag=dag,
        pool='analysis_pool'
    )
    
    # Top stocks detailed analysis
    top_stocks_task = PythonOperator(
        task_id='analyze_top_stocks',
        python_callable=analyze_top_stocks,
        dag=dag,
        pool='analysis_pool'
    )

# Results processing group
with TaskGroup("results_processing", dag=dag) as results_processing_group:
    
    # Save analysis results to database
    save_results_task = PythonOperator(
        task_id='save_analysis_results',
        python_callable=save_analysis_results,
        dag=dag
    )
    
    # Generate detailed report
    generate_report_task = PythonOperator(
        task_id='generate_detailed_report',
        python_callable=generate_detailed_report,
        dag=dag
    )

# Communication and cleanup group
with TaskGroup("communication_cleanup", dag=dag) as communication_cleanup_group:
    
    # Send Telegram report
    telegram_task = PythonOperator(
        task_id='send_telegram_report',
        python_callable=send_telegram_report,
        dag=dag
    )
    
    # Cleanup old data
    cleanup_task = PythonOperator(
        task_id='cleanup_old_analysis',
        python_callable=cleanup_old_analysis,
        dag=dag
    )

# Health check task
health_check = BashOperator(
    task_id='health_check',
    bash_command='echo "Elliott Wave Analysis DAG completed successfully at $(date)"',
    dag=dag
)

# Define task dependencies
data_validation_group >> elliott_analysis_group >> results_processing_group >> communication_cleanup_group >> health_check

# Individual task dependencies within groups
check_data_task >> data_freshness_sensor
[screening_task, top_stocks_task]  # Parallel execution
save_results_task >> generate_report_task
[telegram_task, cleanup_task]  # Parallel execution