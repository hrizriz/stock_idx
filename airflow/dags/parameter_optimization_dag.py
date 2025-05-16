# parameter_optimization_dag.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.models import Variable
import pendulum
import logging
import pandas as pd
import json

# Import utility modules
from utils.database import get_database_connection, get_latest_stock_date
from utils.parameter_optimization import optimize_indicators_for_symbol
from utils.telegram import send_telegram_message

# Configure timezone and logging
local_tz = pendulum.timezone("Asia/Jakarta")
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Default arguments for DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': pendulum.datetime(2024, 1, 1, tz=local_tz),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': pendulum.duration(minutes=5)
}

def get_top_symbols_to_optimize(limit=30):
    """Get list of top symbols by trading volume for optimization"""
    try:
        conn = get_database_connection()
        query = """
        SELECT 
            symbol,
            AVG(volume) as avg_volume
        FROM public_analytics.daily_stock_metrics
        WHERE date >= CURRENT_DATE - INTERVAL '30 days'
        GROUP BY symbol
        ORDER BY avg_volume DESC
        LIMIT %s
        """
        
        df = pd.read_sql(query, conn, params=(limit,))
        symbols = df['symbol'].tolist()
        conn.close()
        
        logger.info(f"Selected {len(symbols)} top symbols for parameter optimization")
        return symbols
    except Exception as e:
        logger.error(f"Error getting top symbols: {str(e)}")
        return []

def optimize_symbol_parameters(symbol, **kwargs):
    """Run optimization for a specific symbol"""
    try:
        logger.info(f"Starting parameter optimization for {symbol}")
        result = optimize_indicators_for_symbol(symbol)
        logger.info(f"Completed parameter optimization for {symbol}")
        
        # Save task result to XCom for later reporting
        ti = kwargs['ti']
        ti.xcom_push(key=f'optimization_result_{symbol}', value=result)
        
        return json.dumps(result)
    except Exception as e:
        logger.error(f"Error optimizing parameters for {symbol}: {str(e)}")
        return f"Error: {str(e)}"

def generate_optimization_report(**kwargs):
    """Generate and send optimization report"""
    try:
        ti = kwargs['ti']
        symbols = ti.xcom_pull(task_ids='get_symbols_to_optimize')
        
        # Collect all results
        results = []
        for symbol in symbols:
            result_json = ti.xcom_pull(task_ids=f'optimize_{symbol}', key=f'optimization_result_{symbol}')
            if result_json:
                try:
                    result = result_json if isinstance(result_json, dict) else json.loads(result_json)
                    results.append(result)
                except:
                    logger.warning(f"Could not parse result for {symbol}")
        
        # Generate report
        if results:
            report = "📊 *PARAMETER OPTIMIZATION REPORT* 📊\n\n"
            
            # Summary stats
            total_symbols = len(results)
            symbols_with_rsi = sum(1 for r in results if r.get('rsi_optimized', False))
            symbols_with_macd = sum(1 for r in results if r.get('macd_optimized', False))
            
            report += f"*Optimized Parameters for {total_symbols} Symbols*\n"
            report += f"RSI Optimization: {symbols_with_rsi}/{total_symbols} symbols\n"
            report += f"MACD Optimization: {symbols_with_macd}/{total_symbols} symbols\n\n"
            
            # Top 10 RSI results by score
            report += "*Top 10 RSI Optimization Results:*\n"
            rsi_results = [r for r in results if r.get('best_rsi')]
            rsi_results.sort(key=lambda x: x['best_rsi'].get('score', 0), reverse=True)
            
            for i, result in enumerate(rsi_results[:10], 1):
                best_rsi = result['best_rsi']
                symbol = result['symbol']
                period = int(best_rsi['period'])
                oversold = int(best_rsi['oversold'])
                overbought = int(best_rsi['overbought'])
                win_rate = best_rsi['win_rate_5d']
                avg_return = best_rsi['avg_return_5d']
                
                report += f"{i}. *{symbol}*: Period={period}, Oversold={oversold}, Overbought={overbought}\n"
                report += f"   Win Rate: {win_rate:.1f}%, Avg Return: {avg_return:.2f}%\n"
            
            report += "\n*Top 10 MACD Optimization Results:*\n"
            macd_results = [r for r in results if r.get('best_macd')]
            macd_results.sort(key=lambda x: x['best_macd'].get('score', 0), reverse=True)
            
            for i, result in enumerate(macd_results[:10], 1):
                best_macd = result['best_macd']
                symbol = result['symbol']
                fast = int(best_macd['fast_period'])
                slow = int(best_macd['slow_period'])
                signal = int(best_macd['signal_period'])
                win_rate = best_macd['win_rate_5d']
                avg_return = best_macd['avg_return_5d']
                
                report += f"{i}. *{symbol}*: Fast={fast}, Slow={slow}, Signal={signal}\n"
                report += f"   Win Rate: {win_rate:.1f}%, Avg Return: {avg_return:.2f}%\n"
            
            # Send report
            send_telegram_message(report)
            return f"Optimization report sent for {total_symbols} symbols"
        else:
            logger.warning("No optimization results to report")
            return "No optimization results to report"
    except Exception as e:
        logger.error(f"Error generating optimization report: {str(e)}")
        return f"Error: {str(e)}"

# DAG definition
with DAG(
    dag_id="parameter_optimization",
    start_date=pendulum.datetime(2024, 1, 1, tz=local_tz),
    schedule_interval="0 1 * * 0",  # Run weekly on Sunday at 1 AM
    catchup=False,
    default_args=default_args,
    tags=["optimization", "parameters", "trading"]
) as dag:
    
    # Get symbols to optimize
    get_symbols = PythonOperator(
        task_id="get_symbols_to_optimize",
        python_callable=get_top_symbols_to_optimize,
        op_kwargs={'limit': 30}  # Optimize top 30 symbols
    )
    
    # Create dummy end task
    end_task = DummyOperator(
        task_id="end_task"
    )
    
    # Generate report task
    generate_report = PythonOperator(
        task_id="generate_optimization_report",
        python_callable=generate_optimization_report,
        provide_context=True
    )
    
    # Dynamic tasks for each symbol
    # These will be created at runtime
    get_symbols >> generate_report >> end_task
    
    # The dependent tasks will be created dynamically when the DAG runs
    def create_optimization_tasks():
        symbols = Variable.get("optimization_symbols", default_var=[])
        if not symbols:
            # Use default list if variable not set
            symbols = ["BBCA", "BBRI", "ASII", "TLKM", "BMRI", "UNVR", "HMSP", "ICBP", "PGAS", "ANTM"]
        
        for symbol in symbols:
            task = PythonOperator(
                task_id=f"optimize_{symbol}",
                python_callable=optimize_symbol_parameters,
                op_kwargs={'symbol': symbol},
                dag=dag
            )
            get_symbols >> task >> generate_report
    
    # This function is called when the DAG is parsed
    create_optimization_tasks()