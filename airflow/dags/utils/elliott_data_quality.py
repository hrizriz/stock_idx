# Save this as: airflow/dags/utils/elliott_data_quality.py

"""
Elliott Wave Data Quality Checker
Memastikan kualitas data sebelum menjalankan Elliott Wave analysis
"""

import pandas as pd
import numpy as np
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Any, Optional
import warnings
from dataclasses import dataclass
from enum import Enum

from .database import get_database_connection
from .elliott_config import ElliottWaveConfig

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DataQualityLevel(Enum):
    """Data quality levels"""
    EXCELLENT = "EXCELLENT"    # 95-100% quality
    GOOD = "GOOD"             # 85-94% quality
    FAIR = "FAIR"             # 70-84% quality
    POOR = "POOR"             # 50-69% quality
    UNUSABLE = "UNUSABLE"     # <50% quality

class DataIssueType(Enum):
    """Types of data quality issues"""
    MISSING_DATA = "MISSING_DATA"
    DUPLICATE_DATA = "DUPLICATE_DATA"
    OUTLIER_DATA = "OUTLIER_DATA"
    INCONSISTENT_DATA = "INCONSISTENT_DATA"
    INSUFFICIENT_DATA = "INSUFFICIENT_DATA"
    STALE_DATA = "STALE_DATA"
    VOLUME_ANOMALY = "VOLUME_ANOMALY"
    PRICE_ANOMALY = "PRICE_ANOMALY"

@dataclass
class DataQualityIssue:
    """Data quality issue structure"""
    issue_type: DataIssueType
    severity: str  # LOW, MEDIUM, HIGH, CRITICAL
    description: str
    affected_dates: List[str]
    impact_score: float  # 0-100
    recommendation: str

@dataclass
class DataQualityReport:
    """Comprehensive data quality report"""
    symbol: str
    check_date: datetime
    overall_quality_score: float
    quality_level: DataQualityLevel
    data_completeness_pct: float
    data_consistency_pct: float
    data_freshness_days: int
    total_records: int
    date_range: Tuple[str, str]
    issues: List[DataQualityIssue]
    recommendations: List[str]
    elliott_wave_readiness: bool

class ElliottDataQualityChecker:
    """
    Comprehensive data quality checker untuk Elliott Wave analysis
    """
    
    def __init__(self):
        self.config = ElliottWaveConfig()
        self.min_data_days = 60  # Minimum days for Elliott analysis
        self.max_missing_pct = 5  # Maximum missing data percentage
        self.outlier_threshold = 3  # Standard deviations for outlier detection
        
    def check_symbol_data_quality(self, symbol: str, days_back: int = 200) -> DataQualityReport:
        """
        Comprehensive data quality check untuk specific symbol
        
        Parameters:
        symbol (str): Stock symbol to check
        days_back (int): Number of days to check
        
        Returns:
        DataQualityReport: Comprehensive quality report
        """
        try:
            logger.info(f"Starting data quality check for {symbol}")
            
            # Get data from database
            data = self._fetch_symbol_data(symbol, days_back)
            
            if data.empty:
                return DataQualityReport(
                    symbol=symbol,
                    check_date=datetime.now(),
                    overall_quality_score=0,
                    quality_level=DataQualityLevel.UNUSABLE,
                    data_completeness_pct=0,
                    data_consistency_pct=0,
                    data_freshness_days=999,
                    total_records=0,
                    date_range=("N/A", "N/A"),
                    issues=[DataQualityIssue(
                        issue_type=DataIssueType.INSUFFICIENT_DATA,
                        severity="CRITICAL",
                        description="No data available for symbol",
                        affected_dates=[],
                        impact_score=100,
                        recommendation="Check data source and symbol availability"
                    )],
                    recommendations=["Verify symbol exists and data is available"],
                    elliott_wave_readiness=False
                )
            
            # Initialize quality checks
            quality_issues = []
            quality_scores = []
            
            # Check 1: Data Completeness
            completeness_score, completeness_issues = self._check_data_completeness(data, symbol)
            quality_scores.append(completeness_score)
            quality_issues.extend(completeness_issues)
            
            # Check 2: Data Consistency
            consistency_score, consistency_issues = self._check_data_consistency(data, symbol)
            quality_scores.append(consistency_score)
            quality_issues.extend(consistency_issues)
            
            # Check 3: Data Freshness
            freshness_score, freshness_days, freshness_issues = self._check_data_freshness(data, symbol)
            quality_scores.append(freshness_score)
            quality_issues.extend(freshness_issues)
            
            # Check 4: Price Data Quality
            price_score, price_issues = self._check_price_data_quality(data, symbol)
            quality_scores.append(price_score)
            quality_issues.extend(price_issues)
            
            # Check 5: Volume Data Quality
            volume_score, volume_issues = self._check_volume_data_quality(data, symbol)
            quality_scores.append(volume_score)
            quality_issues.extend(volume_issues)
            
            # Check 6: Outlier Detection
            outlier_score, outlier_issues = self._check_for_outliers(data, symbol)
            quality_scores.append(outlier_score)
            quality_issues.extend(outlier_issues)
            
            # Check 7: Duplicate Detection
            duplicate_score, duplicate_issues = self._check_for_duplicates(data, symbol)
            quality_scores.append(duplicate_score)
            quality_issues.extend(duplicate_issues)
            
            # Calculate overall quality score
            overall_quality_score = np.mean(quality_scores)
            
            # Determine quality level
            quality_level = self._determine_quality_level(overall_quality_score)
            
            # Calculate specific metrics
            data_completeness_pct = completeness_score
            data_consistency_pct = consistency_score
            
            # Date range
            date_range = (
                data['date'].min().strftime('%Y-%m-%d'),
                data['date'].max().strftime('%Y-%m-%d')
            )
            
            # Elliott Wave readiness assessment
            elliott_readiness = self._assess_elliott_wave_readiness(
                data, overall_quality_score, quality_issues
            )
            
            # Generate recommendations
            recommendations = self._generate_recommendations(quality_issues, elliott_readiness)
            
            # Create comprehensive report
            report = DataQualityReport(
                symbol=symbol,
                check_date=datetime.now(),
                overall_quality_score=overall_quality_score,
                quality_level=quality_level,
                data_completeness_pct=data_completeness_pct,
                data_consistency_pct=data_consistency_pct,
                data_freshness_days=freshness_days,
                total_records=len(data),
                date_range=date_range,
                issues=quality_issues,
                recommendations=recommendations,
                elliott_wave_readiness=elliott_readiness
            )
            
            logger.info(f"Data quality check completed for {symbol}: {quality_level.value} ({overall_quality_score:.1f}%)")
            
            return report
            
        except Exception as e:
            logger.error(f"Error checking data quality for {symbol}: {str(e)}")
            return DataQualityReport(
                symbol=symbol,
                check_date=datetime.now(),
                overall_quality_score=0,
                quality_level=DataQualityLevel.UNUSABLE,
                data_completeness_pct=0,
                data_consistency_pct=0,
                data_freshness_days=999,
                total_records=0,
                date_range=("N/A", "N/A"),
                issues=[DataQualityIssue(
                    issue_type=DataIssueType.INCONSISTENT_DATA,
                    severity="CRITICAL",
                    description=f"Error during quality check: {str(e)}",
                    affected_dates=[],
                    impact_score=100,
                    recommendation="Check data source and database connectivity"
                )],
                recommendations=["Fix data source issues"],
                elliott_wave_readiness=False
            )
    
    def _fetch_symbol_data(self, symbol: str, days_back: int) -> pd.DataFrame:
        """Fetch data for quality checking"""
        try:
            conn = get_database_connection()
            
            query = f"""
            SELECT 
                date,
                open_price as open,
                high,
                low,
                close,
                volume,
                market_cap,
                turnover
            FROM public.daily_stock_summary
            WHERE symbol = '{symbol}'
            AND date >= CURRENT_DATE - INTERVAL '{days_back} days'
            ORDER BY date
            """
            
            df = pd.read_sql(query, conn)
            conn.close()
            
            # Convert date column
            df['date'] = pd.to_datetime(df['date'])
            
            return df
            
        except Exception as e:
            logger.error(f"Error fetching data for {symbol}: {str(e)}")
            return pd.DataFrame()
    
    def _check_data_completeness(self, data: pd.DataFrame, symbol: str) -> Tuple[float, List[DataQualityIssue]]:
        """Check data completeness"""
        issues = []
        
        try:
            if data.empty:
                return 0, [DataQualityIssue(
                    issue_type=DataIssueType.MISSING_DATA,
                    severity="CRITICAL",
                    description="No data available",
                    affected_dates=[],
                    impact_score=100,
                    recommendation="Check data source"
                )]
            
            # Check for missing essential columns
            essential_columns = ['open', 'high', 'low', 'close', 'volume']
            missing_columns = [col for col in essential_columns if col not in data.columns]
            
            if missing_columns:
                issues.append(DataQualityIssue(
                    issue_type=DataIssueType.MISSING_DATA,
                    severity="CRITICAL",
                    description=f"Missing essential columns: {missing_columns}",
                    affected_dates=[],
                    impact_score=80,
                    recommendation="Ensure all OHLCV data is available"
                ))
                return 20, issues
            
            # Check for missing values in essential columns
            total_rows = len(data)
            missing_counts = {}
            
            for col in essential_columns:
                missing_count = data[col].isna().sum()
                if missing_count > 0:
                    missing_pct = (missing_count / total_rows) * 100
                    missing_counts[col] = missing_pct
                    
                    severity = "CRITICAL" if missing_pct > 10 else "HIGH" if missing_pct > 5 else "MEDIUM"
                    
                    affected_dates = data[data[col].isna()]['date'].dt.strftime('%Y-%m-%d').tolist()
                    
                    issues.append(DataQualityIssue(
                        issue_type=DataIssueType.MISSING_DATA,
                        severity=severity,
                        description=f"Missing {col} data: {missing_count} records ({missing_pct:.1f}%)",
                        affected_dates=affected_dates[:10],  # Limit to first 10 dates
                        impact_score=missing_pct,
                        recommendation=f"Fill missing {col} data or interpolate values"
                    ))
            
            # Check for gaps in date sequence
            if len(data) > 1:
                data_sorted = data.sort_values('date')
                date_gaps = self._find_date_gaps(data_sorted['date'])
                
                if date_gaps:
                    gap_dates = [f"{gap[0]} to {gap[1]}" for gap in date_gaps[:5]]
                    issues.append(DataQualityIssue(
                        issue_type=DataIssueType.MISSING_DATA,
                        severity="MEDIUM",
                        description=f"Date gaps found: {len(date_gaps)} gaps",
                        affected_dates=gap_dates,
                        impact_score=len(date_gaps) * 2,
                        recommendation="Fill date gaps with appropriate data"
                    ))
            
            # Calculate completeness score
            if missing_counts:
                avg_missing_pct = np.mean(list(missing_counts.values()))
                completeness_score = max(0, 100 - avg_missing_pct)
            else:
                completeness_score = 100
            
            # Adjust for date gaps
            if len(data) < self.min_data_days:
                completeness_score *= 0.5
                issues.append(DataQualityIssue(
                    issue_type=DataIssueType.INSUFFICIENT_DATA,
                    severity="HIGH",
                    description=f"Insufficient data: {len(data)} days (minimum: {self.min_data_days})",
                    affected_dates=[],
                    impact_score=50,
                    recommendation=f"Collect at least {self.min_data_days} days of data"
                ))
            
            return completeness_score, issues
            
        except Exception as e:
            logger.error(f"Error checking data completeness: {str(e)}")
            return 0, [DataQualityIssue(
                issue_type=DataIssueType.INCONSISTENT_DATA,
                severity="CRITICAL",
                description=f"Error in completeness check: {str(e)}",
                affected_dates=[],
                impact_score=100,
                recommendation="Fix data processing issues"
            )]
    
    def _check_data_consistency(self, data: pd.DataFrame, symbol: str) -> Tuple[float, List[DataQualityIssue]]:
        """Check data consistency"""
        issues = []
        consistency_checks = []
        
        try:
            if data.empty:
                return 0, issues
            
            # Check OHLC relationships
            ohlc_consistent = (
                (data['high'] >= data['open']) &
                (data['high'] >= data['close']) &
                (data['low'] <= data['open']) &
                (data['low'] <= data['close']) &
                (data['high'] >= data['low'])
            )
            
            ohlc_violations = (~ohlc_consistent).sum()
            ohlc_consistency_pct = (len(data) - ohlc_violations) / len(data) * 100
            consistency_checks.append(ohlc_consistency_pct)
            
            if ohlc_violations > 0:
                violation_dates = data[~ohlc_consistent]['date'].dt.strftime('%Y-%m-%d').tolist()
                issues.append(DataQualityIssue(
                    issue_type=DataIssueType.INCONSISTENT_DATA,
                    severity="HIGH",
                    description=f"OHLC relationship violations: {ohlc_violations} records",
                    affected_dates=violation_dates[:10],
                    impact_score=ohlc_violations / len(data) * 100,
                    recommendation="Fix OHLC data inconsistencies"
                ))
            
            # Check for negative values
            numeric_columns = ['open', 'high', 'low', 'close', 'volume']
            for col in numeric_columns:
                if col in data.columns:
                    negative_values = (data[col] < 0).sum()
                    if negative_values > 0:
                        negative_dates = data[data[col] < 0]['date'].dt.strftime('%Y-%m-%d').tolist()
                        issues.append(DataQualityIssue(
                            issue_type=DataIssueType.INCONSISTENT_DATA,
                            severity="HIGH",
                            description=f"Negative values in {col}: {negative_values} records",
                            affected_dates=negative_dates[:10],
                            impact_score=negative_values / len(data) * 100,
                            recommendation=f"Fix negative values in {col}"
                        ))
                        consistency_checks.append(max(0, 100 - (negative_values / len(data) * 100)))
                    else:
                        consistency_checks.append(100)
            
            # Check for zero volume
            if 'volume' in data.columns:
                zero_volume = (data['volume'] == 0).sum()
                if zero_volume > 0:
                    zero_vol_dates = data[data['volume'] == 0]['date'].dt.strftime('%Y-%m-%d').tolist()
                    issues.append(DataQualityIssue(
                        issue_type=DataIssueType.VOLUME_ANOMALY,
                        severity="MEDIUM",
                        description=f"Zero volume days: {zero_volume} records",
                        affected_dates=zero_vol_dates[:10],
                        impact_score=zero_volume / len(data) * 50,  # Lower impact for volume
                        recommendation="Investigate zero volume days"
                    ))
            
            # Check for price continuity (extreme gaps)
            if len(data) > 1:
                data_sorted = data.sort_values('date')
                price_changes = data_sorted['close'].pct_change().abs()
                extreme_changes = price_changes > 0.5  # More than 50% daily change
                
                extreme_count = extreme_changes.sum()
                if extreme_count > 0:
                    extreme_dates = data_sorted[extreme_changes]['date'].dt.strftime('%Y-%m-%d').tolist()
                    issues.append(DataQualityIssue(
                        issue_type=DataIssueType.PRICE_ANOMALY,
                        severity="MEDIUM",
                        description=f"Extreme price changes: {extreme_count} occurrences",
                        affected_dates=extreme_dates[:10],
                        impact_score=extreme_count / len(data) * 30,
                        recommendation="Verify extreme price movements"
                    ))
                    consistency_checks.append(max(0, 100 - (extreme_count / len(data) * 30)))
                else:
                    consistency_checks.append(100)
            
            # Calculate overall consistency score
            consistency_score = np.mean(consistency_checks) if consistency_checks else 100
            
            return consistency_score, issues
            
        except Exception as e:
            logger.error(f"Error checking data consistency: {str(e)}")
            return 0, [DataQualityIssue(
                issue_type=DataIssueType.INCONSISTENT_DATA,
                severity="CRITICAL",
                description=f"Error in consistency check: {str(e)}",
                affected_dates=[],
                impact_score=100,
                recommendation="Fix data processing issues"
            )]
    
    def _check_data_freshness(self, data: pd.DataFrame, symbol: str) -> Tuple[float, int, List[DataQualityIssue]]:
        """Check data freshness"""
        issues = []
        
        try:
            if data.empty:
                return 0, 999, issues
            
            # Get latest data date
            latest_date = data['date'].max()
            days_since_latest = (datetime.now().date() - latest_date.date()).days
            
            # Determine freshness score based on days
            if days_since_latest <= 1:
                freshness_score = 100
            elif days_since_latest <= 3:
                freshness_score = 85
            elif days_since_latest <= 7:
                freshness_score = 70
            elif days_since_latest <= 14:
                freshness_score = 50
            else:
                freshness_score = 25
            
            # Generate issues based on staleness
            if days_since_latest > 1:
                severity = "LOW" if days_since_latest <= 3 else "MEDIUM" if days_since_latest <= 7 else "HIGH"
                
                issues.append(DataQualityIssue(
                    issue_type=DataIssueType.STALE_DATA,
                    severity=severity,
                    description=f"Data is {days_since_latest} days old (latest: {latest_date.strftime('%Y-%m-%d')})",
                    affected_dates=[latest_date.strftime('%Y-%m-%d')],
                    impact_score=min(days_since_latest * 5, 100),
                    recommendation="Update data source to get recent data"
                ))
            
            return freshness_score, days_since_latest, issues
            
        except Exception as e:
            logger.error(f"Error checking data freshness: {str(e)}")
            return 0, 999, [DataQualityIssue(
                issue_type=DataIssueType.STALE_DATA,
                severity="CRITICAL",
                description=f"Error checking freshness: {str(e)}",
                affected_dates=[],
                impact_score=100,
                recommendation="Fix data processing issues"
            )]
    
    def _check_price_data_quality(self, data: pd.DataFrame, symbol: str) -> Tuple[float, List[DataQualityIssue]]:
        """Check price data quality"""
        issues = []
        quality_checks = []
        
        try:
            if data.empty or 'close' not in data.columns:
                return 0, issues
            
            # Check for constant prices (potential data feed issue)
            if len(data) > 1:
                price_changes = data['close'].diff().abs()
                constant_price_days = (price_changes == 0).sum()
                constant_price_pct = constant_price_days / len(data) * 100
                
                if constant_price_pct > 20:  # More than 20% of days with no price change
                    issues.append(DataQualityIssue(
                        issue_type=DataIssueType.PRICE_ANOMALY,
                        severity="HIGH",
                        description=f"Constant prices detected: {constant_price_pct:.1f}% of days",
                        affected_dates=[],
                        impact_score=constant_price_pct,
                        recommendation="Check for data feed issues"
                    ))
                
                quality_checks.append(max(0, 100 - constant_price_pct))
            
            # Check for reasonable price ranges
            price_columns = ['open', 'high', 'low', 'close']
            for col in price_columns:
                if col in data.columns:
                    # Check for extremely low prices (potential data error)
                    low_prices = (data[col] < 1).sum()  # Prices below 1 IDR
                    if low_prices > 0:
                        issues.append(DataQualityIssue(
                            issue_type=DataIssueType.PRICE_ANOMALY,
                            severity="HIGH",
                            description=f"Extremely low prices in {col}: {low_prices} records",
                            affected_dates=[],
                            impact_score=low_prices / len(data) * 100,
                            recommendation=f"Verify {col} price data accuracy"
                        ))
                    
                    # Check for extremely high prices (potential data error)
                    price_median = data[col].median()
                    extremely_high = (data[col] > price_median * 100).sum()
                    if extremely_high > 0:
                        issues.append(DataQualityIssue(
                            issue_type=DataIssueType.PRICE_ANOMALY,
                            severity="MEDIUM",
                            description=f"Extremely high prices in {col}: {extremely_high} records",
                            affected_dates=[],
                            impact_score=extremely_high / len(data) * 50,
                            recommendation=f"Verify {col} price data for potential splits/errors"
                        ))
            
            # Calculate price quality score
            if quality_checks:
                price_quality_score = np.mean(quality_checks)
            else:
                price_quality_score = 100
            
            return price_quality_score, issues
            
        except Exception as e:
            logger.error(f"Error checking price data quality: {str(e)}")
            return 0, [DataQualityIssue(
                issue_type=DataIssueType.PRICE_ANOMALY,
                severity="CRITICAL",
                description=f"Error in price quality check: {str(e)}",
                affected_dates=[],
                impact_score=100,
                recommendation="Fix price data processing"
            )]
    
    def _check_volume_data_quality(self, data: pd.DataFrame, symbol: str) -> Tuple[float, List[DataQualityIssue]]:
        """Check volume data quality"""
        issues = []
        
        try:
            if data.empty or 'volume' not in data.columns:
                return 50, issues  # Partial score if volume data missing
            
            # Check for missing volume data
            missing_volume = data['volume'].isna().sum()
            if missing_volume > 0:
                issues.append(DataQualityIssue(
                    issue_type=DataIssueType.MISSING_DATA,
                    severity="MEDIUM",
                    description=f"Missing volume data: {missing_volume} records",
                    affected_dates=[],
                    impact_score=missing_volume / len(data) * 50,
                    recommendation="Obtain volume data for complete analysis"
                ))
            
            # Check for zero volume (which might be legitimate but worth noting)
            zero_volume = (data['volume'] == 0).sum()
            if zero_volume > len(data) * 0.1:  # More than 10% zero volume days
                issues.append(DataQualityIssue(
                    issue_type=DataIssueType.VOLUME_ANOMALY,
                    severity="MEDIUM",
                    description=f"High frequency of zero volume: {zero_volume} days ({zero_volume/len(data)*100:.1f}%)",
                    affected_dates=[],
                    impact_score=30,
                    recommendation="Verify volume data accuracy"
                ))
            
            # Check for volume outliers
            if len(data) > 10:
                volume_clean = data['volume'].replace(0, np.nan).dropna()
                if len(volume_clean) > 5:
                    volume_median = volume_clean.median()
                    volume_mad = (volume_clean - volume_median).abs().median()
                    
                    # Outliers are more than 10x the median
                    volume_outliers = (data['volume'] > volume_median * 10).sum()
                    if volume_outliers > 0:
                        issues.append(DataQualityIssue(
                            issue_type=DataIssueType.VOLUME_ANOMALY,
                            severity="LOW",
                            description=f"Volume outliers detected: {volume_outliers} days",
                            affected_dates=[],
                            impact_score=volume_outliers / len(data) * 20,
                            recommendation="Investigate volume spikes"
                        ))
            
            # Calculate volume quality score
            volume_issues_impact = sum([issue.impact_score for issue in issues if 'volume' in issue.description.lower()])
            volume_quality_score = max(0, 100 - volume_issues_impact)
            
            return volume_quality_score, issues
            
        except Exception as e:
            logger.error(f"Error checking volume data quality: {str(e)}")
            return 50, [DataQualityIssue(
                issue_type=DataIssueType.VOLUME_ANOMALY,
                severity="MEDIUM",
                description=f"Error in volume quality check: {str(e)}",
                affected_dates=[],
                impact_score=50,
                recommendation="Fix volume data processing"
            )]
    
    def _check_for_outliers(self, data: pd.DataFrame, symbol: str) -> Tuple[float, List[DataQualityIssue]]:
        """Check for statistical outliers"""
        issues = []
        
        try:
            if data.empty or len(data) < 10:
                return 100, issues
            
            outlier_count = 0
            total_checks = 0
            
            # Check price columns for outliers
            price_columns = ['open', 'high', 'low', 'close']
            for col in price_columns:
                if col in data.columns:
                    # Calculate z-scores
                    z_scores = np.abs((data[col] - data[col].mean()) / data[col].std())
                    outliers = z_scores > self.outlier_threshold
                    col_outliers = outliers.sum()
                    
                    outlier_count += col_outliers
                    total_checks += len(data)
                    
                    if col_outliers > 0:
                        outlier_dates = data[outliers]['date'].dt.strftime('%Y-%m-%d').tolist()
                        issues.append(DataQualityIssue(
                            issue_type=DataIssueType.OUTLIER_DATA,
                            severity="LOW",
                            description=f"Statistical outliers in {col}: {col_outliers} records",
                            affected_dates=outlier_dates[:5],
                            impact_score=col_outliers / len(data) * 20,
                            recommendation=f"Review {col} outlier values for accuracy"
                        ))
            
            # Calculate outlier score
            if total_checks > 0:
                outlier_pct = (outlier_count / total_checks) * 100
                outlier_score = max(0, 100 - outlier_pct * 5)  # Reduce score by 5x outlier percentage
            else:
                outlier_score = 100
            
            return outlier_score, issues
            
        except Exception as e:
            logger.error(f"Error checking for outliers: {str(e)}")
            return 100, []
    
    def _check_for_duplicates(self, data: pd.DataFrame, symbol: str) -> Tuple[float, List[DataQualityIssue]]:
        """Check for duplicate records"""
        issues = []
        
        try:
            if data.empty:
                return 100, issues
            
            # Check for duplicate dates
            duplicate_dates = data['date'].duplicated().sum()
            
            if duplicate_dates > 0:
                dup_date_list = data[data['date'].duplicated(keep=False)]['date'].dt.strftime('%Y-%m-%d').unique().tolist()
                issues.append(DataQualityIssue(
                    issue_type=DataIssueType.DUPLICATE_DATA,
                    severity="HIGH",
                    description=f"Duplicate dates found: {duplicate_dates} records",
                    affected_dates=dup_date_list[:10],
                    impact_score=duplicate_dates / len(data) * 100,
                    recommendation="Remove or consolidate duplicate date records"
                ))
            
            # Check for completely identical rows
            duplicate_rows = data.duplicated().sum()
            
            if duplicate_rows > 0:
                issues.append(DataQualityIssue(
                    issue_type=DataIssueType.DUPLICATE_DATA,
                    severity="MEDIUM",
                    description=f"Completely duplicate rows: {duplicate_rows} records",
                    affected_dates=[],
                    impact_score=duplicate_rows / len(data) * 80,
                    recommendation="Remove duplicate rows"
                ))
            
            # Calculate duplicate score
            total_duplicates = duplicate_dates + duplicate_rows
            duplicate_score = max(0, 100 - (total_duplicates / len(data) * 100))
            
            return duplicate_score, issues
            
        except Exception as e:
            logger.error(f"Error checking for duplicates: {str(e)}")
            return 100, []
    
    def _find_date_gaps(self, dates: pd.Series) -> List[Tuple[str, str]]:
        """Find gaps in date sequence"""
        gaps = []
        
        try:
            if len(dates) < 2:
                return gaps
            
            dates_sorted = dates.sort_values()
            
            for i in range(len(dates_sorted) - 1):
                current_date = dates_sorted.iloc[i]
                next_date = dates_sorted.iloc[i + 1]
                
                # Check if gap is more than 3 business days (accounting for weekends)
                date_diff = (next_date - current_date).days
                
                if date_diff > 5:  # More than 5 days gap (including weekends)
                    gaps.append((
                        current_date.strftime('%Y-%m-%d'),
                        next_date.strftime('%Y-%m-%d')
                    ))
            
            return gaps
            
        except Exception:
            return []
    
    def _determine_quality_level(self, score: float) -> DataQualityLevel:
        """Determine quality level based on score"""
        if score >= 95:
            return DataQualityLevel.EXCELLENT
        elif score >= 85:
            return DataQualityLevel.GOOD
        elif score >= 70:
            return DataQualityLevel.FAIR
        elif score >= 50:
            return DataQualityLevel.POOR
        else:
            return DataQualityLevel.UNUSABLE
    
    def _assess_elliott_wave_readiness(self, data: pd.DataFrame, quality_score: float, issues: List[DataQualityIssue]) -> bool:
        """Assess if data is ready for Elliott Wave analysis"""
        try:
            # Minimum requirements for Elliott Wave analysis
            min_requirements = [
                len(data) >= self.min_data_days,  # Sufficient data points
                quality_score >= 70,             # Minimum quality score
                'high' in data.columns and 'low' in data.columns and 'close' in data.columns,  # Essential price data
                not any(issue.severity == "CRITICAL" for issue in issues)  # No critical issues
            ]
            
            return all(min_requirements)
            
        except Exception:
            return False
    
    def _generate_recommendations(self, issues: List[DataQualityIssue], elliott_readiness: bool) -> List[str]:
        """Generate actionable recommendations"""
        recommendations = []
        
        try:
            # Group issues by type
            issue_types = {}
            for issue in issues:
                issue_type = issue.issue_type
                if issue_type not in issue_types:
                    issue_types[issue_type] = []
                issue_types[issue_type].append(issue)
            
            # Generate specific recommendations
            if DataIssueType.MISSING_DATA in issue_types:
                recommendations.append("Fill missing data through data source improvements or interpolation")
            
            if DataIssueType.INCONSISTENT_DATA in issue_types:
                recommendations.append("Implement data validation rules to prevent inconsistent data entry")
            
            if DataIssueType.OUTLIER_DATA in issue_types:
                recommendations.append("Review and validate outlier data points for accuracy")
            
            if DataIssueType.STALE_DATA in issue_types:
                recommendations.append("Set up automated data refresh to ensure timely updates")
            
            if DataIssueType.DUPLICATE_DATA in issue_types:
                recommendations.append("Implement duplicate detection and removal processes")
            
            # Elliott Wave specific recommendations
            if not elliott_readiness:
                recommendations.append("Address critical data quality issues before running Elliott Wave analysis")
                recommendations.append("Ensure minimum 60 days of clean OHLC data for reliable Elliott Wave patterns")
            else:
                recommendations.append("Data quality is sufficient for Elliott Wave analysis")
            
            # Critical issues recommendations
            critical_issues = [issue for issue in issues if issue.severity == "CRITICAL"]
            if critical_issues:
                recommendations.insert(0, "PRIORITY: Fix critical data quality issues immediately")
            
            return recommendations
            
        except Exception:
            return ["Review data quality issues and fix accordingly"]

def run_data_quality_check(symbols: List[str] = None, save_to_db: bool = True) -> Dict[str, Any]:
    """
    Run data quality check pada multiple symbols
    
    Parameters:
    symbols (List[str]): List of symbols to check
    save_to_db (bool): Whether to save results to database
    
    Returns:
    Dict: Quality check results summary
    """
    try:
        if symbols is None:
            # Default to top Indonesian stocks
            symbols = [
                'BBCA', 'BBRI', 'BMRI', 'BBNI', 'ASII', 'TLKM', 'UNVR', 'ICBP',
                'GGRM', 'INDF', 'PGAS', 'ADRO', 'ITMG', 'KLBF', 'CPIN'
            ]
        
        checker = ElliottDataQualityChecker()
        results = {
            'check_date': datetime.now().isoformat(),
            'symbols_checked': len(symbols),
            'quality_summary': {},
            'elliott_ready_symbols': [],
            'symbols_with_issues': [],
            'overall_statistics': {},
            'detailed_reports': {}
        }
        
        quality_scores = []
        elliott_ready_count = 0
        
        for symbol in symbols:
            logger.info(f"Checking data quality for {symbol}")
            
            report = checker.check_symbol_data_quality(symbol)
            
            # Store detailed report
            results['detailed_reports'][symbol] = {
                'quality_score': report.overall_quality_score,
                'quality_level': report.quality_level.value,
                'completeness_pct': report.data_completeness_pct,
                'consistency_pct': report.data_consistency_pct,
                'freshness_days': report.data_freshness_days,
                'total_records': report.total_records,
                'elliott_ready': report.elliott_wave_readiness,
                'issues_count': len(report.issues),
                'critical_issues': len([i for i in report.issues if i.severity == "CRITICAL"])
            }
            
            quality_scores.append(report.overall_quality_score)
            
            if report.elliott_wave_readiness:
                elliott_ready_count += 1
                results['elliott_ready_symbols'].append(symbol)
            
            if report.issues:
                results['symbols_with_issues'].append({
                    'symbol': symbol,
                    'issues_count': len(report.issues),
                    'top_issues': [issue.description for issue in report.issues[:3]]
                })
            
            # Save to database if requested
            if save_to_db:
                _save_quality_report_to_db(report)
        
        # Calculate overall statistics
        results['overall_statistics'] = {
            'average_quality_score': np.mean(quality_scores),
            'elliott_ready_percentage': (elliott_ready_count / len(symbols)) * 100,
            'symbols_with_excellent_quality': len([s for s in quality_scores if s >= 95]),
            'symbols_with_good_quality': len([s for s in quality_scores if 85 <= s < 95]),
            'symbols_with_fair_quality': len([s for s in quality_scores if 70 <= s < 85]),
            'symbols_with_poor_quality': len([s for s in quality_scores if 50 <= s < 70]),
            'symbols_unusable': len([s for s in quality_scores if s < 50])
        }
        
        # Quality summary
        results['quality_summary'] = {
            'total_symbols': len(symbols),
            'elliott_ready': elliott_ready_count,
            'need_attention': len(results['symbols_with_issues']),
            'average_score': results['overall_statistics']['average_quality_score']
        }
        
        logger.info(f"Data quality check completed: {elliott_ready_count}/{len(symbols)} symbols ready for Elliott Wave analysis")
        
        return results
        
    except Exception as e:
        logger.error(f"Error running data quality check: {str(e)}")
        return {'error': str(e)}

def _save_quality_report_to_db(report: DataQualityReport) -> None:
    """Save quality report to database"""
    try:
        conn = get_database_connection()
        cursor = conn.cursor()
        
        # Create table if not exists
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS public_analytics.elliott_data_quality (
            id SERIAL PRIMARY KEY,
            symbol TEXT NOT NULL,
            check_date DATE NOT NULL,
            overall_quality_score NUMERIC(5,2),
            quality_level TEXT,
            data_completeness_pct NUMERIC(5,2),
            data_consistency_pct NUMERIC(5,2),
            data_freshness_days INTEGER,
            total_records INTEGER,
            date_range_start DATE,
            date_range_end DATE,
            elliott_wave_readiness BOOLEAN,
            issues_count INTEGER,
            critical_issues_count INTEGER,
            issues_detail JSONB,
            recommendations JSONB,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(symbol, check_date)
        )
        """)
        
        # Insert report
        cursor.execute("""
        INSERT INTO public_analytics.elliott_data_quality
        (symbol, check_date, overall_quality_score, quality_level, data_completeness_pct,
         data_consistency_pct, data_freshness_days, total_records, date_range_start,
         date_range_end, elliott_wave_readiness, issues_count, critical_issues_count,
         issues_detail, recommendations)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (symbol, check_date)
        DO UPDATE SET
            overall_quality_score = EXCLUDED.overall_quality_score,
            quality_level = EXCLUDED.quality_level,
            data_completeness_pct = EXCLUDED.data_completeness_pct,
            data_consistency_pct = EXCLUDED.data_consistency_pct,
            data_freshness_days = EXCLUDED.data_freshness_days,
            total_records = EXCLUDED.total_records,
            date_range_start = EXCLUDED.date_range_start,
            date_range_end = EXCLUDED.date_range_end,
            elliott_wave_readiness = EXCLUDED.elliott_wave_readiness,
            issues_count = EXCLUDED.issues_count,
            critical_issues_count = EXCLUDED.critical_issues_count,
            issues_detail = EXCLUDED.issues_detail,
            recommendations = EXCLUDED.recommendations,
            created_at = CURRENT_TIMESTAMP
        """, (
            report.symbol,
            report.check_date.date(),
            report.overall_quality_score,
            report.quality_level.value,
            report.data_completeness_pct,
            report.data_consistency_pct,
            report.data_freshness_days,
            report.total_records,
            pd.to_datetime(report.date_range[0]).date() if report.date_range[0] != "N/A" else None,
            pd.to_datetime(report.date_range[1]).date() if report.date_range[1] != "N/A" else None,
            report.elliott_wave_readiness,
            len(report.issues),
            len([i for i in report.issues if i.severity == "CRITICAL"]),
            json.dumps([{
                'type': issue.issue_type.value,
                'severity': issue.severity,
                'description': issue.description,
                'impact_score': issue.impact_score
            } for issue in report.issues]),
            json.dumps(report.recommendations)
        ))
        
        conn.commit()
        cursor.close()
        conn.close()
        
    except Exception as e:
        logger.error(f"Error saving quality report to database: {str(e)}")

# Export functions
__all__ = [
    'ElliottDataQualityChecker',
    'DataQualityReport',
    'DataQualityIssue',
    'DataQualityLevel',
    'DataIssueType',
    'run_data_quality_check'
]