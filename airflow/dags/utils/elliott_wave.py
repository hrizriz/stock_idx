# Save this as: airflow/dags/utils/elliott_wave.py

import pandas as pd
import numpy as np
import logging
import json
from datetime import datetime, timedelta
from scipy.signal import find_peaks, find_peaks_cwt
from .database import get_database_connection, fetch_data, create_table_if_not_exists
from .telegram import send_telegram_message

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ElliottWaveAnalyzer:
    """
    Elliott Wave Analysis untuk identifikasi pola 5-3 wave
    
    Elliott Wave Theory:
    - Wave 1-5: Impulse waves (trend direction)
    - Wave A-C: Corrective waves (against trend)
    - Rules: Wave 2 < 100% of Wave 1, Wave 3 ≠ shortest, Wave 4 doesn't overlap Wave 1
    """
    
    def __init__(self):
        # Fibonacci ratios yang umum digunakan dalam Elliott Wave
        self.fibonacci_ratios = [0.236, 0.382, 0.5, 0.618, 0.786, 1.0, 1.272, 1.618, 2.618]
        self.min_wave_size = 0.015  # Minimum 1.5% price movement untuk wave yang valid
        
    def identify_swing_points(self, df, window=5):
        """
        Identifikasi swing highs dan swing lows menggunakan algoritma peak detection
        
        Parameters:
        df (DataFrame): Data dengan kolom high, low, close, date
        window (int): Window size untuk peak detection
        
        Returns:
        DataFrame: Data dengan swing points yang teridentifikasi
        """
        try:
            df = df.copy()
            df = df.sort_values('date').reset_index(drop=True)
            
            # Identifikasi swing highs menggunakan find_peaks
            high_peaks, high_properties = find_peaks(
                df['high'], 
                distance=window, 
                prominence=df['high'].std() * 0.1  # Minimum prominence
            )
            df['swing_high'] = False
            df.loc[high_peaks, 'swing_high'] = True
            
            # Identifikasi swing lows (invert data untuk find_peaks)
            low_peaks, low_properties = find_peaks(
                -df['low'], 
                distance=window,
                prominence=df['low'].std() * 0.1
            )
            df['swing_low'] = False
            df.loc[low_peaks, 'swing_low'] = True
            
            # Create pivot points dengan prioritas
            df['pivot_type'] = 'none'
            df.loc[df['swing_high'], 'pivot_type'] = 'high'
            df.loc[df['swing_low'], 'pivot_type'] = 'low'
            
            # Filter pivot points berdasarkan significance
            df['price_significance'] = 0
            for i in range(len(df)):
                if df.loc[i, 'pivot_type'] != 'none':
                    # Calculate local price range untuk menentukan significance
                    start_idx = max(0, i - window)
                    end_idx = min(len(df), i + window + 1)
                    local_range = df.loc[start_idx:end_idx, 'high'].max() - df.loc[start_idx:end_idx, 'low'].min()
                    
                    if df.loc[i, 'pivot_type'] == 'high':
                        price_level = df.loc[i, 'high']
                    else:
                        price_level = df.loc[i, 'low']
                    
                    df.loc[i, 'price_significance'] = local_range / price_level if price_level > 0 else 0
            
            # Filter out insignificant pivots
            df.loc[df['price_significance'] < self.min_wave_size, 'pivot_type'] = 'none'
            
            return df
            
        except Exception as e:
            logger.error(f"Error identifying swing points: {str(e)}")
            return df
    
    def extract_waves(self, df):
        """
        Extract wave patterns dari swing points yang telah diidentifikasi
        
        Parameters:
        df (DataFrame): Data dengan swing points
        
        Returns:
        list: List of waves dengan informasi detail
        """
        try:
            # Filter hanya pivot points yang significant
            pivots = df[df['pivot_type'] != 'none'].copy().reset_index(drop=True)
            if len(pivots) < 3:
                return []
            
            waves = []
            
            for i in range(len(pivots) - 1):
                current = pivots.iloc[i]
                next_point = pivots.iloc[i + 1]
                
                # Determine wave direction dan price levels
                if current['pivot_type'] == 'low' and next_point['pivot_type'] == 'high':
                    direction = 'up'
                    start_price = current['low']
                    end_price = next_point['high']
                    start_type = 'low'
                    end_type = 'high'
                elif current['pivot_type'] == 'high' and next_point['pivot_type'] == 'low':
                    direction = 'down'
                    start_price = current['high']
                    end_price = next_point['low']
                    start_type = 'high'
                    end_type = 'low'
                else:
                    continue  # Skip invalid transitions
                
                # Calculate wave metrics
                magnitude = abs(end_price - start_price) / start_price
                duration = (next_point['date'] - current['date']).days
                price_change = (end_price - start_price) / start_price * 100
                
                # Calculate volume profile if available
                volume_avg = 0
                if 'volume' in current and 'volume' in next_point:
                    # Get volume data for the wave period
                    wave_data = df[(df['date'] >= current['date']) & (df['date'] <= next_point['date'])]
                    volume_avg = wave_data['volume'].mean() if not wave_data.empty else 0
                
                wave = {
                    'wave_number': i + 1,
                    'start_date': current['date'],
                    'end_date': next_point['date'],
                    'start_price': float(start_price),
                    'end_price': float(end_price),
                    'start_type': start_type,  # 'high' or 'low'
                    'end_type': end_type,      # 'high' or 'low'
                    'direction': direction,    # 'up' or 'down'
                    'magnitude': float(magnitude),
                    'price_change_pct': float(price_change),
                    'duration_days': int(duration),
                    'avg_volume': float(volume_avg),
                    'start_index': int(current.name),
                    'end_index': int(next_point.name)
                }
                
                waves.append(wave)
            
            return waves
            
        except Exception as e:
            logger.error(f"Error extracting waves: {str(e)}")
            return []
    
    def identify_elliott_pattern(self, waves):
        """
        Identifikasi pola Elliott Wave 5-3 berdasarkan aturan Elliott Wave Theory
        
        Elliott Wave Rules:
        1. Wave 2 never retraces more than 100% of Wave 1
        2. Wave 3 is never the shortest among waves 1, 3, and 5
        3. Wave 4 never overlaps with Wave 1
        
        Parameters:
        waves (list): List of waves
        
        Returns:
        dict: Elliott wave pattern analysis dengan confidence score
        """
        try:
            if len(waves) < 5:
                return {
                    'pattern_found': False, 
                    'reason': f'Insufficient waves: {len(waves)} (need minimum 5)',
                    'confidence': 0
                }
            
            # Ambil waves untuk analisis (minimal 5, maksimal 8)
            analysis_waves = waves[-8:] if len(waves) >= 8 else waves[-5:]
            
            elliott_analysis = {
                'pattern_found': False,
                'pattern_type': None,
                'wave_labels': [],
                'current_wave': None,
                'next_expected': None,
                'fibonacci_levels': {},
                'confidence': 0,
                'rules_validation': {},
                'wave_characteristics': []
            }
            
            # Analisis untuk 5-wave impulse pattern
            if len(analysis_waves) >= 5:
                impulse_waves = analysis_waves[:5]
                confidence_score = 0
                rules_passed = []
                rules_failed = []
                
                # Validasi aturan Elliott Wave
                wave1 = impulse_waves[0]
                wave2 = impulse_waves[1]
                wave3 = impulse_waves[2]
                wave4 = impulse_waves[3]
                wave5 = impulse_waves[4]
                
                # Rule 1: Wave 2 retracement validation
                if self._validate_wave2_retracement(wave1, wave2):
                    confidence_score += 25
                    rules_passed.append("Wave 2 retracement valid")
                else:
                    rules_failed.append("Wave 2 retracement exceeds 100%")
                
                # Rule 2: Wave 3 is not the shortest
                if self._validate_wave3_not_shortest(wave1, wave3, wave5):
                    confidence_score += 25
                    rules_passed.append("Wave 3 is not the shortest")
                else:
                    rules_failed.append("Wave 3 is the shortest wave")
                
                # Rule 3: Wave 4 overlap validation
                if self._validate_wave4_no_overlap(wave1, wave4):
                    confidence_score += 20
                    rules_passed.append("Wave 4 does not overlap Wave 1")
                else:
                    rules_failed.append("Wave 4 overlaps with Wave 1")
                
                # Fibonacci ratio validation
                fib_score, fib_details = self._check_fibonacci_ratios(impulse_waves)
                confidence_score += fib_score
                
                # Time and volume validation
                time_volume_score = self._validate_time_volume_relationships(impulse_waves)
                confidence_score += time_volume_score
                
                # Pattern recognition berdasarkan confidence
                if confidence_score >= 60:
                    elliott_analysis['pattern_found'] = True
                    elliott_analysis['pattern_type'] = 'impulse'
                    elliott_analysis['wave_labels'] = ['1', '2', '3', '4', '5']
                    elliott_analysis['confidence'] = confidence_score
                    elliott_analysis['rules_validation'] = {
                        'passed': rules_passed,
                        'failed': rules_failed
                    }
                    
                    # Determine current wave position
                    elliott_analysis.update(self._determine_current_position(analysis_waves))
                    
                    # Calculate Fibonacci levels untuk trading
                    elliott_analysis['fibonacci_levels'] = self._calculate_comprehensive_fibonacci_levels(impulse_waves)
                    
                    # Wave characteristics
                    elliott_analysis['wave_characteristics'] = self._analyze_wave_characteristics(impulse_waves)
            
            return elliott_analysis
            
        except Exception as e:
            logger.error(f"Error identifying Elliott pattern: {str(e)}")
            return {'pattern_found': False, 'error': str(e), 'confidence': 0}
    
    def _validate_wave2_retracement(self, wave1, wave2):
        """Validate Elliott Wave Rule 1: Wave 2 < 100% retracement"""
        try:
            if wave1['direction'] != 'up' or wave2['direction'] != 'down':
                return False
            
            wave1_size = abs(wave1['end_price'] - wave1['start_price'])
            wave2_size = abs(wave2['end_price'] - wave2['start_price'])
            
            retracement_ratio = wave2_size / wave1_size
            return retracement_ratio < 0.99  # Allow small tolerance
            
        except Exception:
            return False
    
    def _validate_wave3_not_shortest(self, wave1, wave3, wave5):
        """Validate Elliott Wave Rule 2: Wave 3 is not the shortest"""
        try:
            wave1_magnitude = wave1['magnitude']
            wave3_magnitude = wave3['magnitude']
            wave5_magnitude = wave5['magnitude']
            
            magnitudes = [wave1_magnitude, wave3_magnitude, wave5_magnitude]
            return wave3_magnitude != min(magnitudes)
            
        except Exception:
            return False
    
    def _validate_wave4_no_overlap(self, wave1, wave4):
        """Validate Elliott Wave Rule 3: Wave 4 does not overlap Wave 1"""
        try:
            if wave1['direction'] == 'up':
                wave1_high = wave1['end_price']
                wave4_low = wave4['end_price'] if wave4['direction'] == 'down' else wave4['start_price']
                return wave4_low > wave1_high * 0.85  # Allow small tolerance
            else:
                wave1_low = wave1['end_price']
                wave4_high = wave4['end_price'] if wave4['direction'] == 'up' else wave4['start_price']
                return wave4_high < wave1_low * 1.15  # Allow small tolerance
                
        except Exception:
            return False
    
    def _check_fibonacci_ratios(self, waves):
        """Check adherence to Fibonacci ratios dengan detailed analysis"""
        try:
            score = 0
            fib_details = []
            
            if len(waves) < 3:
                return score, fib_details
            
            # Wave 2 retracement ratios (38.2%, 50%, 61.8%)
            if len(waves) >= 2:
                wave1_size = waves[0]['magnitude']
                wave2_size = waves[1]['magnitude']
                retracement_ratio = wave2_size / wave1_size
                
                fib_targets = [(0.382, 'Strong'), (0.5, 'Moderate'), (0.618, 'Deep')]
                for fib_level, strength in fib_targets:
                    if abs(retracement_ratio - fib_level) < 0.08:  # 8% tolerance
                        score += 10
                        fib_details.append(f"Wave 2 {strength} retracement ({fib_level:.1%})")
                        break
            
            # Wave 3 extension ratios (161.8%, 261.8%, 423.6%)
            if len(waves) >= 3:
                wave1_size = waves[0]['magnitude']
                wave3_size = waves[2]['magnitude']
                extension_ratio = wave3_size / wave1_size
                
                fib_targets = [(1.618, 'Common'), (2.618, 'Strong'), (4.236, 'Extreme')]
                for fib_level, strength in fib_targets:
                    if abs(extension_ratio - fib_level) < 0.15:  # 15% tolerance
                        score += 15
                        fib_details.append(f"Wave 3 {strength} extension ({fib_level:.1%})")
                        break
            
            # Wave 4 retracement ratios (23.6%, 38.2%)
            if len(waves) >= 4:
                wave3_size = waves[2]['magnitude']
                wave4_size = waves[3]['magnitude']
                retracement_ratio = wave4_size / wave3_size
                
                fib_targets = [(0.236, 'Shallow'), (0.382, 'Common')]
                for fib_level, strength in fib_targets:
                    if abs(retracement_ratio - fib_level) < 0.05:  # 5% tolerance
                        score += 10
                        fib_details.append(f"Wave 4 {strength} retracement ({fib_level:.1%})")
                        break
            
            # Wave 5 relationships
            if len(waves) >= 5:
                wave1_size = waves[0]['magnitude']
                wave5_size = waves[4]['magnitude']
                
                # Wave 5 = Wave 1 (common)
                if abs(wave5_size - wave1_size) / wave1_size < 0.1:  # 10% tolerance
                    score += 10
                    fib_details.append("Wave 5 equals Wave 1 (100%)")
                
                # Wave 5 = 0.618 * Wave 1 to Wave 3 distance
                wave1_to_3_size = waves[0]['magnitude'] + waves[2]['magnitude']
                target_wave5 = wave1_to_3_size * 0.618
                if abs(wave5_size - target_wave5) / target_wave5 < 0.15:
                    score += 8
                    fib_details.append("Wave 5 = 61.8% of Wave 1-3 distance")
            
            return min(score, 25), fib_details  # Cap at 25 points
            
        except Exception as e:
            logger.error(f"Error checking Fibonacci ratios: {str(e)}")
            return 0, []
    
    def _validate_time_volume_relationships(self, waves):
        """Validate time and volume relationships dalam Elliott Waves"""
        try:
            score = 0
            
            if len(waves) < 3:
                return score
            
            # Wave 3 should have higher volume than Wave 1
            if (len(waves) >= 3 and 
                waves[2].get('avg_volume', 0) > waves[0].get('avg_volume', 0)):
                score += 8
            
            # Wave 5 often has lower volume than Wave 3 (divergence)
            if (len(waves) >= 5 and 
                waves[4].get('avg_volume', 0) < waves[2].get('avg_volume', 0)):
                score += 5
            
            # Time relationships: Wave 4 often takes longer than Wave 2
            if (len(waves) >= 4 and 
                waves[3].get('duration_days', 0) >= waves[1].get('duration_days', 0)):
                score += 5
            
            return score
            
        except Exception:
            return 0
    
    def _determine_current_position(self, waves):
        """Determine current wave position dalam Elliott cycle"""
        try:
            position_info = {
                'current_wave': None,
                'next_expected': None,
                'cycle_position': None
            }
            
            if len(waves) == 5:
                position_info['current_wave'] = '5'
                position_info['next_expected'] = 'A (corrective)'
                position_info['cycle_position'] = 'End of impulse'
            elif len(waves) == 6:
                position_info['current_wave'] = 'A'
                position_info['next_expected'] = 'B (counter-trend bounce)'
                position_info['cycle_position'] = 'Corrective phase'
            elif len(waves) == 7:
                position_info['current_wave'] = 'B'
                position_info['next_expected'] = 'C (final corrective)'
                position_info['cycle_position'] = 'Corrective bounce'
            elif len(waves) >= 8:
                position_info['current_wave'] = 'C'
                position_info['next_expected'] = 'New cycle Wave 1'
                position_info['cycle_position'] = 'End of correction'
            else:
                # Determine based on wave count
                wave_count = len(waves) % 8
                if wave_count <= 5:
                    position_info['current_wave'] = str(wave_count)
                    position_info['cycle_position'] = 'Impulse phase'
                else:
                    corrective_labels = ['A', 'B', 'C']
                    corrective_index = (wave_count - 6)
                    if corrective_index < len(corrective_labels):
                        position_info['current_wave'] = corrective_labels[corrective_index]
                        position_info['cycle_position'] = 'Corrective phase'
            
            return position_info
            
        except Exception:
            return {'current_wave': None, 'next_expected': None, 'cycle_position': None}
    
    def _calculate_comprehensive_fibonacci_levels(self, waves):
        """Calculate comprehensive Fibonacci levels untuk trading strategy"""
        try:
            levels = {
                'retracement': {},
                'extension': {},
                'projection': {},
                'time_targets': {}
            }
            
            if len(waves) < 2:
                return levels
            
            # Current wave untuk calculation base
            latest_wave = waves[-1]
            previous_wave = waves[-2] if len(waves) > 1 else None
            
            if previous_wave:
                # Price levels
                high_price = max(latest_wave['start_price'], latest_wave['end_price'])
                low_price = min(latest_wave['start_price'], latest_wave['end_price'])
                price_range = high_price - low_price
                
                # Retracement levels (for corrective waves)
                levels['retracement'] = {
                    '23.6%': high_price - (price_range * 0.236),
                    '38.2%': high_price - (price_range * 0.382),
                    '50.0%': high_price - (price_range * 0.5),
                    '61.8%': high_price - (price_range * 0.618),
                    '78.6%': high_price - (price_range * 0.786),
                    '88.6%': high_price - (price_range * 0.886)
                }
                
                # Extension levels (for impulse waves)
                if latest_wave['direction'] == 'up':
                    levels['extension'] = {
                        '127.2%': low_price + (price_range * 1.272),
                        '161.8%': low_price + (price_range * 1.618),
                        '200.0%': low_price + (price_range * 2.0),
                        '261.8%': low_price + (price_range * 2.618),
                        '361.8%': low_price + (price_range * 3.618)
                    }
                else:
                    levels['extension'] = {
                        '127.2%': high_price - (price_range * 1.272),
                        '161.8%': high_price - (price_range * 1.618),
                        '200.0%': high_price - (price_range * 2.0),
                        '261.8%': high_price - (price_range * 2.618),
                        '361.8%': high_price - (price_range * 3.618)
                    }
                
                # Wave projections jika ada Wave 1 data
                if len(waves) >= 3:
                    wave1 = waves[0]
                    wave1_range = abs(wave1['end_price'] - wave1['start_price'])
                    
                    # Project Wave 5 berdasarkan Wave 1
                    if latest_wave['direction'] == 'up':
                        levels['projection'] = {
                            'Wave5_100%_of_Wave1': low_price + wave1_range,
                            'Wave5_61.8%_of_Wave1': low_price + (wave1_range * 0.618),
                            'Wave5_161.8%_of_Wave1': low_price + (wave1_range * 1.618)
                        }
                    else:
                        levels['projection'] = {
                            'Wave5_100%_of_Wave1': high_price - wave1_range,
                            'Wave5_61.8%_of_Wave1': high_price - (wave1_range * 0.618),
                            'Wave5_161.8%_of_Wave1': high_price - (wave1_range * 1.618)
                        }
                
                # Time projections
                duration = latest_wave.get('duration_days', 0)
                if duration > 0:
                    levels['time_targets'] = {
                        '61.8%_time': duration * 0.618,
                        '100%_time': duration,
                        '161.8%_time': duration * 1.618
                    }
            
            return levels
            
        except Exception as e:
            logger.error(f"Error calculating Fibonacci levels: {str(e)}")
            return {}
    
    def _analyze_wave_characteristics(self, waves):
        """Analyze individual wave characteristics"""
        try:
            characteristics = []
            
            for i, wave in enumerate(waves):
                char = {
                    'wave_number': i + 1,
                    'direction': wave['direction'],
                    'magnitude': wave['magnitude'],
                    'duration': wave.get('duration_days', 0),
                    'strength': self._classify_wave_strength(wave),
                    'volume_profile': self._analyze_volume_profile(wave),
                    'fibonacci_relationship': None
                }
                
                # Add Fibonacci relationship jika ada wave sebelumnya
                if i > 0:
                    prev_wave = waves[i-1]
                    char['fibonacci_relationship'] = self._calculate_wave_relationship(prev_wave, wave)
                
                characteristics.append(char)
            
            return characteristics
            
        except Exception:
            return []
    
    def _classify_wave_strength(self, wave):
        """Classify wave strength berdasarkan magnitude dan duration"""
        magnitude = wave.get('magnitude', 0)
        duration = wave.get('duration_days', 0)
        
        if magnitude > 0.15 and duration >= 5:  # >15% move in 5+ days
            return 'Strong'
        elif magnitude > 0.08 and duration >= 3:  # >8% move in 3+ days
            return 'Moderate'
        elif magnitude > 0.03:  # >3% move
            return 'Weak'
        else:
            return 'Very Weak'
    
    def _analyze_volume_profile(self, wave):
        """Analyze volume profile untuk wave"""
        avg_volume = wave.get('avg_volume', 0)
        
        if avg_volume == 0:
            return 'Unknown'
        elif avg_volume > 1000000:  # High volume threshold
            return 'High Volume'
        elif avg_volume > 500000:  # Medium volume threshold
            return 'Medium Volume'
        else:
            return 'Low Volume'
    
    def _calculate_wave_relationship(self, prev_wave, current_wave):
        """Calculate Fibonacci relationship between waves"""
        try:
            prev_magnitude = prev_wave.get('magnitude', 0)
            current_magnitude = current_wave.get('magnitude', 0)
            
            if prev_magnitude == 0:
                return 'Unknown'
            
            ratio = current_magnitude / prev_magnitude
            
            # Check common Fibonacci ratios
            fib_ratios = {
                0.236: '23.6%',
                0.382: '38.2%',
                0.5: '50%',
                0.618: '61.8%',
                0.786: '78.6%',
                1.0: '100%',
                1.272: '127.2%',
                1.618: '161.8%',
                2.618: '261.8%'
            }
            
            for fib_ratio, label in fib_ratios.items():
                if abs(ratio - fib_ratio) < 0.1:  # 10% tolerance
                    return f'{label} of previous wave'
            
            return f'{ratio:.2f}x of previous wave'
            
        except Exception:
            return 'Unknown'
    
    def generate_trading_signals(self, elliott_analysis, current_price, symbol=None):
        """
        Generate comprehensive trading signals based on Elliott Wave analysis
        
        Parameters:
        elliott_analysis (dict): Elliott Wave analysis results
        current_price (float): Current stock price
        symbol (str): Stock symbol for context
        
        Returns:
        dict: Comprehensive trading signals dan recommendations
        """
        try:
            signals = {
                'signal': 'HOLD',
                'confidence': 0,
                'entry_price': None,
                'stop_loss': None,
                'take_profit_1': None,
                'take_profit_2': None,
                'risk_reward_ratio': None,
                'position_size_recommendation': 'Standard',
                'time_horizon': 'Medium',
                'reasoning': [],
                'risk_factors': []
            }
            
            if not elliott_analysis.get('pattern_found', False):
                signals['reasoning'].append('No clear Elliott Wave pattern identified')
                signals['risk_factors'].append('Pattern uncertainty')
                return signals
            
            current_wave = elliott_analysis.get('current_wave')
            next_expected = elliott_analysis.get('next_expected')
            pattern_confidence = elliott_analysis.get('confidence', 0)
            fib_levels = elliott_analysis.get('fibonacci_levels', {})
            
            # Base confidence dari pattern recognition
            base_confidence = min(pattern_confidence, 100)
            
            # Trading logic berdasarkan current wave position
            if current_wave == '2':
                # End of Wave 2 - Strongest buy signal for Wave 3
                signals['signal'] = 'STRONG_BUY'
                signals['confidence'] = min(base_confidence + 15, 95)
                signals['position_size_recommendation'] = 'Large'
                signals['time_horizon'] = 'Medium'
                signals['reasoning'].append('End of Wave 2 - Most powerful Wave 3 setup')
                signals['reasoning'].append('Wave 3 typically strongest and longest impulse wave')
                
                # Price targets menggunakan Fibonacci
                if 'retracement' in fib_levels and 'extension' in fib_levels:
                    signals['entry_price'] = fib_levels['retracement'].get('61.8%', current_price * 0.98)
                    signals['stop_loss'] = fib_levels['retracement'].get('78.6%', current_price * 0.95)
                    signals['take_profit_1'] = fib_levels['extension'].get('161.8%', current_price * 1.15)
                    signals['take_profit_2'] = fib_levels['extension'].get('261.8%', current_price * 1.25)
                
            elif current_wave == '4':
                # End of Wave 4 - Good buy signal for Wave 5
                signals['signal'] = 'BUY'
                signals['confidence'] = min(base_confidence + 5, 85)
                signals['position_size_recommendation'] = 'Standard'
                signals['time_horizon'] = 'Short'
                signals['reasoning'].append('End of Wave 4 - Final rally Wave 5 expected')
                signals['reasoning'].append('Wave 5 often reaches Wave 1 extension levels')
                
                if 'retracement' in fib_levels and 'projection' in fib_levels:
                    signals['entry_price'] = fib_levels['retracement'].get('38.2%', current_price * 0.99)
                    signals['stop_loss'] = fib_levels['retracement'].get('61.8%', current_price * 0.96)
                    signals['take_profit_1'] = fib_levels['projection'].get('Wave5_100%_of_Wave1', current_price * 1.08)
                    signals['take_profit_2'] = fib_levels['projection'].get('Wave5_161.8%_of_Wave1', current_price * 1.12)
                
                signals['risk_factors'].append('Wave 5 often shows momentum divergence')
            
            elif current_wave == '5':
                # End of Wave 5 - Strong sell signal
                signals['signal'] = 'SELL'
                signals['confidence'] = min(base_confidence + 10, 90)
                signals['position_size_recommendation'] = 'Large'
                signals['time_horizon'] = 'Medium'
                signals['reasoning'].append('End of Wave 5 - Major corrective phase expected')
                signals['reasoning'].append('ABC correction typically retraces 38-62% of entire impulse')
                
                # Target untuk correction
                if 'retracement' in fib_levels:
                    signals['take_profit_1'] = fib_levels['retracement'].get('38.2%', current_price * 0.92)
                    signals['take_profit_2'] = fib_levels['retracement'].get('61.8%', current_price * 0.85)
                
                signals['risk_factors'].append('Possible extended Wave 5 scenario')
            
            elif current_wave == 'A':
                # Wave A of correction - Caution signal
                signals['signal'] = 'HOLD'
                signals['confidence'] = min(base_confidence - 10, 70)
                signals['position_size_recommendation'] = 'Small'
                signals['time_horizon'] = 'Short'
                signals['reasoning'].append('Wave A correction - Waiting for Wave B bounce to sell')
                signals['risk_factors'].append('Uncertain if correction is simple or complex')
            
            elif current_wave == 'B':
                # Wave B bounce - Sell opportunity
                signals['signal'] = 'SELL'
                signals['confidence'] = min(base_confidence, 80)
                signals['position_size_recommendation'] = 'Standard'
                signals['time_horizon'] = 'Short'
                signals['reasoning'].append('Wave B bounce - Final opportunity before Wave C decline')
                signals['reasoning'].append('Wave C typically equals Wave A in magnitude')
                
                if 'retracement' in fib_levels:
                    signals['take_profit_1'] = fib_levels['retracement'].get('50%', current_price * 0.90)
                    signals['take_profit_2'] = fib_levels['retracement'].get('61.8%', current_price * 0.85)
                
                signals['risk_factors'].append('Wave B can be irregular or running triangle')
            
            elif current_wave == 'C':
                # End of Wave C - Strongest buy signal for new cycle
                signals['signal'] = 'STRONG_BUY'
                signals['confidence'] = min(base_confidence + 20, 95)
                signals['position_size_recommendation'] = 'Large'
                signals['time_horizon'] = 'Long'
                signals['reasoning'].append('End of Wave C - New bullish Elliott cycle expected')
                signals['reasoning'].append('Best risk-reward setup at major correction completion')
                
                if 'extension' in fib_levels:
                    signals['take_profit_1'] = fib_levels['extension'].get('161.8%', current_price * 1.20)
                    signals['take_profit_2'] = fib_levels['extension'].get('261.8%', current_price * 1.35)
                
            else:
                # Unknown or transitional state
                signals['signal'] = 'HOLD'
                signals['confidence'] = max(base_confidence - 20, 30)
                signals['reasoning'].append('Transitional Elliott Wave state - Wait for clearer pattern')
                signals['risk_factors'].append('Pattern development incomplete')
            
            # Calculate risk-reward ratio
            if signals['entry_price'] and signals['stop_loss'] and signals['take_profit_1']:
                risk = abs(signals['entry_price'] - signals['stop_loss'])
                reward = abs(signals['take_profit_1'] - signals['entry_price'])
                signals['risk_reward_ratio'] = reward / risk if risk > 0 else 0
            
            # Adjust confidence berdasarkan Elliott pattern strength
            pattern_confidence_factor = elliott_analysis.get('confidence', 50) / 100
            signals['confidence'] = int(signals['confidence'] * pattern_confidence_factor)
            
            # Add Elliott-specific risk factors
            rules_validation = elliott_analysis.get('rules_validation', {})
            if rules_validation.get('failed'):
                signals['risk_factors'].extend(rules_validation['failed'])
                signals['confidence'] = max(signals['confidence'] - 15, 20)
            
            return signals
            
        except Exception as e:
            logger.error(f"Error generating Elliott Wave trading signals: {str(e)}")
            return {
                'signal': 'HOLD', 
                'confidence': 0, 
                'reasoning': [f'Error generating signals: {str(e)}'],
                'risk_factors': ['Signal generation error']
            }

# Main analysis functions
def analyze_elliott_waves_for_symbol(symbol, lookback_days=200):
    """
    Comprehensive Elliott Wave analysis untuk specific symbol
    
    Parameters:
    symbol (str): Stock symbol
    lookback_days (int): Number of days untuk historical analysis
    
    Returns:
    dict: Complete Elliott Wave analysis results
    """
    try:
        conn = get_database_connection()
        
        # Get historical data dengan volume information
        query = f"""
        SELECT 
            date, 
            high, 
            low, 
            close, 
            volume,
            open_price as open
        FROM public.daily_stock_summary
        WHERE symbol = '{symbol}'
        AND date >= CURRENT_DATE - INTERVAL '{lookback_days} days'
        ORDER BY date
        """
        
        df = pd.read_sql(query, conn)
        conn.close()
        
        if df.empty or len(df) < 30:
            return {
                'error': f'Insufficient data for {symbol}: {len(df)} days available',
                'minimum_required': 30
            }
        
        # Initialize Elliott Wave analyzer
        analyzer = ElliottWaveAnalyzer()
        
        # Step 1: Identify swing points
        logger.info(f"Identifying swing points for {symbol}")
        df_with_swings = analyzer.identify_swing_points(df, window=5)
        
        # Step 2: Extract waves
        logger.info(f"Extracting wave patterns for {symbol}")
        waves = analyzer.extract_waves(df_with_swings)
        
        if len(waves) < 3:
            return {
                'error': f'Insufficient wave patterns for {symbol}: {len(waves)} waves found',
                'minimum_required': 3,
                'suggestion': 'Try increasing lookback period or reducing minimum wave size'
            }
        
        # Step 3: Identify Elliott pattern
        logger.info(f"Analyzing Elliott Wave patterns for {symbol}")
        elliott_analysis = analyzer.identify_elliott_pattern(waves)
        
        # Step 4: Generate trading signals
        current_price = df['close'].iloc[-1]
        trading_signals = analyzer.generate_trading_signals(
            elliott_analysis, 
            current_price, 
            symbol
        )
        
        # Step 5: Compile comprehensive results
        results = {
            'symbol': symbol,
            'analysis_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'data_period': {
                'start_date': df['date'].min().strftime('%Y-%m-%d'),
                'end_date': df['date'].max().strftime('%Y-%m-%d'),
                'total_days': len(df)
            },
            'current_price': float(current_price),
            'price_context': {
                'period_high': float(df['high'].max()),
                'period_low': float(df['low'].min()),
                'current_position_pct': float((current_price - df['low'].min()) / (df['high'].max() - df['low'].min()) * 100)
            },
            'wave_analysis': {
                'total_waves_identified': len(waves),
                'significant_waves': len([w for w in waves if w['magnitude'] > 0.05]),
                'recent_waves': waves[-8:] if len(waves) >= 8 else waves
            },
            'elliott_pattern': elliott_analysis,
            'trading_signals': trading_signals,
            'fibonacci_levels': elliott_analysis.get('fibonacci_levels', {}),
            'technical_context': {
                'recent_volatility': float(df['close'].pct_change().tail(20).std() * np.sqrt(252)),
                'average_volume': float(df['volume'].tail(20).mean()),
                'volume_trend': 'Increasing' if df['volume'].tail(5).mean() > df['volume'].tail(20).mean() else 'Decreasing'
            }
        }
        
        logger.info(f"Elliott Wave analysis completed for {symbol}: Pattern confidence {elliott_analysis.get('confidence', 0)}%")
        
        return results
        
    except Exception as e:
        logger.error(f"Error analyzing Elliott Waves for {symbol}: {str(e)}")
        return {'error': str(e), 'symbol': symbol}

def elliott_wave_screening(max_symbols=50, min_data_days=100):
    """
    Screen multiple stocks untuk Elliott Wave opportunities
    
    Parameters:
    max_symbols (int): Maximum number of symbols to analyze
    min_data_days (int): Minimum data days required
    
    Returns:
    list: List of stocks dengan Elliott Wave opportunities
    """
    try:
        conn = get_database_connection()
        
        # Get active stocks dengan sufficient data dan liquidity
        query = f"""
        SELECT 
            symbol,
            COUNT(*) as data_days,
            AVG(volume) as avg_volume,
            MAX(close) - MIN(close) as price_range,
            AVG(close) as avg_price
        FROM public.daily_stock_summary
        WHERE date >= CURRENT_DATE - INTERVAL '200 days'
        GROUP BY symbol
        HAVING COUNT(*) >= {min_data_days}
        AND AVG(volume) >= 100000  -- Minimum liquidity
        AND (MAX(close) - MIN(close)) / AVG(close) >= 0.2  -- Minimum volatility
        ORDER BY AVG(volume) DESC
        LIMIT {max_symbols}
        """
        
        symbols_df = pd.read_sql(query, conn)
        conn.close()
        
        logger.info(f"Screening {len(symbols_df)} symbols for Elliott Wave opportunities")
        
        opportunities = []
        processed_count = 0
        
        for _, row in symbols_df.iterrows():
            symbol = row['symbol']
            processed_count += 1
            
            logger.info(f"Processing {symbol} ({processed_count}/{len(symbols_df)})")
            
            try:
                analysis = analyze_elliott_waves_for_symbol(symbol, lookback_days=150)
                
                if 'error' not in analysis:
                    trading_signals = analysis.get('trading_signals', {})
                    elliott_pattern = analysis.get('elliott_pattern', {})
                    
                    # Filter untuk strong signals atau patterns
                    signal_confidence = trading_signals.get('confidence', 0)
                    pattern_confidence = elliott_pattern.get('confidence', 0)
                    
                    if (signal_confidence >= 60 or pattern_confidence >= 70):
                        opportunity = {
                            'symbol': symbol,
                            'signal': trading_signals.get('signal', 'HOLD'),
                            'signal_confidence': signal_confidence,
                            'pattern_confidence': pattern_confidence,
                            'current_wave': elliott_pattern.get('current_wave'),
                            'next_expected': elliott_pattern.get('next_expected'),
                            'current_price': analysis['current_price'],
                            'entry_price': trading_signals.get('entry_price'),
                            'take_profit_1': trading_signals.get('take_profit_1'),
                            'stop_loss': trading_signals.get('stop_loss'),
                            'risk_reward_ratio': trading_signals.get('risk_reward_ratio'),
                            'reasoning': trading_signals.get('reasoning', []),
                            'time_horizon': trading_signals.get('time_horizon', 'Medium'),
                            'position_size': trading_signals.get('position_size_recommendation', 'Standard')
                        }
                        
                        opportunities.append(opportunity)
                        
            except Exception as e:
                logger.warning(f"Error analyzing {symbol}: {str(e)}")
                continue
        
        # Sort by combined confidence score
        opportunities.sort(
            key=lambda x: (x['signal_confidence'] + x['pattern_confidence']) / 2, 
            reverse=True
        )
        
        logger.info(f"Elliott Wave screening completed: {len(opportunities)} opportunities found from {processed_count} symbols")
        
        return opportunities
        
    except Exception as e:
        logger.error(f"Error in Elliott Wave screening: {str(e)}")
        return []

def save_elliott_analysis_to_db(analysis_results):
    """
    Save Elliott Wave analysis results ke database
    
    Parameters:
    analysis_results (list): List of analysis results
    """
    try:
        if not analysis_results:
            logger.warning("No Elliott Wave analysis results to save")
            return
        
        conn = get_database_connection()
        cursor = conn.cursor()
        
        # Create Elliott Wave analysis table
        create_table_if_not_exists(
            'public_analytics.elliott_wave_analysis',
            """
            CREATE TABLE public_analytics.elliott_wave_analysis (
                id SERIAL PRIMARY KEY,
                symbol TEXT,
                analysis_date DATE,
                current_price NUMERIC,
                elliott_pattern JSONB,
                trading_signal TEXT,
                signal_confidence INTEGER,
                pattern_confidence INTEGER,
                current_wave TEXT,
                next_expected TEXT,
                fibonacci_levels JSONB,
                entry_price NUMERIC,
                stop_loss NUMERIC,
                take_profit_1 NUMERIC,
                take_profit_2 NUMERIC,
                risk_reward_ratio NUMERIC,
                time_horizon TEXT,
                position_size_recommendation TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(symbol, analysis_date)
            )
            """
        )
        
        saved_count = 0
        for result in analysis_results:
            try:
                cursor.execute("""
                INSERT INTO public_analytics.elliott_wave_analysis
                (symbol, analysis_date, current_price, elliott_pattern, trading_signal, 
                 signal_confidence, pattern_confidence, current_wave, next_expected, 
                 fibonacci_levels, entry_price, stop_loss, take_profit_1, take_profit_2,
                 risk_reward_ratio, time_horizon, position_size_recommendation)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (symbol, analysis_date) 
                DO UPDATE SET
                    current_price = EXCLUDED.current_price,
                    elliott_pattern = EXCLUDED.elliott_pattern,
                    trading_signal = EXCLUDED.trading_signal,
                    signal_confidence = EXCLUDED.signal_confidence,
                    pattern_confidence = EXCLUDED.pattern_confidence,
                    current_wave = EXCLUDED.current_wave,
                    next_expected = EXCLUDED.next_expected,
                    fibonacci_levels = EXCLUDED.fibonacci_levels,
                    entry_price = EXCLUDED.entry_price,
                    stop_loss = EXCLUDED.stop_loss,
                    take_profit_1 = EXCLUDED.take_profit_1,
                    take_profit_2 = EXCLUDED.take_profit_2,
                    risk_reward_ratio = EXCLUDED.risk_reward_ratio,
                    time_horizon = EXCLUDED.time_horizon,
                    position_size_recommendation = EXCLUDED.position_size_recommendation,
                    created_at = CURRENT_TIMESTAMP
                """, (
                    result['symbol'],
                    datetime.now().date(),
                    result['current_price'],
                    json.dumps(result.get('elliott_pattern', {})),
                    result.get('signal', 'HOLD'),
                    result.get('signal_confidence', 0),
                    result.get('pattern_confidence', 0),
                    result.get('current_wave'),
                    result.get('next_expected'),
                    json.dumps(result.get('fibonacci_levels', {})),
                    result.get('entry_price'),
                    result.get('stop_loss'),
                    result.get('take_profit_1'),
                    result.get('take_profit_2'),
                    result.get('risk_reward_ratio'),
                    result.get('time_horizon'),
                    result.get('position_size')
                ))
                saved_count += 1
                
            except Exception as e:
                logger.error(f"Error saving Elliott analysis for {result.get('symbol', 'unknown')}: {str(e)}")
                continue
        
        conn.commit()
        cursor.close()
        conn.close()
        
        logger.info(f"Successfully saved {saved_count}/{len(analysis_results)} Elliott Wave analyses to database")
        
    except Exception as e:
        logger.error(f"Error saving Elliott Wave analysis to database: {str(e)}")

def send_elliott_wave_report():
    """
    Send comprehensive Elliott Wave analysis report via Telegram
    
    Returns:
    str: Status message
    """
    try:
        # Run screening untuk current opportunities
        logger.info("Running Elliott Wave screening for report")
        opportunities = elliott_wave_screening(max_symbols=30, min_data_days=80)
        
        if not opportunities:
            return "No Elliott Wave opportunities found for report"
        
        # Save analysis results to database
        save_elliott_analysis_to_db(opportunities)
        
        # Prepare comprehensive Telegram message
        message = "🌊 *ELLIOTT WAVE ANALYSIS REPORT* 🌊\n\n"
        message += f"📊 Analysis completed for {len(opportunities)} high-potential stocks\n\n"
        
        # Group opportunities by signal type
        strong_buy = [o for o in opportunities if o['signal'] in ['STRONG_BUY', 'BUY'] and o['signal_confidence'] >= 75]
        moderate_buy = [o for o in opportunities if o['signal'] == 'BUY' and o['signal_confidence'] >= 60 and o['signal_confidence'] < 75]
        sell_signals = [o for o in opportunities if o['signal'] in ['SELL', 'STRONG_SELL'] and o['signal_confidence'] >= 60]
        
        # Strong Buy Signals
        if strong_buy:
            message += "🚀 *STRONG BUY SIGNALS:*\n\n"
            for i, opp in enumerate(strong_buy[:5], 1):  # Top 5
                message += f"{i}. *{opp['symbol']}* - Wave {opp['current_wave']}\n"
                message += f"   💰 Price: Rp{opp['current_price']:,.0f}\n"
                message += f"   📈 Confidence: {opp['signal_confidence']}% | Pattern: {opp['pattern_confidence']}%\n"
                message += f"   🎯 Entry: Rp{opp['entry_price']:,.0f} | TP: Rp{opp['take_profit_1']:,.0f}\n" if opp['entry_price'] and opp['take_profit_1'] else ""
                message += f"   ⏰ Horizon: {opp['time_horizon']} | Size: {opp['position_size']}\n"
                message += f"   📝 Next: {opp['next_expected']}\n\n"
        
        # Moderate Buy Signals
        if moderate_buy:
            message += "📈 *MODERATE BUY SIGNALS:*\n\n"
            for i, opp in enumerate(moderate_buy[:3], 1):  # Top 3
                message += f"{i}. *{opp['symbol']}* - Wave {opp['current_wave']}\n"
                message += f"   Price: Rp{opp['current_price']:,.0f} | Confidence: {opp['signal_confidence']}%\n"
                message += f"   Next: {opp['next_expected']}\n\n"
        
        # Sell Signals
        if sell_signals:
            message += "📉 *SELL SIGNALS:*\n\n"
            for i, opp in enumerate(sell_signals[:3], 1):  # Top 3
                message += f"{i}. *{opp['symbol']}* - Wave {opp['current_wave']}\n"
                message += f"   Price: Rp{opp['current_price']:,.0f} | Confidence: {opp['signal_confidence']}%\n"
                message += f"   Next: {opp['next_expected']}\n\n"
        
        # Elliott Wave education
        message += "🌊 *ELLIOTT WAVE GUIDE:*\n\n"
        message += "*Wave Patterns:*\n"
        message += "• Waves 1-5: Impulse (main trend)\n"
        message += "• Waves A-C: Correction (counter-trend)\n\n"
        
        message += "*Best Entry Points:*\n"
        message += "• End of Wave 2: Strongest setup (Wave 3 coming)\n"
        message += "• End of Wave 4: Good setup (Wave 5 coming)\n"
        message += "• End of Wave C: New cycle beginning\n\n"
        
        message += "*Risk Management:*\n"
        message += "• Wave 2 entries: SL below 78.6% Fibonacci\n"
        message += "• Wave 4 entries: SL below 61.8% Fibonacci\n"
        message += "• Wave 5: Take profits, expect correction\n\n"
        
        message += "📱 *Trading Tips:*\n"
        message += "• Use Fibonacci levels for precise entries\n"
        message += "• Combine with volume confirmation\n"
        message += "• Watch for momentum divergence in Wave 5\n"
        message += "• Best results on trending markets\n\n"
        
        message += "*⚠️ Disclaimer:* Elliott Wave analysis is subjective and requires experience. "
        message += "Always combine with other technical analysis and proper risk management. "
        message += "Past patterns do not guarantee future results."
        
        # Send message via Telegram
        result = send_telegram_message(message)
        
        if "successfully" in result:
            logger.info(f"Elliott Wave report sent successfully: {len(opportunities)} opportunities")
            return f"Elliott Wave report sent: {len(opportunities)} opportunities ({len(strong_buy)} strong buy, {len(sell_signals)} sell)"
        else:
            logger.error(f"Failed to send Elliott Wave report: {result}")
            return f"Failed to send report: {result}"
            
    except Exception as e:
        logger.error(f"Error sending Elliott Wave report: {str(e)}")
        return f"Error sending report: {str(e)}"