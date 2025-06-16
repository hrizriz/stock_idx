# ============================================================================
# PDF TRADE CONFIRMATION PARSER
# File: portofolio_tracker/utils/pdf_parser.py  
# ============================================================================

import re
from datetime import datetime
import streamlit as st

try:
    import PyPDF2
    PDF_AVAILABLE = True
except ImportError:
    PDF_AVAILABLE = False
    st.warning("⚠️ PyPDF2 not installed. PDF parsing will be limited.")

class TradeConfirmationParser:
    def __init__(self):
        # Stockbit trade confirmation patterns
        self.stockbit_patterns = {
            'trade_date': r'Transaction Date\s+(\d{2}/\d{2}/\d{4})',
            'settlement_date': r'Settlement Date\s+(\d{2}/\d{2}/\d{4})',
            'ref_number': r'REF #\s+(\d+)',
            'transaction_line': r'(\d+)\s+RG\s+([A-Z]{4})\s+(.+?)\s+(\d+)\s+([\d,]+\.?\d*)\s+([\d,]+\.?\d*)\s+([\d,]+\.?\d*)\s+([\d,]+\.?\d*)'
        }
    
    def parse_stockbit_pdf(self, pdf_path):
        """Parse Stockbit trade confirmation PDF"""
        if not PDF_AVAILABLE:
            st.error("❌ PDF parsing library not available. Please install PyPDF2.")
            return []
        
        try:
            with open(pdf_path, 'rb') as file:
                pdf_reader = PyPDF2.PdfReader(file)
                text = ""
                
                for page in pdf_reader.pages:
                    text += page.extract_text()
            
            transactions = self._extract_stockbit_transactions(text, pdf_path)
            return transactions
        
        except Exception as e:
            st.error(f"Error parsing PDF: {e}")
            return []
    
    def _extract_stockbit_transactions(self, text, source_file):
        """Extract transactions from Stockbit PDF text"""
        transactions = []
        
        try:
            # Extract dates
            trade_date = self._extract_trade_date(text)
            settlement_date = self._extract_settlement_date(text)
            
            # Extract transaction lines
            lines = text.split('\n')
            
            for line in lines:
                transaction = self._parse_transaction_line(line, trade_date, settlement_date, source_file)
                if transaction:
                    transactions.append(transaction)
            
            # Remove duplicates and clean data
            transactions = self._clean_transactions(transactions)
            
            return transactions
        
        except Exception as e:
            st.error(f"Error extracting transactions: {e}")
            return []
    
    def _extract_trade_date(self, text):
        """Extract trade date from PDF text"""
        match = re.search(self.stockbit_patterns['trade_date'], text)
        if match:
            try:
                return datetime.strptime(match.group(1), '%d/%m/%Y').date()
            except ValueError:
                pass
        return datetime.now().date()
    
    def _extract_settlement_date(self, text):
        """Extract settlement date from PDF text"""
        match = re.search(self.stockbit_patterns['settlement_date'], text)
        if match:
            try:
                return datetime.strptime(match.group(1), '%d/%m/%Y').date()
            except ValueError:
                pass
        return None
    
    def _parse_transaction_line(self, line, trade_date, settlement_date, source_file):
        """Parse individual transaction line"""
        # Known stock symbols to help with parsing
        known_symbols = [
            'AGRS', 'AHAP', 'AREA', 'AWAN', 'BMHS', 'GOLF', 'HILL', 'IMJS', 
            'INPC', 'KOKA', 'MLPL', 'PANS', 'SMKL', 'SOLA', 'SURE', 'TBLA', 
            'TCID', 'TOTO', 'BBCA', 'BMRI', 'BBRI', 'TLKM', 'UNVR', 'KLBF',
            'ASII', 'ICBP', 'INTP', 'JPFA', 'KAEF', 'LPPF', 'MNCN', 'PTBA',
            'SMGR', 'TINS', 'UNTR', 'WIKA', 'WSKT', 'ADRO', 'ANTM', 'BUMI'
        ]
        
        # Look for transaction pattern
        if not ('RG' in line and any(symbol in line for symbol in known_symbols)):
            return None
        
        try:
            parts = line.split()
            
            # Find reference number (first number)
            ref_number = None
            for part in parts:
                if part.isdigit() and len(part) >= 6:
                    ref_number = part
                    break
            
            # Find stock symbol
            symbol = None
            symbol_index = -1
            for i, part in enumerate(parts):
                if part in known_symbols:
                    symbol = part
                    symbol_index = i
                    break
            
            if not symbol:
                return None
            
            # Extract company name (follows symbol until numbers start)
            company_parts = []
            for i in range(symbol_index + 1, len(parts)):
                part = parts[i]
                # Stop when we hit a number
                if self._is_numeric(part):
                    break
                company_parts.append(part)
            
            company_name = ' '.join(company_parts).strip()
            
            # Extract numeric values
            numeric_values = []
            for part in parts:
                if self._is_numeric(part):
                    try:
                        value = float(part.replace(',', ''))
                        numeric_values.append(value)
                    except ValueError:
                        continue
            
            if len(numeric_values) < 4:
                return None
            
            # Parse values based on position
            # Typical format: REF# RG SYMBOL COMPANY LOT QUANTITY PRICE BUY_VALUE SELL_VALUE
            lot = int(numeric_values[0]) if len(numeric_values) > 0 else 0
            quantity = int(numeric_values[1]) if len(numeric_values) > 1 else 0
            price = numeric_values[2] if len(numeric_values) > 2 else 0
            buy_value = numeric_values[3] if len(numeric_values) > 3 else 0
            sell_value = numeric_values[4] if len(numeric_values) > 4 else 0
            
            # Determine transaction type and value
            if buy_value > 0:
                transaction_type = 'BUY'
                total_value = buy_value
            elif sell_value > 0:
                transaction_type = 'SELL'
                total_value = sell_value
            else:
                return None
            
            # Validate transaction
            if quantity <= 0 or price <= 0 or total_value <= 0:
                return None
            
            transaction = {
                'trade_date': trade_date,
                'settlement_date': settlement_date,
                'symbol': symbol,
                'company_name': company_name,
                'transaction_type': transaction_type,
                'quantity': quantity,
                'price': price,
                'total_value': total_value,
                'fees': 0,  # Will be calculated separately from PDF fees section
                'ref_number': ref_number,
                'source_file': source_file
            }
            
            return transaction
        
        except Exception as e:
            # Silently skip malformed lines
            return None
    
    def _is_numeric(self, text):
        """Check if text represents a number"""
        try:
            float(text.replace(',', ''))
            return True
        except ValueError:
            return False
    
    def _clean_transactions(self, transactions):
        """Remove duplicates and clean transaction data"""
        cleaned = []
        seen = set()
        
        for transaction in transactions:
            # Create unique key for deduplication
            key = (
                transaction['symbol'],
                transaction['transaction_type'],
                transaction['quantity'],
                transaction['price'],
                transaction['trade_date']
            )
            
            if key not in seen:
                # Clean company name
                company_name = transaction['company_name']
                company_name = re.sub(r'\s+', ' ', company_name)  # Multiple spaces to single
                company_name = company_name.replace('Tbk.', 'Tbk').strip()
                transaction['company_name'] = company_name
                
                # Ensure proper data types
                transaction['quantity'] = int(transaction['quantity'])
                transaction['price'] = float(transaction['price'])
                transaction['total_value'] = float(transaction['total_value'])
                transaction['fees'] = float(transaction.get('fees', 0))
                
                cleaned.append(transaction)
                seen.add(key)
        
        return cleaned
    
    def parse_manual_input(self, data):
        """Parse manually entered transaction data"""
        try:
            transaction = {
                'trade_date': data['trade_date'],
                'settlement_date': data.get('settlement_date'),
                'symbol': data['symbol'].upper().strip(),
                'company_name': data['company_name'].strip(),
                'transaction_type': data['transaction_type'].upper(),
                'quantity': int(data['quantity']),
                'price': float(data['price']),
                'total_value': float(data['quantity']) * float(data['price']),
                'fees': float(data.get('fees', 0)),
                'ref_number': data.get('ref_number', '').strip(),
                'source_file': 'MANUAL_ENTRY'
            }
            
            return transaction
        
        except Exception as e:
            st.error(f"Error parsing manual input: {e}")
            return None
    
    def validate_transaction(self, transaction):
        """Validate transaction data"""
        required_fields = ['symbol', 'company_name', 'transaction_type', 'quantity', 'price', 'total_value']
        
        for field in required_fields:
            if field not in transaction or not transaction[field]:
                return False, f"Missing required field: {field}"
        
        # Validate transaction type
        if transaction['transaction_type'] not in ['BUY', 'SELL']:
            return False, "Transaction type must be BUY or SELL"
        
        # Validate numeric fields
        try:
            quantity = float(transaction['quantity'])
            price = float(transaction['price'])
            total_value = float(transaction['total_value'])
            
            if quantity <= 0:
                return False, "Quantity must be positive"
            
            if price <= 0:
                return False, "Price must be positive"
            
            if total_value <= 0:
                return False, "Total value must be positive"
            
            # Check if total_value matches quantity * price (with small tolerance)
            expected_value = quantity * price
            if abs(total_value - expected_value) > 1:  # Allow 1 rupiah difference for rounding
                return False, f"Total value ({total_value:,.0f}) doesn't match quantity × price ({expected_value:,.0f})"
        
        except (ValueError, TypeError):
            return False, "Invalid numeric values"
        
        # Validate symbol format (4 letters)
        symbol = transaction['symbol'].strip().upper()
        if not re.match(r'^[A-Z]{4}$', symbol):
            return False, "Symbol must be 4 uppercase letters"
        
        return True, "Valid transaction"
    
    def extract_fees_from_pdf(self, text):
        """Extract fee information from PDF text"""
        fees = {
            'commission': 0,
            'vat': 0,
            'idx_fee': 0,
            'income_tax': 0,
            'stamp_duty': 0,
            'total_cost': 0
        }
        
        try:
            # Look for fee patterns in PDF
            fee_patterns = {
                'commission': r'Commission\s+([\d,]+\.?\d*)',
                'vat': r'V\.A\.T\s+([\d,]+\.?\d*)',
                'idx_fee': r'IDX Fee\s+([\d,]+\.?\d*)',
                'income_tax': r'Income Tax\s+([\d,]+\.?\d*)',
                'stamp_duty': r'Stamp Duty\s+([\d,]+\.?\d*)',
                'total_cost': r'Total Cost\s+([\d,]+\.?\d*)'
            }
            
            for fee_type, pattern in fee_patterns.items():
                match = re.search(pattern, text)
                if match:
                    try:
                        fees[fee_type] = float(match.group(1).replace(',', ''))
                    except ValueError:
                        pass
            
            return fees
        
        except Exception as e:
            st.warning(f"Could not extract fees: {e}")
            return fees