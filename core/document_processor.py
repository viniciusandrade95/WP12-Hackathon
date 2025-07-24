# core/document_processor_fixed.py
"""
SIMPLIFIED & RELIABLE Document Processor
This version focuses on actually extracting metrics rather than complex verification
"""

import json
import time
import pdfplumber
import re
from pathlib import Path
from typing import Dict, List, Optional
from datetime import datetime

class DocumentProcessor:
    """
    Simplified, working document processor
    """
    
    def __init__(self, llm_client, db_manager):
        self.llm_client = llm_client
        self.db_manager = db_manager
        
        # Simple configuration
        self.MAX_PAGES_TO_PROCESS = 15
        self.MAX_PROCESSING_TIME = 600  # 10 minutes
        
    def process_document(self, pdf_path: str) -> Dict:
        """
        Main processing pipeline - SIMPLIFIED
        """
        document_id = None
        start_time = time.time()
        
        try:
            print(f"\n🚀 Starting Document Processing...")
            
            # Step 1: Basic document analysis
            with pdfplumber.open(pdf_path) as pdf:
                total_pages = len(pdf.pages)
                print(f"  📄 Document has {total_pages} pages")
                
                # Extract company name from first few pages
                company_name = self._extract_company_name_simple(pdf)
                print(f"  🏢 Company: {company_name}")
                
                # Simple industry detection
                industry = self._detect_industry_simple(pdf)
                print(f"  🏭 Industry: {industry}")
            
            # Step 2: Create database records
            document_id = self._create_simple_document_record(
                pdf_path, company_name, industry, total_pages
            )
            print(f"  💾 Created document record: {document_id}")
            
            # Step 3: Extract metrics using HYBRID approach
            print("🔍 Extracting metrics...")
            all_metrics = []
            
            pages_to_process = self._select_key_pages(pdf_path, total_pages)
            print(f"  📋 Processing {len(pages_to_process)} key pages")
            
            with pdfplumber.open(pdf_path) as pdf:
                for page_num in pages_to_process:
                    if page_num > len(pdf.pages):
                        continue
                        
                    page = pdf.pages[page_num - 1]
                    text = page.extract_text() or ""
                    
                    if len(text.strip()) < 50:
                        continue
                    
                    print(f"    📄 Processing page {page_num}...")
                    
                    # HYBRID EXTRACTION: Try LLM first, then regex fallback
                    page_metrics = self._extract_page_metrics_hybrid(
                        text, page_num, industry
                    )
                    
                    if page_metrics:
                        # Store immediately
                        self._store_metrics_simple(document_id, page_metrics)
                        all_metrics.extend(page_metrics)
                        print(f"      ✅ Found {len(page_metrics)} metrics")
                    else:
                        print(f"      ⚠️ No metrics found")
            
            # Step 4: Generate simple insights
            insights = self._generate_simple_insights(document_id, all_metrics)
            self._store_insights_simple(document_id, insights)
            
            # Step 5: Complete processing
            processing_time = time.time() - start_time
            self._complete_document_processing(
                document_id, len(all_metrics), len(pages_to_process), processing_time
            )
            
            print(f"✅ Processing completed in {processing_time:.1f}s")
            print(f"📊 Total metrics extracted: {len(all_metrics)}")
            
            return {
                'success': True,
                'document_id': document_id,
                'metrics_extracted': len(all_metrics),
                'pages_processed': len(pages_to_process),
                'processing_time': processing_time
            }
            
        except Exception as e:
            print(f"❌ Processing failed: {str(e)}")
            if document_id:
                self._mark_document_failed(document_id, str(e))
            
            return {
                'success': False,
                'error': str(e),
                'document_id': document_id
            }
    
    def _extract_company_name_simple(self, pdf) -> str:
        """Extract company name from first 3 pages"""
        text = ""
        for i in range(min(3, len(pdf.pages))):
            text += pdf.pages[i].extract_text() or ""
        
        # Simple patterns
        patterns = [
            r'([A-Z][A-Za-z\s&\.]{2,50}(?:GROUP|PLC|INC|CORP|LIMITED|LTD|HOLDINGS|SA|AG|NV))',
            r'ANNUAL\s+REPORT.*?(?:2024|2023|2022).*?([A-Z][A-Za-z\s&\.]{5,40})',
        ]
        
        for pattern in patterns:
            matches = re.findall(pattern, text, re.IGNORECASE)
            for match in matches:
                name = match.strip()
                if 5 <= len(name) <= 50 and name.count(' ') <= 4:
                    return name
        
        return "Unknown Company"
    
    def _detect_industry_simple(self, pdf) -> str:
        """Simple industry detection"""
        text = ""
        for i in range(min(10, len(pdf.pages))):
            text += (pdf.pages[i].extract_text() or "").lower()
        
        # Industry keywords
        if any(word in text for word in ['aircraft', 'passengers', 'flights', 'aviation', 'airline']):
            return 'airlines'
        elif any(word in text for word in ['deposits', 'loans', 'branches', 'bank', 'credit']):
            return 'banking'
        elif any(word in text for word in ['software', 'users', 'platform', 'digital', 'saas']):
            return 'technology'
        elif any(word in text for word in ['stores', 'retail', 'merchandise', 'customers']):
            return 'retail'
        else:
            return 'other'
    
    def _select_key_pages(self, pdf_path: str, total_pages: int) -> List[int]:
        """Select the most important pages for processing"""
        if total_pages <= self.MAX_PAGES_TO_PROCESS:
            return list(range(1, total_pages + 1))
        
        # Strategic selection
        key_pages = []
        
        # Always include first 5 pages (usually contain key metrics)
        key_pages.extend(range(1, min(6, total_pages + 1)))
        
        # Add middle section (often contains detailed financials)
        if total_pages > 10:
            middle_start = total_pages // 3
            middle_end = min(middle_start + 5, total_pages + 1)
            key_pages.extend(range(middle_start, middle_end))
        
        # Add a few later pages
        if total_pages > 15:
            late_start = max(total_pages - 3, middle_end if 'middle_end' in locals() else 10)
            key_pages.extend(range(late_start, total_pages + 1))
        
        # Remove duplicates and sort
        return sorted(list(set(key_pages)))[:self.MAX_PAGES_TO_PROCESS]
    
    def _extract_page_metrics_hybrid(self, text: str, page_num: int, industry: str) -> List[Dict]:
        """
        HYBRID extraction: LLM first, then regex fallback
        """
        # Try LLM extraction first
        llm_metrics = self._extract_with_llm(text, page_num, industry)
        if llm_metrics:
            print(f"      🤖 LLM extracted {len(llm_metrics)} metrics")
            return llm_metrics
        
        # Fallback to regex extraction
        print(f"      🔧 Falling back to regex extraction")
        regex_metrics = self._extract_with_regex(text, page_num)
        if regex_metrics:
            print(f"      📊 Regex extracted {len(regex_metrics)} metrics")
            return regex_metrics
        
        return []
    
    def _extract_with_llm(self, text: str, page_num: int, industry: str) -> List[Dict]:
        """LLM-based extraction with robust parsing"""
        try:
            prompt = """
            Extract ALL numerical financial metrics from this text.
            
            Look for:
            - Revenue, income, costs, profits (with € or $ amounts)
            - Employee counts, customer numbers
            - Any number with "million", "billion", "%"
            - Growth rates, ratios, percentages
            
            Return ONLY a JSON array like this:
            [
                {"name": "Total Revenue", "value": 13949, "unit": "million EUR", "period": "2025"},
                {"name": "Net Income", "value": 1612, "unit": "million EUR", "period": "2025"}
            ]
            
            CRITICAL: Return ONLY the JSON array, no other text.
            """
            
            # Call LLM
            response = self.llm_client.extract_metrics(text, page_num, prompt, 60, industry)
            
            if not response:
                return []
            
            # Convert to our format
            metrics = []
            for item in response:
                if isinstance(item, dict) and 'name' in item or 'metric' in item or 'metric_name' in item:
                    metric_name = (item.get('name') or 
                                 item.get('metric') or 
                                 item.get('metric_name', 'Unknown'))
                    
                    value = item.get('value', 0)
                    if isinstance(value, str):
                        # Clean string values
                        value = re.sub(r'[€$£,]', '', value)
                        try:
                            value = float(value)
                        except:
                            continue
                    
                    if isinstance(value, (int, float)) and value != 0:
                        metrics.append({
                            'metric': metric_name,
                            'value': float(value),
                            'unit': item.get('unit', 'unknown'),
                            'period': item.get('period', '2024'),
                            'confidence': 0.85,
                            'page_number': page_num,
                            'extraction_method': 'llm',
                            'source_text': item.get('source_text', '')
                        })
            
            return metrics
            
        except Exception as e:
            print(f"        ❌ LLM extraction failed: {e}")
            return []
    
    def _extract_with_regex(self, text: str, page_num: int) -> List[Dict]:
        """FIXED regex extraction for your specific data format"""
        metrics = []
        
        print(f"        🔍 Analyzing text (length: {len(text)} chars)")
        
        # Pattern 1: Financial line items with multiple years
        # Matches: "Total Revenue         13,949     13,444     10,775"
        pattern1 = r'([A-Za-z\s&-]+?)\s+([\d,]+)\s+([\d,]+)\s+([\d,]+)'
        matches1 = re.findall(pattern1, text)
        
        print(f"        📊 Pattern 1 found {len(matches1)} matches")
        
        for match in matches1:
            try:
                metric_name = match[0].strip()
                # Take the first (most recent) value
                value_str = match[1].replace(',', '')
                value = float(value_str)
                
                # Skip if not a meaningful metric name
                if len(metric_name) < 3 or metric_name.isdigit():
                    continue
                    
                # Skip header rows
                if any(word in metric_name.lower() for word in ['mar', 'income statement', 'balance sheet', "€'m"]):
                    continue
                
                metrics.append({
                    'metric': metric_name,
                    'value': value,
                    'unit': 'million EUR',
                    'period': '2025',
                    'confidence': 0.85,
                    'page_number': page_num,
                    'extraction_method': 'regex_fixed',
                    'source_text': f"{metric_name}: {match[1]}"
                })
                
                print(f"          ✅ Found: {metric_name} = {value}")
                
            except Exception as e:
                print(f"          ❌ Error processing match: {e}")
                continue
        
        # Pattern 2: Look for specific metrics by name (targeted approach)
        target_metrics = [
            'Total Revenue', 'Scheduled Revenue', 'Ancillary Revenue',
            'Profit After Tax', 'Profit Before Tax', 'Net Income',
            'Total Assets', 'Current Assets', 'Non-Current Assets',
            'Shareholder Equity', 'Total Liabilities', 'Net Cash',
            'Gross Cash', 'Current Liabilities', 'Non-Current Liabilities'
        ]
        
        for target in target_metrics:
            # Look for the metric name followed by numbers
            pattern = rf'{re.escape(target)}\s*[:\s]*(\d{{1,2}},?\d{{3,4}})'
            matches = re.findall(pattern, text, re.IGNORECASE)
            
            if matches:
                try:
                    value_str = matches[0].replace(',', '')
                    value = float(value_str)
                    
                    # Check if we already have this metric
                    if not any(target.lower() in m['metric'].lower() for m in metrics):
                        metrics.append({
                            'metric': target,
                            'value': value,
                            'unit': 'million EUR',
                            'period': '2025',
                            'confidence': 0.90,
                            'page_number': page_num,
                            'extraction_method': 'regex_targeted',
                            'source_text': f"{target}: {matches[0]}"
                        })
                        
                        print(f"          🎯 Targeted find: {target} = {value}")
                        
                except Exception as e:
                    continue
        
        # Pattern 3: Simple number extraction for remaining cases
        lines = text.split('\n')
        for line in lines:
            line = line.strip()
            if not line or len(line) < 10:
                continue
                
            # Match: text followed by a number
            match = re.match(r'([A-Za-z\s&-]+?)\s+([\d,]{3,6})(?:\s|$)', line)
            if match:
                try:
                    metric_name = match.group(1).strip()
                    value_str = match.group(2).replace(',', '')
                    value = float(value_str)
                    
                    # Skip if already found, too short, or invalid
                    if (len(metric_name) < 5 or 
                        any(m['metric'].lower() == metric_name.lower() for m in metrics) or
                        any(skip in metric_name.lower() for skip in ['mar', "€'m", 'income', 'balance'])):
                        continue
                    
                    # Only include if value is reasonable (not year like 2025)
                    if 100 <= value <= 50000:  # Reasonable range for financial metrics
                        metrics.append({
                            'metric': metric_name,
                            'value': value,
                            'unit': 'million EUR',
                            'period': '2025',
                            'confidence': 0.75,
                            'page_number': page_num,
                            'extraction_method': 'regex_line',
                            'source_text': line
                        })
                        
                        print(f"          📋 Line match: {metric_name} = {value}")
                        
                except Exception as e:
                    continue
        
        print(f"        📊 Total metrics extracted: {len(metrics)}")
        return metrics
    
    def _create_simple_document_record(self, pdf_path: str, company_name: str, 
                                     industry: str, total_pages: int) -> int:
        """Create database records"""
        cursor = self.db_manager.connection.cursor()
        
        try:
            # Create or get company
            cursor.execute("SELECT id FROM companies WHERE name = ?", (company_name,))
            company_row = cursor.fetchone()
            
            if company_row:
                company_id = company_row[0]
            else:
                cursor.execute("""
                    INSERT INTO companies (name, detected_industry, industry_confidence)
                    VALUES (?, ?, ?)
                """, (company_name, industry, 0.8))
                company_id = cursor.lastrowid
            
            # Create document record
            filename = Path(pdf_path).name
            cursor.execute("""
                INSERT INTO documents 
                (company_id, filename, file_path, total_pages, status)
                VALUES (?, ?, ?, ?, ?)
            """, (company_id, filename, pdf_path, total_pages, 'processing'))
            
            document_id = cursor.lastrowid
            self.db_manager.connection.commit()
            
            return document_id
            
        except Exception as e:
            self.db_manager.connection.rollback()
            raise Exception(f"Database error: {str(e)}")
    
    def _store_metrics_simple(self, document_id: int, metrics: List[Dict]):
        """Store metrics in database"""
        if not metrics:
            return
        
        cursor = self.db_manager.connection.cursor()
        
        for metric in metrics:
            cursor.execute("""
                INSERT INTO financial_metrics 
                (document_id, page_number, metric_name, value, unit, 
                 period, confidence, extraction_method, source_text)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                document_id,
                metric['page_number'],
                metric['metric'],
                metric['value'],
                metric['unit'],
                metric['period'],
                metric['confidence'],
                metric['extraction_method'],
                metric.get('source_text', '')
            ))
        
        self.db_manager.connection.commit()
    
    def _generate_simple_insights(self, document_id: int, metrics: List[Dict]) -> List[Dict]:
        """Generate basic business insights"""
        insights = []
        
        if not metrics:
            return insights
        
        # Revenue insight
        revenue_metrics = [m for m in metrics if 'revenue' in m['metric'].lower()]
        if revenue_metrics:
            revenue = revenue_metrics[0]['value']
            insight = f"Revenue of {revenue:,.0f} {revenue_metrics[0]['unit']} indicates {'strong' if revenue > 1000 else 'moderate'} business scale"
            insights.append({
                'concept': 'Financial Performance',
                'insight': insight,
                'confidence': 0.80
            })
        
        # Profitability insight
        profit_metrics = [m for m in metrics if any(word in m['metric'].lower() for word in ['profit', 'income', 'earnings'])]
        if profit_metrics:
            profit = profit_metrics[0]['value']
            insight = f"Profit of {profit:,.0f} {profit_metrics[0]['unit']} shows {'healthy' if profit > 0 else 'concerning'} profitability"
            insights.append({
                'concept': 'Profitability',
                'insight': insight,
                'confidence': 0.75
            })
        
        return insights
    
    def _store_insights_simple(self, document_id: int, insights: List[Dict]):
        """Store insights in database"""
        if not insights:
            return
        
        cursor = self.db_manager.connection.cursor()
        
        for insight in insights:
            cursor.execute("""
                INSERT INTO business_intelligence 
                (document_id, concept, insight_text, confidence)
                VALUES (?, ?, ?, ?)
            """, (
                document_id,
                insight['concept'],
                insight['insight'],
                insight['confidence']
            ))
        
        self.db_manager.connection.commit()
    
    def _complete_document_processing(self, document_id: int, metrics_count: int, 
                                    pages_processed: int, processing_time: float):
        """Mark document as completed"""
        cursor = self.db_manager.connection.cursor()
        cursor.execute("""
            UPDATE documents 
            SET status = ?, pages_processed = ?, processing_time = ?, completed_at = ?
            WHERE id = ?
        """, ('completed', pages_processed, processing_time, datetime.now(), document_id))
        
        self.db_manager.connection.commit()
    
    def _mark_document_failed(self, document_id: int, error: str):
        """Mark document as failed"""
        cursor = self.db_manager.connection.cursor()
        cursor.execute("""
            UPDATE documents 
            SET status = ?, completed_at = ?
            WHERE id = ?
        """, ('failed', datetime.now(), document_id))
        
        self.db_manager.connection.commit()
    
    def get_company_intelligence(self, document_id: int) -> Dict:
        """Get company intelligence - simplified version"""
        cursor = self.db_manager.connection.cursor()
        
        try:
            # Get document info
            cursor.execute("""
                SELECT c.name, c.detected_industry, d.filename, d.total_pages, 
                       d.pages_processed, d.processing_time, d.status
                FROM companies c
                JOIN documents d ON c.id = d.company_id
                WHERE d.id = ?
            """, (document_id,))
            
            doc_info = cursor.fetchone()
            if not doc_info:
                return {'error': 'Document not found'}
            
            # Get all metrics with page information
            cursor.execute("""
                SELECT metric_name, value, unit, period, confidence, extraction_method, 
                       page_number, source_text
                FROM financial_metrics
                WHERE document_id = ?
                ORDER BY confidence DESC, page_number ASC
            """, (document_id,))
            
            metrics_data = cursor.fetchall()
            
            # Get insights
            cursor.execute("""
                SELECT concept, insight_text, confidence
                FROM business_intelligence
                WHERE document_id = ?
            """, (document_id,))
            
            insights_data = cursor.fetchall()
            
            # Organize metrics by type
            financial_metrics = {}
            operational_metrics = {}
            
            for row in metrics_data:
                (metric_name, value, unit, period, confidence, 
                 method, page_number, source_text) = row
                
                metric_info = {
                    'value': value,
                    'unit': unit,
                    'period': period,
                    'confidence': confidence,
                    'method': method,
                    'extraction_method': method,  # For template compatibility
                    'page_number': page_number,
                    'source_text': source_text
                }
                
                # Simple categorization
                if any(word in metric_name.lower() for word in ['revenue', 'income', 'profit', 'cost', 'cash', 'asset', 'liability', 'equity']):
                    financial_metrics[metric_name] = metric_info
                else:
                    operational_metrics[metric_name] = metric_info
            
            # Format insights
            insights = []
            for row in insights_data:
                insights.append({
                    'concept': row[0],
                    'insight': row[1],
                    'confidence': row[2]
                })
            
            return {
                'company_profile': {
                    'name': doc_info[0],
                    'detected_industry': doc_info[1],
                    'filename': doc_info[2],
                    'total_pages': doc_info[3],
                    'pages_processed': doc_info[4],
                    'processing_time': doc_info[5],
                    'status': doc_info[6]
                },
                'financial_metrics': financial_metrics,
                'operational_metrics': operational_metrics,
                'business_intelligence': insights,
                'summary': {
                    'total_metrics': len(metrics_data),
                    'financial_metrics_count': len(financial_metrics),
                    'operational_metrics_count': len(operational_metrics),
                    'insights_count': len(insights)
                }
            }
            
        except Exception as e:
            print(f"Error getting intelligence: {e}")
            return {'error': f'Failed to get intelligence: {str(e)}'}
    
    def get_processing_progress(self, document_id: int) -> Dict:
        """Get processing progress"""
        cursor = self.db_manager.connection.cursor()
        
        try:
            cursor.execute("""
                SELECT status, pages_processed, total_pages
                FROM documents
                WHERE id = ?
            """, (document_id,))
            
            result = cursor.fetchone()
            if not result:
                return {'error': 'Document not found'}
            
            status, pages_processed, total_pages = result
            
            if status == 'completed':
                progress = 100
            elif status == 'failed':
                progress = 0
            elif pages_processed and total_pages:
                progress = min(int((pages_processed / total_pages) * 100), 95)
            else:
                progress = 10
            
            return {
                'status': status,
                'progress': progress,
                'pages_processed': pages_processed or 0,
                'total_pages': total_pages or 0
            }
            
        except Exception as e:
            return {'error': f'Progress check failed: {str(e)}'}
    
    def get_recent_analyses(self, limit: int = 6) -> List[Dict]:
        """Get recent analyses"""
        cursor = self.db_manager.connection.cursor()
        
        try:
            cursor.execute("""
                SELECT d.id, c.name, c.detected_industry, d.created_at, 
                       COUNT(fm.id) as metric_count
                FROM documents d
                JOIN companies c ON d.company_id = c.id
                LEFT JOIN financial_metrics fm ON d.id = fm.document_id
                WHERE d.status = 'completed'
                GROUP BY d.id
                ORDER BY d.created_at DESC
                LIMIT ?
            """, (limit,))
            
            results = cursor.fetchall()
            
            return [{
                'id': row[0],
                'company': row[1],
                'industry': row[2] or 'Other',
                'date': row[3],
                'metrics': row[4]
            } for row in results]
            
        except Exception as e:
            print(f"Error getting recent analyses: {e}")
            return []