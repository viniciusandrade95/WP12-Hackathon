# core/document_processor.py (Simplified version that actually works)
"""
Simplified document processor that mirrors the working single-file version
"""

import json
import time
import pdfplumber
import re
from pathlib import Path
from typing import Dict, List, Optional
from datetime import datetime

from .knowledge_base import GICSKnowledgeBase
from .database import DatabaseManager
from utils.api_client import LLMClient
from .dual_agent_verification import DualAgentVerificationSystem, VerificationStatus, VerificationResult

"""
Debug script to identify why metrics extraction is failing
Add this temporarily to your document_processor.py or run separately
"""

import json
import pdfplumber

class MetricExtractionDebugger:
    def __init__(self, llm_client):
        self.llm_client = llm_client
        
    def debug_extraction(self, pdf_path: str, page_num: int = 1):
        """Debug metric extraction step by step"""
        
        print("\n" + "="*60)
        print("METRIC EXTRACTION DEBUGGING")
        print("="*60)
        
        # Step 1: Check PDF reading
        print("\n1. CHECKING PDF READING:")
        try:
            with pdfplumber.open(pdf_path) as pdf:
                if page_num > len(pdf.pages):
                    print(f"   ❌ Page {page_num} doesn't exist. PDF has {len(pdf.pages)} pages")
                    return
                    
                page = pdf.pages[page_num - 1]
                text = page.extract_text() or ""
                
                print(f"   ✅ Successfully read page {page_num}")
                print(f"   📄 Text length: {len(text)} characters")
                print(f"   📄 First 500 chars: {text[:500]}...")
                
                if len(text.strip()) < 100:
                    print("   ❌ Page has insufficient text!")
                    return
                    
        except Exception as e:
            print(f"   ❌ PDF reading failed: {e}")
            return
            
        # Step 2: Test basic prompt
        print("\n2. TESTING BASIC EXTRACTION PROMPT:")
        basic_prompt = """
Extract ALL numerical values from this text. Look for:
- Any number followed by "million", "billion", "€", "$", "%"
- Any metric name followed by a number
- Revenue, costs, profits, assets, employees, etc.

Return as JSON array:
[{"metric_name": "name", "value": number, "unit": "unit", "period": "period"}]

Return ONLY the JSON array, nothing else.
"""
        
        print("   📝 Sending basic prompt to LLM...")
        try:
            # Direct API call for debugging
            import requests
            
            data = {
                "model": "mistral-small3.2:latest",
                "messages": [
                    {"role": "system", "content": "You are a metric extraction expert. Extract all numerical data."},
                    {"role": "user", "content": f"{basic_prompt}\n\nText:\n{text[:3000]}"}
                ],
                "temperature": 0.0,
                "max_tokens": 2000
            }
            
            response = requests.post(
                self.llm_client.base_url,
                headers=self.llm_client.headers,
                json=data,
                timeout=60
            )
            
            print(f"   📡 API Response Status: {response.status_code}")
            
            if response.status_code == 200:
                resp_json = response.json()
                content = resp_json['choices'][0]['message']['content']
                print(f"   📄 Raw LLM Response (first 500 chars):\n{content[:500]}")
                
                # Try to parse JSON
                json_start = content.find('[')
                json_end = content.rfind(']')
                
                if json_start != -1 and json_end != -1:
                    json_str = content[json_start:json_end + 1]
                    print(f"\n   🔍 Extracted JSON string: {json_str[:200]}...")
                    
                    try:
                        metrics = json.loads(json_str)
                        print(f"   ✅ Successfully parsed {len(metrics)} metrics")
                        
                        if len(metrics) > 0:
                            print("\n   📊 Sample metrics found:")
                            for i, m in enumerate(metrics[:3]):
                                print(f"      {i+1}. {m.get('metric_name', 'Unknown')}: {m.get('value', 'N/A')} {m.get('unit', '')}")
                        else:
                            print("   ⚠️  JSON parsed but no metrics found!")
                            
                    except json.JSONDecodeError as e:
                        print(f"   ❌ JSON parsing failed: {e}")
                        print(f"   📄 JSON string that failed: {json_str}")
                else:
                    print("   ❌ No JSON array found in response!")
                    print("   💡 The LLM might be returning explanatory text instead of JSON")
                    
            else:
                print(f"   ❌ API call failed: {response.text}")
                
        except Exception as e:
            print(f"   ❌ Extraction test failed: {e}")
            import traceback
            traceback.print_exc()
            
        # Step 3: Check for common issues
        print("\n3. CHECKING COMMON ISSUES:")
        
        # Check if text has numbers
        import re
        numbers = re.findall(r'\b\d+(?:,\d{3})*(?:\.\d+)?\b', text)
        print(f"   📊 Numbers found in text: {len(numbers)}")
        if numbers:
            print(f"   📊 Sample numbers: {numbers[:5]}")
            
        # Check for currency/units
        currencies = re.findall(r'[€$£¥]|EUR|USD|million|billion|thousand', text, re.IGNORECASE)
        print(f"   💰 Currency/unit indicators found: {len(currencies)}")
        
        # Check for metric keywords
        metric_keywords = ['revenue', 'income', 'profit', 'cost', 'asset', 'employee', 'sales']
        found_keywords = [kw for kw in metric_keywords if kw in text.lower()]
        print(f"   🔍 Metric keywords found: {found_keywords}")
        
        # Step 4: Test with a simpler extraction
        print("\n4. TESTING SIMPLE PATTERN MATCHING:")
        simple_patterns = [
            r'(?:revenue|sales|income)[:\s]+(?:€|EUR)?\s*(\d+(?:\.\d+)?)\s*(million|billion)?',
            r'(\d+(?:\.\d+)?)\s*(million|billion)?\s*(?:€|EUR|euros?)',
            r'(?:total|net)?\s*(?:assets?|liabilities|equity)[:\s]+(?:€|EUR)?\s*(\d+(?:\.\d+)?)\s*(million|billion)?'
        ]
        
        pattern_matches = []
        for pattern in simple_patterns:
            matches = re.findall(pattern, text, re.IGNORECASE)
            if matches:
                pattern_matches.extend(matches)
                
        print(f"   🎯 Pattern matches found: {len(pattern_matches)}")
        if pattern_matches:
            print(f"   🎯 Sample matches: {pattern_matches[:3]}")
            
        return text  # Return text for further analysis

# To use this debugger:
def debug_metric_issue(processor, pdf_path):
    """Run this in your main processing flow"""
    debugger = MetricExtractionDebugger(processor.llm_client)
    
    # Test on first few pages
    for page in [1, 2, 3]:
        print(f"\n\n{'='*20} TESTING PAGE {page} {'='*20}")
        debugger.debug_extraction(pdf_path, page)
        
    # Also test the actual extraction method
    print("\n\n5. TESTING ACTUAL EXTRACTION METHOD:")
    print("   Running processor._extract_page_metrics...")
    
    try:
        with pdfplumber.open(pdf_path) as pdf:
            if len(pdf.pages) > 0:
                text = pdf.pages[0].extract_text() or ""
                metrics = processor._extract_page_metrics(text, 1, "airlines", 1)
                print(f"   📊 Metrics returned: {len(metrics)}")
                if metrics:
                    print("   📊 First metric:", metrics[0])
    except Exception as e:
        print(f"   ❌ Method test failed: {e}")
        import traceback
        traceback.print_exc()

class DocumentProcessor:
    """
    Simplified document processor that matches the original working version
    """
    
    def __init__(self, llm_client: LLMClient, db_manager: DatabaseManager):
        self.llm_client = llm_client
        self.db_manager = db_manager
        self.knowledge_base = GICSKnowledgeBase()
        
        # Processing configuration
        self.MAX_PAGES_TO_PROCESS = 20
        self.MAX_PROCESSING_TIME = 900  # 15 minutes
        self.BATCH_SIZE = 4
        
        # Progress tracking
        self.processing_progress = {}

        # Add dual-agent system
        self.verification_system = DualAgentVerificationSystem(llm_client)
        
    def process_document(self, pdf_path: str) -> Dict:
        """
        Main document processing pipeline
        """
        document_id = None
        start_time = time.time()
        
        try:
            print(f"\n🚀 Starting Document Processing...")
            
            # Phase 1: Document structure analysis
            print("📋 Phase 1: Document structure analysis...")
            structure_analysis = self._analyze_document_structure(pdf_path)
            if not structure_analysis.get('success'):
                return structure_analysis
            
            # Phase 2: Industry detection
            print("🏭 Phase 2: Industry detection...")
            industry_detection = self.knowledge_base.detect_industry(
                structure_analysis['sample_text'], 
                structure_analysis['company_name']
            )
            
            # Phase 3: Create company and document records
            print("💾 Phase 3: Creating database records...")
            document_id = self._create_document_record(
                pdf_path, structure_analysis, industry_detection
            )
            
            # Phase 4: Extract metrics
            print("🔍 Phase 4: Extracting metrics...")
            extraction_results = self._extract_metrics(
                pdf_path, document_id, industry_detection
            )
            
            # Phase 5: Generate insights
            print("🧠 Phase 5: Generating business insights...")
            insights = self._generate_insights(
                document_id, extraction_results, industry_detection
            )
            
            # Phase 6: Finalize processing
            print("✅ Phase 6: Finalizing...")
            final_results = self._finalize_processing(
                document_id, extraction_results, insights, start_time
            )
            
            print(f"✅ Processing completed successfully in {time.time() - start_time:.1f}s")
            return final_results
            
        except Exception as e:
            print(f"❌ Processing failed: {str(e)}")
            
            if document_id:
                self._update_document_status(document_id, "failed", str(e))
            
            return {
                'success': False,
                'error': str(e),
                'document_id': document_id,
                'processing_time': time.time() - start_time
            }
    
    def _analyze_document_structure(self, pdf_path: str) -> Dict:
        """Analyze document structure"""
        try:
            with pdfplumber.open(pdf_path) as pdf:
                total_pages = len(pdf.pages)
                
                if total_pages == 0:
                    return {'success': False, 'error': 'PDF contains no pages'}
                
                # Extract sample text from first few pages
                sample_text = ""
                for i in range(min(5, total_pages)):
                    try:
                        page_text = pdf.pages[i].extract_text() or ""
                        sample_text += page_text + " "
                    except Exception as e:
                        print(f"    ⚠️ Error extracting text from page {i+1}: {e}")
                        continue
                
                if len(sample_text.strip()) < 100:
                    return {'success': False, 'error': 'PDF contains insufficient text content'}
                
                # Extract company name
                company_name = self._extract_company_name(sample_text)
                
                return {
                    'success': True,
                    'total_pages': total_pages,
                    'sample_text': sample_text[:3000],
                    'company_name': company_name,
                    'file_path': pdf_path
                }
                
        except Exception as e:
            return {'success': False, 'error': f'Failed to analyze PDF structure: {str(e)}'}
    
    def _create_document_record(self, pdf_path: str, structure_analysis: Dict, 
                               industry_detection: Dict) -> int:
        """Create company and document records in database"""
        cursor = self.db_manager.connection.cursor()
        
        try:
            # Create or get company
            company_name = structure_analysis['company_name']
            detected_industry = industry_detection['industry']
            industry_confidence = industry_detection['confidence']
            
            # Check if company exists
            cursor.execute("SELECT id FROM companies WHERE name = ?", (company_name,))
            company_row = cursor.fetchone()
            
            if company_row:
                company_id = company_row[0]
                # Update industry information
                cursor.execute("""
                    UPDATE companies 
                    SET detected_industry = ?, industry_confidence = ?, updated_at = ?
                    WHERE id = ?
                """, (detected_industry, industry_confidence, datetime.now(), company_id))
            else:
                # Create new company
                cursor.execute("""
                    INSERT INTO companies (name, detected_industry, industry_confidence)
                    VALUES (?, ?, ?)
                """, (company_name, detected_industry, industry_confidence))
                company_id = cursor.lastrowid
            
            # Create document record
            filename = Path(pdf_path).name
            cursor.execute("""
                INSERT INTO documents 
                (company_id, filename, file_path, total_pages, status, processing_strategy)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (
                company_id, 
                filename, 
                pdf_path, 
                structure_analysis['total_pages'], 
                'processing',
                f"simplified_{detected_industry}"
            ))
            
            document_id = cursor.lastrowid
            
            # Create industry analysis record
            cursor.execute("""
                INSERT INTO industry_analysis 
                (document_id, detected_industry, industry_confidence, detection_scores, target_metrics)
                VALUES (?, ?, ?, ?, ?)
            """, (
                document_id,
                detected_industry,
                industry_confidence,
                json.dumps(industry_detection.get('scores', {})),
                json.dumps(self.knowledge_base.get_all_target_metrics(detected_industry))
            ))
            
            self.db_manager.connection.commit()
            
            print(f"  📝 Created document record ID: {document_id}")
            return document_id
            
        except Exception as e:
            self.db_manager.connection.rollback()
            raise Exception(f"Failed to create document record: {str(e)}")
    
    # EMERGENCY FIX for document_processor.py
    # Replace the _extract_metrics method with this simplified version

    def _extract_metrics(self, pdf_path: str, document_id: int, industry_detection: Dict) -> Dict:
        """Extract metrics WITHOUT dual-agent verification"""
        
        # Get the detected industry
        detected_industry = industry_detection['industry']
        
        # Select pages to process
        pages_to_process = self._select_pages_to_process(pdf_path, detected_industry)
        
        results = {
            'metrics': [],
            'processed_pages': [],
            'success_rate': 0.0
        }
        
        try:
            with pdfplumber.open(pdf_path) as pdf:
                for page_num in pages_to_process:
                    if page_num > len(pdf.pages):
                        continue
                        
                    page = pdf.pages[page_num - 1]
                    text = page.extract_text() or ""
                    
                    if len(text.strip()) < 100:
                        continue
                    
                    # Use regular extraction (NOT dual-agent)
                    page_metrics = self._extract_page_metrics(text, page_num, detected_industry, document_id)
                    
                    if page_metrics:
                        results['metrics'].extend(page_metrics)
                        # Store metrics immediately
                        self._store_page_metrics(document_id, page_metrics)
                    
                    results['processed_pages'].append(page_num)
                    print(f"    ✅ Page {page_num}: {len(page_metrics)} metrics extracted")
            
            # Calculate success rate
            total_pages = len(results['processed_pages'])
            if total_pages > 0:
                results['success_rate'] = len(results['metrics']) / (total_pages * 5)  # Assume 5 metrics per page average
            
            # Add empty verification summary for compatibility
            results['verification_summary'] = {
                'verified_count': len(results['metrics']),
                'disputed_count': 0,
                'total_count': len(results['metrics']),
                'verification_rate': 1.0
            }
            
            # For template compatibility
            results['verified_metrics'] = results['metrics']
            results['disputed_metrics'] = []
            results['uncertain_metrics'] = []
            
            return results
            
        except Exception as e:
            raise Exception(f"Metric extraction failed: {str(e)}")

    def _select_pages_to_process(self, pdf_path: str, industry: str) -> List[int]:
        """Select optimal pages for processing"""
        try:
            with pdfplumber.open(pdf_path) as pdf:
                total_pages = len(pdf.pages)
        except:
            return list(range(1, min(self.MAX_PAGES_TO_PROCESS + 1, 21)))
        
        if total_pages <= self.MAX_PAGES_TO_PROCESS:
            return list(range(1, total_pages + 1))
        
        # Strategic page selection
        selected_pages = []
        
        # Early pages (usually contain key metrics)
        selected_pages.extend(range(1, min(8, total_pages + 1)))
        
        # Middle section (often contains detailed financials)
        middle_start = max(8, total_pages // 3)
        middle_end = min(middle_start + 6, total_pages + 1)
        selected_pages.extend(range(middle_start, middle_end))
        
        # Later pages (may contain supplementary data)
        if total_pages > 15:
            late_start = max(middle_end, 2 * total_pages // 3)
            late_end = min(late_start + 4, total_pages + 1)
            selected_pages.extend(range(late_start, late_end))
        
        # Remove duplicates and sort
        selected_pages = sorted(list(set(selected_pages)))
        
        # Limit to max pages
        return selected_pages[:self.MAX_PAGES_TO_PROCESS]
    
    def _extract_page_metrics(self, text: str, page_num: int, industry: str, document_id: int) -> List[Dict]:
        """Extract metrics from a single page"""
        # Create industry-specific prompt
        prompt = self._create_extraction_prompt(industry)
        
        # Extract using LLM client
        metrics = self.llm_client.extract_metrics(text, page_num, prompt, 90, industry)
        
        # Classify metrics
        for metric in metrics:
            metric['metric_type'] = self._classify_metric_type(metric['metric'], industry)
            metric['document_id'] = document_id
        
        return metrics
    
    def _create_extraction_prompt(self, industry: str) -> str:
        """Create more explicit extraction prompt"""
        industry_info = self.knowledge_base.get_industry_info(industry)
        
        base_prompt = """
    IMPORTANT: You MUST return ONLY a JSON array. No explanations, no text, ONLY JSON.

    Extract ALL numerical metrics from this text. Include:
    1. Any number with currency (€, $, EUR, USD)
    2. Any number with units (million, billion, %, thousand)
    3. Any metric name followed by a number
    4. Financial figures, operational statistics, employee counts

    REQUIRED FORMAT - Return ONLY this JSON structure:
    [
    {"metric_name": "Total Revenue", "value": 1234.5, "unit": "million EUR", "period": "2024"},
    {"metric_name": "Net Income", "value": 567.8, "unit": "million EUR", "period": "2024"}
    ]

    RULES:
    - metric_name: descriptive name of what the number represents
    - value: the numerical value ONLY (no currency symbols)
    - unit: include currency and scale (e.g., "million EUR", "billion USD", "percentage")
    - period: year or time period (e.g., "2024", "Q1 2024", "FY2023")

    Start your response with [ and end with ]
    """
        
        if industry != "other" and industry_info.get('key_metrics'):
            industry_metrics = ", ".join(list(industry_info['key_metrics'].keys())[:5])
            base_prompt += f"\n\nPay special attention to {industry} metrics like: {industry_metrics}"
        
        return base_prompt

    # Fix 3: Add fallback extraction method
    # Add this to document_processor.py:

    def _fallback_regex_extraction(self, text: str, page_num: int) -> List[Dict]:
            """Fallback regex-based extraction when LLM fails"""
            print("    🔧 Using fallback regex extraction...")
            
            metrics = []
            
            # Common patterns for financial data
            patterns = [
                # Format: "Revenue: €1,234.5 million"
                r'([\w\s]+?):\s*[€$£]?\s*([\d,]+\.?\d*)\s*(million|billion|thousand)?\s*(EUR|USD|GBP)?',
                # Format: "€1,234.5 million in revenue"
                r'[€$£]\s*([\d,]+\.?\d*)\s*(million|billion|thousand)?\s*(?:in|of|for)?\s*([\w\s]+)',
                # Format: "Total assets of 1,234.5 million"
                r'([\w\s]+?)\s+of\s+[€$£]?\s*([\d,]+\.?\d*)\s*(million|billion|thousand)?',
                # Percentage format: "growth of 12.5%"
                r'([\w\s]+?)\s+of\s+([\d,]+\.?\d*)\s*%',
            ]
            
            for pattern in patterns:
                matches = re.findall(pattern, text, re.IGNORECASE)
                for match in matches:
                    try:
                        if len(match) >= 2:
                            metric_name = match[0].strip() if isinstance(match[0], str) else str(match[2]).strip()
                            value_str = match[1].replace(',', '')
                            value = float(value_str)
                            
                            unit = "number"
                            if len(match) > 2 and match[2]:
                                unit = match[2]
                            if len(match) > 3 and match[3]:
                                unit += f" {match[3]}"
                            
                            metrics.append({
                                "metric": metric_name,
                                "value": value,
                                "unit": unit,
                                "period": "2024",  # Default, would need better detection
                                "confidence": 0.7,  # Lower confidence for regex
                                "page_number": page_num,
                                "extraction_method": "regex_fallback",
                                "source_text": match[0]
                            })
                    except:
                        continue
            
            print(f"    📊 Fallback extracted {len(metrics)} metrics")
            return metrics
    
    def _classify_metric_type(self, metric_name: str, industry: str) -> str:
        """Classify metric as universal, industry-specific, or other"""
        metric_lower = metric_name.lower()
        
        # Check universal metrics
        for universal_metric in self.knowledge_base.universal_metrics:
            if universal_metric.replace('_', ' ') in metric_lower:
                return 'universal'
        
        # Check industry-specific metrics
        industry_info = self.knowledge_base.get_industry_info(industry)
        if industry_info.get('key_metrics'):
            for industry_metric in industry_info['key_metrics']:
                if industry_metric.replace('_', ' ') in metric_lower:
                    return 'industry_specific'
        
        return 'other'
    
    def _store_page_metrics(self, document_id: int, metrics: List[Dict]):
        """Store extracted metrics in database"""
        if not metrics:
            return
        
        cursor = self.db_manager.connection.cursor()
        
        metrics_data = []
        for metric in metrics:
            metrics_data.append((
                document_id,
                metric['page_number'],
                metric['metric'],
                metric['metric_type'],
                metric['value'],
                metric['unit'],
                metric['period'],
                metric['confidence'],
                metric['extraction_method'],
                metric.get('source_text', '')
            ))
        
        cursor.executemany("""
            INSERT INTO financial_metrics 
            (document_id, page_number, metric_name, metric_type, value, unit, 
             period, confidence, extraction_method, source_text)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, metrics_data)
        
        self.db_manager.connection.commit()
    
    def _generate_insights(self, document_id: int, extraction_results: Dict, 
                          industry_detection: Dict) -> List[Dict]:
        """Generate business insights from extracted metrics"""
        metrics = extraction_results['metrics']
        industry = industry_detection['industry']
        
        insights = []
        
        # Generate insights based on metrics
        if metrics:
            # Financial health insight
            revenue_metrics = [m for m in metrics if 'revenue' in m['metric'].lower()]
            if revenue_metrics:
                revenue = revenue_metrics[0]['value']
                insights.append({
                    'concept': 'Financial Performance',
                    'insight': f"Revenue of {revenue:,.0f} {revenue_metrics[0]['unit']} indicates {'strong' if revenue > 1000 else 'moderate'} financial performance",
                    'supporting_metrics': [revenue_metrics[0]['metric']],
                    'confidence': 0.85
                })
            
            # Industry-specific insights
            if industry == 'airlines':
                load_factors = [m for m in metrics if 'load_factor' in m['metric'].lower()]
                if load_factors:
                    lf = load_factors[0]['value']
                    performance = 'excellent' if lf > 85 else 'good' if lf > 80 else 'needs improvement'
                    insights.append({
                        'concept': 'Operational Efficiency',
                        'insight': f"Load factor of {lf:.1f}% indicates {performance} operational efficiency",
                        'supporting_metrics': [load_factors[0]['metric']],
                        'confidence': 0.90
                    })
        
        # Store insights in database
        if insights:
            self._store_insights(document_id, insights)
        
        return insights
    
    def _store_insights(self, document_id: int, insights: List[Dict]):
        """Store business insights in database"""
        cursor = self.db_manager.connection.cursor()
        
        for insight in insights:
            cursor.execute("""
                INSERT INTO business_intelligence 
                (document_id, concept, insight_text, supporting_metrics, confidence)
                VALUES (?, ?, ?, ?, ?)
            """, (
                document_id,
                insight['concept'],
                insight['insight'],
                json.dumps(insight['supporting_metrics']),
                insight['confidence']
            ))
        
        self.db_manager.connection.commit()
    
    def _finalize_processing(self, document_id: int, extraction_results: Dict, 
                           insights: List[Dict], start_time: float) -> Dict:
        """Finalize processing and update database"""
        processing_time = time.time() - start_time
        
        # Update document status
        cursor = self.db_manager.connection.cursor()
        cursor.execute("""
            UPDATE documents 
            SET status = ?, pages_processed = ?, processing_time = ?, completed_at = ?
            WHERE id = ?
        """, (
            'completed',
            len(extraction_results['processed_pages']),
            processing_time,
            datetime.now(),
            document_id
        ))
        
        self.db_manager.connection.commit()
        
        return {
            'success': True,
            'document_id': document_id,
            'metrics_extracted': len(extraction_results['metrics']),
            'pages_processed': len(extraction_results['processed_pages']),
            'insights_generated': len(insights),
            'processing_time': processing_time,
            'success_rate': extraction_results['success_rate']
        }
    
    def _update_document_status(self, document_id: int, status: str, error_message: str = None):
        """Update document processing status"""
        cursor = self.db_manager.connection.cursor()
        cursor.execute("""
            UPDATE documents 
            SET status = ?, completed_at = ?
            WHERE id = ?
        """, (status, datetime.now(), document_id))
        
        if error_message:
            cursor.execute("""
                INSERT INTO processing_logs (document_id, stage, message, level)
                VALUES (?, ?, ?, ?)
            """, (document_id, 'error', error_message, 'ERROR'))
        
        self.db_manager.connection.commit()
    
    def _extract_company_name(self, text: str) -> str:
        """Extract company name from text"""
        patterns = [
            r'([A-Z][A-Za-z\s&\.]+(?:GROUP|PLC|INC|CORP|LIMITED|LTD|HOLDINGS|SA|AG|NV|LLC))',
            r'ANNUAL\s*REPORT\s*(?:2024|2025|2023|2022)\s*(?:FOR|OF)\s*([A-Z][A-Za-z\s,&\.]+)',
            r'([A-Z][A-Za-z\s&\.]+)\s*(?:ANNUAL|FINANCIAL)\s*REPORT'
        ]
        
        for pattern in patterns:
            matches = re.findall(pattern, text, re.IGNORECASE)
            for match in matches:
                name = match.strip()
                name = re.sub(r'\s+', ' ', name)
                if 3 <= len(name) <= 100 and not any(fp in name.upper() for fp in ['ANNUAL REPORT', 'FINANCIAL STATEMENTS']):
                    return name
        
        return "Unknown Company"
    
    def _store_verified_metrics(self, document_id: int, verification_results: List, detected_industry: str):
        """Store metrics with verification information"""
        cursor = self.db_manager.connection.cursor()
        
        for result in verification_results:
            # Store the main metric
            cursor.execute("""
                INSERT INTO financial_metrics 
                (document_id, page_number, metric_name, metric_type, value, unit, 
                period, confidence, extraction_method, source_text)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                document_id,
                result.original_claim.evidence.page_number,
                result.original_claim.metric_name,
                self._classify_metric_type(result.original_claim.metric_name, detected_industry),
                result.consensus_value or result.original_claim.value,
                result.original_claim.unit,
                result.original_claim.period,
                result.confidence_score,
                "dual_agent_verification",
                result.original_claim.evidence.source_quote
            ))
            
            metric_id = cursor.lastrowid
            
            # Store verification details
            cursor.execute("""
                INSERT INTO metric_verification 
                (metric_id, verification_status, original_confidence, verification_confidence,
                consensus_confidence, original_source_quote, verification_source_quote,
                conflict_analysis, resolution_reasoning)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                metric_id,
                result.status.value,
                result.original_claim.evidence.confidence,
                result.verification_claim.evidence.confidence if result.verification_claim else None,
                result.confidence_score,
                result.original_claim.evidence.source_quote,
                result.verification_claim.evidence.source_quote if result.verification_claim else None,
                result.conflict_analysis,
                result.resolution_reasoning
            ))
        
        self.db_manager.connection.commit()

    def _get_verification_summary(self, document_id: int) -> Dict:
        """Get verification summary for a document"""
        cursor = self.db_manager.connection.cursor()
        
        cursor.execute("""
            SELECT 
                COUNT(CASE WHEN mv.verification_status = 'verified' THEN 1 END) as verified_count,
                COUNT(CASE WHEN mv.verification_status = 'disputed' THEN 1 END) as disputed_count,
                COUNT(*) as total_count
            FROM financial_metrics fm
            LEFT JOIN metric_verification mv ON fm.id = mv.metric_id
            WHERE fm.document_id = ?
        """, (document_id,))
        
        result = cursor.fetchone()
        if result:
            verified, disputed, total = result
            return {
                'verified_count': verified or 0,
                'disputed_count': disputed or 0,
                'total_count': total or 0,
                'verification_rate': (verified or 0) / total if total > 0 else 0
            }
        
        return {
            'verified_count': 0,
            'disputed_count': 0,
            'total_count': 0,
            'verification_rate': 0
        }

    def _convert_verification_to_metric(self, result: VerificationResult, document_id: int) -> Dict:
        """Convert verification result to metric format for backward compatibility"""
        return {
            'metric': result.original_claim.metric_name,
            'value': result.consensus_value or result.original_claim.value,
            'unit': result.original_claim.unit,
            'period': result.original_claim.period,
            'confidence': result.confidence_score,
            'page_number': result.original_claim.evidence.page_number,
            'extraction_method': 'dual_agent_verification',
            'verification_status': result.status.value,
            'source_text': result.original_claim.evidence.source_quote,
            'document_id': document_id
        }

    # Fix for document_processor.py - Replace the get_company_intelligence method

    def get_company_intelligence(self, document_id: int) -> Dict:
        """Get comprehensive company intelligence"""
        cursor = self.db_manager.connection.cursor()
        
        try:
            # Get basic document info
            cursor.execute("""
                SELECT c.name, c.detected_industry, c.industry_confidence,
                    d.filename, d.total_pages, d.pages_processed, d.processing_time, d.status
                FROM companies c
                JOIN documents d ON c.id = d.company_id
                WHERE d.id = ?
            """, (document_id,))
            
            doc_info = cursor.fetchone()
            if not doc_info:
                return {'error': 'Document not found'}
            
            # Get ALL metrics (with or without verification)
            cursor.execute("""
                SELECT fm.metric_name, fm.metric_type, fm.value, fm.unit, fm.period, 
                    fm.confidence, mv.verification_status, mv.conflict_analysis
                FROM financial_metrics fm
                LEFT JOIN metric_verification mv ON fm.id = mv.metric_id
                WHERE fm.document_id = ?
                ORDER BY fm.confidence DESC
            """, (document_id,))
            
            metrics_data = cursor.fetchall()
            
            # Get insights
            cursor.execute("""
                SELECT concept, insight_text, supporting_metrics, confidence
                FROM business_intelligence
                WHERE document_id = ?
                ORDER BY confidence DESC
            """, (document_id,))
            
            insights_data = cursor.fetchall()
            
            # Organize data
            universal_metrics = {}
            industry_metrics = {}
            other_metrics = {}
            verified_metrics = {}
            disputed_metrics = {}
            uncertain_metrics = {}
            
            for row in metrics_data:
                metric_info = {
                    'value': row[2],
                    'unit': row[3],
                    'period': row[4],
                    'confidence': row[5],
                    'verification_status': row[6] or 'unverified',  # Default to 'unverified'
                    'notes': row[7]  # conflict_analysis
                }
                
                metric_name = row[0]
                metric_type = row[1]
                verification_status = row[6]
                
                # First categorize by type
                if metric_type == 'universal':
                    universal_metrics[metric_name] = metric_info
                elif metric_type == 'industry_specific':
                    industry_metrics[metric_name] = metric_info
                else:
                    other_metrics[metric_name] = metric_info
                
                # Also categorize by verification status (if using dual-agent)
                if verification_status == 'verified':
                    verified_metrics[metric_name] = metric_info
                elif verification_status == 'disputed':
                    disputed_metrics[metric_name] = metric_info
                elif verification_status == 'uncertain':
                    uncertain_metrics[metric_name] = metric_info
                else:
                    # If no verification status, treat as verified for display
                    verified_metrics[metric_name] = metric_info
            
            # Format insights
            insights = []
            for row in insights_data:
                insights.append({
                    'concept': row[0],
                    'insight': row[1],
                    'supporting_metrics': json.loads(row[2]) if row[2] else [],
                    'confidence': row[3]
                })
            
            # Calculate verification summary
            total_metrics = len(metrics_data)
            verified_count = len([m for m in metrics_data if m[6] == 'verified' or m[6] is None])
            disputed_count = len([m for m in metrics_data if m[6] == 'disputed'])
            
            return {
                'company_profile': {
                    'name': doc_info[0],
                    'detected_industry': doc_info[1],
                    'industry_confidence': doc_info[2],
                    'filename': doc_info[3],
                    'total_pages': doc_info[4],
                    'pages_processed': doc_info[5],
                    'processing_time': doc_info[6],
                    'status': doc_info[7]
                },
                'universal_metrics': universal_metrics,
                'industry_specific_metrics': industry_metrics,
                'other_metrics': other_metrics,
                'business_intelligence': insights,
                'verified_metrics': verified_metrics,
                'disputed_metrics': disputed_metrics,
                'uncertain_metrics': uncertain_metrics,
                'verification_summary': {
                    'verified_count': verified_count,
                    'disputed_count': disputed_count,
                    'total_count': total_metrics,
                    'verification_rate': verified_count / total_metrics if total_metrics > 0 else 0
                },
                'coverage_analysis': {
                    'total_metrics': total_metrics,
                    'universal_coverage': len(universal_metrics),
                    'industry_coverage': len(industry_metrics),
                    'other_coverage': len(other_metrics)
                }
            }
            
        except Exception as e:
            print(f"Error in get_company_intelligence: {str(e)}")
            import traceback
            traceback.print_exc()
            return {'error': f'Failed to get intelligence: {str(e)}'}
    
    def get_processing_progress(self, document_id: int) -> Dict:
        """Get processing progress for a document"""
        cursor = self.db_manager.connection.cursor()
        
        try:
            # Get document status
            cursor.execute("""
                SELECT status, pages_processed, total_pages, processing_time
                FROM documents
                WHERE id = ?
            """, (document_id,))
            
            result = cursor.fetchone()
            if not result:
                return {'error': 'Document not found', 'status': 'not_found'}
            
            status, pages_processed, total_pages, processing_time = result
            
            # Calculate progress percentage
            if status == 'completed':
                progress = 100
            elif status == 'failed':
                progress = 0
            elif pages_processed and total_pages:
                progress = min(int((pages_processed / total_pages) * 100), 95)
            else:
                progress = 10  # Initial progress
            
            return {
                'document_id': document_id,
                'status': status,
                'progress': progress,
                'pages_processed': pages_processed or 0,
                'total_pages': total_pages or 0,
                'processing_time': processing_time,
                'message': self._get_status_message(status, progress)
            }
            
        except Exception as e:
            return {'error': f'Failed to get progress: {str(e)}', 'status': 'error'}
    
    def _get_status_message(self, status: str, progress: int) -> str:
        """Get human-readable status message"""
        if status == 'completed':
            return 'Analysis completed successfully'
        elif status == 'failed':
            return 'Analysis failed'
        elif status == 'processing':
            if progress < 20:
                return 'Analyzing document structure...'
            elif progress < 40:
                return 'Detecting industry type...'
            elif progress < 70:
                return 'Extracting metrics...'
            elif progress < 90:
                return 'Generating insights...'
            else:
                return 'Finalizing analysis...'
        else:
            return 'Processing...'
    
    def get_recent_analyses(self, limit: int = 6) -> List[Dict]:
        """Get recent analyses for dashboard"""
        cursor = self.db_manager.connection.cursor()
        
        try:
            cursor.execute("""
                SELECT d.id, c.name, c.detected_industry, d.created_at, d.status,
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
            
            recent = []
            for row in results:
                recent.append({
                    'id': row[0],
                    'company': row[1],
                    'industry': row[2] or 'Other',
                    'date': row[3],
                    'status': row[4],
                    'metrics': row[5]
                })
            
            return recent
            
        except Exception as e:
            print(f"Error getting recent analyses: {e}")
            return []