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
from .dual_agent_verification import DualAgentVerificationSystem, VerificationStatus

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
    
    def _extract_metrics(self, pdf_path: str, document_id: int, processing_plan: Dict) -> Dict:
        """Extract metrics using dual-agent verification system"""
        pages_to_process = processing_plan['pages_to_process']
        detected_industry = processing_plan['detected_industry']
        
        results = {
            'verified_metrics': [],
            'disputed_metrics': [],
            'uncertain_metrics': [],
            'processed_pages': [],
            'verification_summary': {}
        }
        
        try:
            with pdfplumber.open(pdf_path) as pdf:
                for page_num in pages_to_process:
                    page = pdf.pages[page_num - 1]
                    text = page.extract_text() or ""
                    
                    if len(text.strip()) < 100:
                        continue
                    
                    # Use dual-agent verification instead of single extraction
                    verification_results = self.verification_system.extract_and_verify(
                        text, page_num, detected_industry
                    )
                    
                    # Categorize results by verification status
                    for result in verification_results:
                        metric_data = self._convert_verification_to_metric(result, document_id)
                        
                        if result.status == VerificationStatus.VERIFIED:
                            results['verified_metrics'].append(metric_data)
                        elif result.status == VerificationStatus.DISPUTED:
                            results['disputed_metrics'].append(metric_data)
                        else:
                            results['uncertain_metrics'].append(metric_data)
                    
                    results['processed_pages'].append(page_num)
                    
                    # Store all metrics in database with verification status
                    self._store_verified_metrics(document_id, verification_results)
                    
                    print(f"    ✅ Page {page_num}: {len(verification_results)} metrics processed")
            
            # Get verification summary
            results['verification_summary'] = self.verification_system.get_verification_summary()
            
            # Combine all metrics for backward compatibility
            all_metrics = (results['verified_metrics'] + 
                          results['uncertain_metrics'] + 
                          results['disputed_metrics'])
            results['metrics'] = all_metrics
            
            return results
            
        except Exception as e:
            raise Exception(f"Dual-agent metric extraction failed: {str(e)}")
    
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
        """Create extraction prompt based on industry"""
        industry_info = self.knowledge_base.get_industry_info(industry)
        
        if not industry_info.get('key_metrics'):
            return """
Extract all financial and operational metrics from this text.
Focus on: revenue, costs, profits, employee data, operational statistics.
Return JSON array: [{"metric_name": "name", "value": number, "unit": "unit", "period": "period"}]
"""
        
        key_metrics = industry_info['key_metrics']
        metrics_desc = []
        
        for metric, info in key_metrics.items():
            synonyms = ", ".join(info['synonyms'][:3])
            metrics_desc.append("- {}: {} (terms: {})".format(metric, info['description'], synonyms))
        
        metrics_section = "\n".join(metrics_desc)
        
        return """
INDUSTRY-SPECIFIC EXTRACTION FOR {}

Extract financial and operational metrics, prioritizing {}-specific metrics:

{} METRICS:
{}

UNIVERSAL METRICS:
- Total revenue, net income, operating costs, employee count, total assets

Return JSON array: [{{"metric_name": "name", "value": number, "unit": "unit", "period": "period"}}]
Return ONLY valid JSON array, no other text.
""".format(industry.upper(), industry, industry.upper(), metrics_section)
    
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
    
    def _store_verified_metrics(self, document_id: int, verification_results: List[VerificationResult]):
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
            
            # Get metrics
            cursor.execute("""
                SELECT fm.metric_name, fm.metric_type, fm.value, fm.unit, fm.period, 
                    fm.confidence, mv.verification_status, mv.conflict_analysis
                FROM financial_metrics fm
                LEFT JOIN metric_verification mv ON fm.id = mv.metric_id
                WHERE fm.document_id = ?
                ORDER BY mv.verification_status, fm.confidence DESC
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
                    'verification_status': row[6],
                    'notes': row[7]  # conflict_analysis
                }
                
                if row[6] == 'verified':
                    if row[1] == 'universal':
                        universal_metrics[row[0]] = metric_info
                    elif row[1] == 'industry_specific':
                        industry_metrics[row[0]] = metric_info
                    else:
                        other_metrics[row[0]] = metric_info
                elif row[6] == 'disputed':
                    disputed_metrics[row[0]] = metric_info
                else:
                    uncertain_metrics[row[0]] = metric_info
            
            # Format insights
            insights = []
            for row in insights_data:
                insights.append({
                    'concept': row[0],
                    'insight': row[1],
                    'supporting_metrics': json.loads(row[2]) if row[2] else [],
                    'confidence': row[3]
                })
            
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
                'verification_summary': self._get_verification_summary(document_id),
                'coverage_analysis': {
                    'total_metrics': len(metrics_data),
                    'universal_coverage': len(universal_metrics),
                    'industry_coverage': len(industry_metrics),
                    'other_coverage': len(other_metrics)
                }
            }
            
        except Exception as e:
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