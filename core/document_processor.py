"""
Complete document processing orchestrator with industry intelligence
core/document_processor.py
"""

import time
import pdfplumber
from pathlib import Path
from typing import Dict, List
from .industry_analyzer import IndustryIntelligentAnalyzer
from .knowledge_base import GICSKnowledgeBase
from .database import DatabaseManager
from utils.api_client import LLMClient

class DocumentProcessor:
    """
    Main document processing orchestrator with full industry intelligence
    """
    
    def __init__(self, api_key: str, base_url: str, db_manager: DatabaseManager):
        self.db_manager = db_manager
        self.industry_analyzer = IndustryIntelligentAnalyzer()
        self.knowledge_base = GICSKnowledgeBase()
        self.llm_client = LLMClient(api_key, base_url)
        
        # Processing configuration
        self.MAX_PAGES_TO_PROCESS = 20
        self.MAX_PROCESSING_TIME = 900  # 15 minutes
        self.BATCH_SIZE = 4
        
        # Progress tracking
        self.processing_progress = {"current": 0, "total": 0, "status": "idle"}
        
    def process_document(self, pdf_path: str) -> Dict:
        """
        Complete document processing pipeline with industry intelligence
        """
        print(f"\n🚀 Starting Industry-Intelligent Document Processing...")
        start_time = time.time()
        
        try:
            # Phase 1: Industry-intelligent structure analysis
            print("🧠 Phase 1: Industry detection and structure analysis...")
            industry_analysis = self.industry_analyzer.analyze_document_structure(pdf_path)
            
            detected_industry = industry_analysis["detected_industry"]["industry"]
            company_name = industry_analysis["company_name"]
            
            # Phase 2: Create industry-specific processing plan
            print("🎯 Phase 2: Creating industry-specific processing plan...")
            processing_plan = self._create_industry_processing_plan(industry_analysis)
            
            print(f"📋 Industry Processing Plan:")
            print(f"  🏭 Industry: {detected_industry}")
            print(f"  📄 Pages to process: {len(processing_plan['pages_to_process'])}")
            print(f"  🎯 Target metrics: {len(processing_plan['target_metrics'])}")
            print(f"  ⏱️  Estimated time: {processing_plan['estimated_time']:.1f} minutes")
            
            # Phase 3: Industry-aware extraction
            print("🔍 Phase 3: Industry-aware metric extraction...")
            results = self._process_pages_with_industry_intelligence(
                pdf_path, processing_plan, self.MAX_PROCESSING_TIME, start_time
            )
            
            total_time = time.time() - start_time
            
            # Phase 4: Business intelligence synthesis
            print("🧠 Phase 4: Business intelligence synthesis...")
            business_insights = self._generate_business_insights(results, detected_industry)
            
            # Phase 5: Store comprehensive results
            document_id = self.db_manager.store_comprehensive_results(
                pdf_path, industry_analysis, processing_plan, results, 
                business_insights, total_time
            )
            
            # Final results
            final_results = {
                "document_id": document_id,
                "company_name": company_name,
                "detected_industry": detected_industry,
                "total_pages": industry_analysis["total_pages"],
                "pages_processed": len(results.get("processed_pages", [])),
                "universal_metrics_extracted": len([m for m in results.get("metrics", []) if m.get("metric_type") == "universal"]),
                "industry_metrics_extracted": len([m for m in results.get("metrics", []) if m.get("metric_type") == "industry_specific"]),
                "total_metrics_extracted": len(results.get("metrics", [])),
                "business_insights": business_insights,
                "processing_time": total_time,
                "extraction_success_rate": results.get("extraction_success_rate", 0.0),
                "completed": results.get("completed", False),
                "success": True
            }
            
            print(f"\n✅ Industry-Intelligent Processing Complete!")
            print(f"  🏢 Company: {company_name}")
            print(f"  🏭 Industry: {detected_industry}")
            print(f"  ⏱️  Total time: {total_time:.1f}s")
            print(f"  📊 Pages processed: {final_results['pages_processed']}/{final_results['total_pages']}")
            print(f"  💰 Universal metrics: {final_results['universal_metrics_extracted']}")
            print(f"  🎯 Industry metrics: {final_results['industry_metrics_extracted']}")
            print(f"  🧠 Business insights: {len(business_insights)}")
            
            return final_results
            
        except Exception as e:
            print(f"❌ Error during document processing: {e}")
            return {
                "success": False,
                "error": str(e),
                "processing_time": time.time() - start_time
            }
    
    def _create_industry_processing_plan(self, industry_analysis: Dict) -> Dict:
        """
        Create comprehensive processing plan based on industry analysis
        """
        detected_industry = industry_analysis["detected_industry"]["industry"]
        layout_analysis = industry_analysis["layout_analysis"]
        total_pages = industry_analysis["total_pages"]
        
        # Get target metrics for this industry
        target_metrics = self.knowledge_base.get_critical_metrics(detected_industry)
        all_metrics = self.knowledge_base.get_all_target_metrics(detected_industry)
        
        # Smart page selection based on industry and layout analysis
        pages_to_process = self._select_industry_relevant_pages(industry_analysis)
        
        # Determine processing strategy based on industry
        if detected_industry in ["airlines", "banking", "technology"]:
            strategy = "industry_focused"
            batch_size = 3  # Smaller batches for focused extraction
            estimated_time_per_page = 2.5  # More thorough processing
        else:
            strategy = "comprehensive"
            batch_size = 4
            estimated_time_per_page = 2.0
        
        return {
            "detected_industry": detected_industry,
            "target_metrics": all_metrics,
            "critical_metrics": target_metrics,
            "pages_to_process": pages_to_process,
            "strategy": strategy,
            "batch_size": batch_size,
            "estimated_time": len(pages_to_process) * estimated_time_per_page / 60,  # Convert to minutes
            "layout_analysis": layout_analysis
        }
    
    def _select_industry_relevant_pages(self, industry_analysis: Dict) -> List[int]:
        """
        Select pages most relevant to the detected industry
        """
        detected_industry = industry_analysis["detected_industry"]["industry"]
        layout_analysis = industry_analysis["layout_analysis"]
        total_pages = industry_analysis["total_pages"]
        
        selected_pages = []
        
        # Priority 1: High-value pages identified by industry analysis
        high_value_pages = layout_analysis.get("high_value_pages", [])
        selected_pages.extend(high_value_pages[:8])
        
        # Priority 2: Financial pages with strong signals
        financial_pages = layout_analysis.get("financial_pages", [])
        selected_pages.extend([p for p in financial_pages[:6] if p not in selected_pages])
        
        # Priority 3: Pages with industry-specific tables
        layout_types = layout_analysis.get("layout_types", {})
        if layout_types.get("industry_specific_table", 0) > 0:
            # Add pages likely to contain industry-specific tables
            for page_num in range(1, min(total_pages + 1, 50)):
                if page_num not in selected_pages and len(selected_pages) < 15:
                    selected_pages.append(page_num)
        
        # Priority 4: Structured financial pages
        if layout_types.get("structured_table", 0) > 0:
            for page_num in range(1, min(total_pages + 1, 30)):
                if page_num not in selected_pages and len(selected_pages) < 12:
                    selected_pages.append(page_num)
        
        # Ensure minimum coverage - add strategic pages if needed
        if len(selected_pages) < 8:
            # Add pages from different sections of the document
            sections = [
                range(1, min(10, total_pages + 1)),  # Early pages
                range(max(1, total_pages // 3), min(2 * total_pages // 3, total_pages + 1)),  # Middle
                range(max(1, 2 * total_pages // 3), total_pages + 1)  # Later pages
            ]
            
            for section in sections:
                for page_num in section:
                    if page_num not in selected_pages and len(selected_pages) < 12:
                        selected_pages.append(page_num)
                        if len(selected_pages) % 3 == 0:  # Don't add too many at once
                            break
        
        # Final selection and sorting
        selected_pages = list(set(selected_pages))[:self.MAX_PAGES_TO_PROCESS]
        selected_pages.sort()
        
        print(f"  📄 Selected {len(selected_pages)} industry-relevant pages: {selected_pages[:10]}{'...' if len(selected_pages) > 10 else ''}")
        return selected_pages

    def _process_pages_with_industry_intelligence(self, pdf_path: str, processing_plan: Dict, 
                                                max_time_seconds: int, start_time: float) -> Dict:
        """
        Process pages with comprehensive industry-specific intelligence
        """
        pages_to_process = processing_plan["pages_to_process"]
        detected_industry = processing_plan["detected_industry"]
        batch_size = processing_plan["batch_size"]
        
        self.processing_progress = {
            "current": 0, 
            "total": len(pages_to_process), 
            "status": "industry_processing"
        }
        
        results = {
            "metrics": [],
            "processed_pages": [],
            "skipped_pages": [],
            "completed": False,
            "extraction_success_rate": 0.0,
            "method_performance": {
                "industry_intelligence": {"attempts": 0, "successes": 0},
                "focused_extraction": {"attempts": 0, "successes": 0},
                "fallback_extraction": {"attempts": 0, "successes": 0}
            }
        }
        
        print(f"🔍 Processing {len(pages_to_process)} pages with {detected_industry} intelligence...")
        
        try:
            with pdfplumber.open(pdf_path) as pdf:
                for i in range(0, len(pages_to_process), batch_size):
                    # Check time limit
                    if time.time() - start_time > max_time_seconds:
                        results["early_termination_reason"] = "Time limit reached"
                        print(f"  ⏰ Time limit reached, stopping processing")
                        break
                    
                    batch_pages = pages_to_process[i:i + batch_size]
                    batch_results = self._process_industry_batch(
                        pdf, batch_pages, detected_industry, processing_plan, results
                    )
                    
                    # Aggregate results
                    results["metrics"].extend(batch_results.get("metrics", []))
                    results["processed_pages"].extend(batch_results.get("processed_pages", []))
                    results["skipped_pages"].extend(batch_results.get("skipped_pages", []))
                    
                    # Update progress
                    self.processing_progress["current"] = i + len(batch_pages)
                    progress_pct = (self.processing_progress["current"] / self.processing_progress["total"]) * 100
                    print(f"  📊 Progress: {progress_pct:.1f}% ({self.processing_progress['current']}/{self.processing_progress['total']} pages)")
                    
                    # Check for comprehensive industry coverage
                    if len(results["metrics"]) >= 50 and self._has_industry_coverage(results["metrics"], detected_industry):
                        results["early_termination_reason"] = "Comprehensive industry coverage achieved"
                        print(f"  ✅ Early termination: Comprehensive {detected_industry} coverage")
                        break
                
                # Mark as completed if no early termination
                if not results.get("early_termination_reason"):
                    results["completed"] = True
                    
            # Calculate final success rate
            total_attempts = sum(method_data["attempts"] for method_data in results["method_performance"].values())
            total_successes = sum(method_data["successes"] for method_data in results["method_performance"].values())
            
            if total_attempts > 0:
                results["extraction_success_rate"] = total_successes / total_attempts
                
        except Exception as e:
            print(f"❌ Error during industry processing: {e}")
            results["early_termination_reason"] = f"Processing error: {str(e)}"
        finally:
            self.processing_progress["status"] = "completed"
        
        return results

    def _process_industry_batch(self, pdf, page_numbers: List[int], detected_industry: str, 
                              processing_plan: Dict, results: Dict) -> Dict:
        """
        Process batch of pages with industry-specific methods
        """
        batch_results = {"metrics": [], "processed_pages": [], "skipped_pages": []}
        
        # Create industry-specific prompts
        industry_prompts = self.industry_analyzer.create_industry_prompts(
            detected_industry, processing_plan.get("layout_analysis")
        )
        
        for page_num in page_numbers:
            try:
                if page_num > len(pdf.pages):
                    batch_results["skipped_pages"].append(page_num)
                    continue
                    
                page = pdf.pages[page_num - 1]
                text = page.extract_text(x_tolerance=1, y_tolerance=3) or ""
                
                if len(text) < 100:
                    batch_results["skipped_pages"].append(page_num)
                    continue
                
                print(f"    🎯 Processing page {page_num} with {detected_industry} intelligence...")
                
                page_metrics = []
                
                # Method 1: Industry-specific extraction
                results["method_performance"]["industry_intelligence"]["attempts"] += 1
                page_metrics = self._extract_with_industry_intelligence(
                    text, page_num, detected_industry, industry_prompts.get("primary", "")
                )
                
                if page_metrics:
                    results["method_performance"]["industry_intelligence"]["successes"] += 1
                    print(f"    ✅ Industry intelligence: {len(page_metrics)} metrics")
                else:
                    # Method 2: Focused metric extraction
                    if "focused" in industry_prompts:
                        results["method_performance"]["focused_extraction"]["attempts"] += 1
                        for metric_name, focused_prompt in list(industry_prompts["focused"].items())[:2]:
                            focused_metrics = self._extract_with_industry_intelligence(
                                text, page_num, detected_industry, focused_prompt
                            )
                            if focused_metrics:
                                page_metrics.extend(focused_metrics)
                                results["method_performance"]["focused_extraction"]["successes"] += 1
                                print(f"    ✅ Focused extraction ({metric_name}): {len(focused_metrics)} metrics")
                                break
                
                # Method 3: Fallback extraction
                if not page_metrics:
                    results["method_performance"]["fallback_extraction"]["attempts"] += 1
                    page_metrics = self._extract_with_fallback_method(text, page_num)
                    if page_metrics:
                        results["method_performance"]["fallback_extraction"]["successes"] += 1
                        print(f"    ✅ Fallback extraction: {len(page_metrics)} metrics")
                
                # Process and classify metrics
                if page_metrics:
                    for metric in page_metrics:
                        metric["metric_type"] = self._classify_metric_type(
                            metric["metric"], detected_industry
                        )
                    
                    batch_results["metrics"].extend(page_metrics)
                    batch_results["processed_pages"].append(page_num)
                    print(f"    📊 Total extracted: {len(page_metrics)} metrics from page {page_num}")
                else:
                    print(f"    ❌ No metrics extracted from page {page_num}")
                    batch_results["skipped_pages"].append(page_num)
                    
            except Exception as e:
                print(f"    ❌ Error processing page {page_num}: {e}")
                batch_results["skipped_pages"].append(page_num)
                continue
        
        return batch_results

    def _extract_with_industry_intelligence(self, text: str, page_num: int, 
                                          industry: str, prompt: str) -> List[Dict]:
        """
        Extract metrics using industry-specific intelligence
        """
        if not prompt.strip():
            return []
            
        try:
            # Industry-specific timeout configuration
            industry_timeouts = {
                "airlines": 120,  # Complex operational data
                "banking": 90,   # Dense financial data
                "technology": 60, # Usually well-structured
                "retail": 75,    # Mixed data types
                "energy": 105    # Technical data
            }
            
            timeout = industry_timeouts.get(industry, 90)
            
            # Use LLM client for extraction
            extracted_metrics = self.llm_client.extract_metrics(
                text, page_num, prompt, timeout, industry
            )
            
            return extracted_metrics
            
        except Exception as e:
            print(f"    ❌ Industry extraction failed for page {page_num}: {e}")
            return []

    def _extract_with_fallback_method(self, text: str, page_num: int) -> List[Dict]:
        """
        Fallback extraction method for when industry-specific methods fail
        """
        try:
            fallback_prompt = """
Extract all financial and operational metrics from this text.

FOCUS ON:
- Revenue, income, costs, profits
- Employee data, customer data, operational statistics
- Growth rates, ratios, and performance metrics

REQUIRED JSON FORMAT:
{"metric_name": "descriptive_name", "period": "time_period", "value": number, "unit": "unit_type"}

Return ONLY a valid JSON array.
"""
            
            return self.llm_client.extract_metrics(text, page_num, fallback_prompt, 60, "general")
            
        except Exception as e:
            print(f"    ❌ Fallback extraction failed for page {page_num}: {e}")
            return []

    def _classify_metric_type(self, metric_name: str, industry: str) -> str:
        """
        Classify if metric is universal, industry-specific, or other
        """
        # Check universal metrics
        universal_metrics = list(self.knowledge_base.universal_metrics.keys())
        metric_lower = metric_name.lower()
        
        for universal in universal_metrics:
            if universal.replace('_', ' ') in metric_lower or universal in metric_lower:
                return "universal"
        
        # Check industry-specific metrics
        industry_info = self.knowledge_base.get_industry_info(industry)
        if industry_info and "key_metrics" in industry_info:
            industry_metrics = list(industry_info["key_metrics"].keys())
            for industry_metric in industry_metrics:
                if industry_metric.replace('_', ' ') in metric_lower or industry_metric in metric_lower:
                    return "industry_specific"
        
        return "other"

    def _has_industry_coverage(self, metrics: List[Dict], industry: str) -> bool:
        """
        Check if we have good coverage of industry-specific metrics
        """
        if industry == "other" or not metrics:
            return len(metrics) > 40
        
        # Get industry schema
        industry_info = self.knowledge_base.get_industry_info(industry)
        if not industry_info or not industry_info.get("key_metrics"):
            return len(metrics) > 30
        
        # Check coverage
        universal_metrics = list(self.knowledge_base.universal_metrics.keys())
        industry_metrics = list(industry_info["key_metrics"].keys())
        
        found_universal = {m.get("metric", "").lower() for m in metrics if m.get("metric_type") == "universal"}
        found_industry = {m.get("metric", "").lower() for m in metrics if m.get("metric_type") == "industry_specific"}
        
        universal_coverage = len([u for u in universal_metrics if u in ' '.join(found_universal)]) / len(universal_metrics)
        industry_coverage = len([i for i in industry_metrics if i in ' '.join(found_industry)]) / len(industry_metrics)
        
        # Good coverage if we have 40%+ universal and 30%+ industry-specific metrics
        return universal_coverage >= 0.4 and industry_coverage >= 0.3 and len(metrics) > 30

    def _generate_business_insights(self, results: Dict, industry: str) -> List[Dict]:
        """
        Generate comprehensive business intelligence insights
        """
        metrics = results.get("metrics", [])
        
        if not metrics:
            return []
        
        insights = []
        
        # Universal business concepts analysis
        universal_concepts = {
            "Financial Health": ["total_revenue", "net_income", "cash_flow", "operating_income"],
            "Operational Efficiency": ["operating_costs", "efficiency_ratio", "cost_per_ask", "margin"],
            "Market Position": ["market_share", "customer_base", "growth_rate", "passengers_carried"],
            "Asset Management": ["total_assets", "fleet_size", "capacity_utilization", "inventory"],
            "Innovation & Growth": ["r_and_d_spending", "new_products", "technology_investment"]
        }
        
        # Analyze each concept
        for concept, related_metrics in universal_concepts.items():
            concept_metrics = []
            for metric in metrics:
                metric_name = metric.get("metric", "").lower()
                if any(rm in metric_name for rm in related_metrics):
                    concept_metrics.append(metric)
            
            if concept_metrics:
                insight_text = self._generate_concept_insight(concept, concept_metrics, industry)
                if insight_text:
                    insights.append({
                        "concept": concept,
                        "insight": insight_text,
                        "supporting_metrics": [m["metric"] for m in concept_metrics[:3]],
                        "confidence": 0.85
                    })
        
        # Industry-specific insights
        industry_insights = self._generate_industry_specific_insights(metrics, industry)
        insights.extend(industry_insights)
        
        return insights

        def _generate_concept_insight(self, concept: str, metrics: List[Dict], industry: str) -> str:
            """
            Generate insight for a business concept
            """
            if not metrics:
                return ""
            
            # Find the most recent metrics
            recent_metrics = {}
            for metric in metrics:
                period = metric.get("period", "unknown")
                if period not in ["unknown", "unspecified"]:
                    key = metric.get("metric", "")
                    if key not in recent_metrics or period > recent_metrics[key].get("period", ""):
                        recent_metrics[key] = metric
            
            if not recent_metrics:
                return ""
            
            # Generate concept-specific insights
            if concept == "Financial Health":
                revenue_metrics = [m for m in recent_metrics.values() if "revenue" in m.get("metric", "").lower()]
                if revenue_metrics:
                    revenue = revenue_metrics[0]
                    return f"Revenue of {revenue['value']:,.0f} {revenue['unit']} indicates {'strong' if revenue['value'] > 1000 else 'moderate'} financial performance"
            
            elif concept == "Operational Efficiency":
                if industry == "airlines":
                    load_factor = next((m for m in recent_metrics.values() if "load_factor" in m.get("metric", "").lower()), None)
                    if load_factor:
                        efficiency = "excellent" if load_factor["value"] > 85 else "good" if load_factor["value"] > 80 else "needs improvement"
                        return f"Load factor of {load_factor['value']:.1f}% indicates {efficiency} operational efficiency"
            
            return f"{concept} metrics available for analysis"

    def _generate_industry_insights(self, metrics: List[Dict], industry: str) -> List[Dict]:
        """
        Generate industry-specific insights
        """
        insights = []
        
        if industry == "airlines":
            # Fleet analysis
            fleet_metrics = [m for m in metrics if "fleet" in m.get("metric", "").lower()]
            if fleet_metrics:
                fleet_size = fleet_metrics[0]["value"]
                insights.append({
                    "concept": "Fleet Management",
                    "insight": f"Operating fleet of {fleet_size:.0f} aircraft indicates {'large-scale' if fleet_size > 400 else 'mid-size'} airline operations",
                    "supporting_metrics": ["fleet_size"],
                    "confidence": 0.90
                })
        
        elif industry == "banking":
            # Branch network analysis
            branch_metrics = [m for m in metrics if "branch" in m.get("metric", "").lower()]
            if branch_metrics:
                branches = branch_metrics[0]["value"]
                insights.append({
                    "concept": "Physical Presence",
                    "insight": f"Network of {branches:.0f} branches indicates {'extensive' if branches > 500 else 'moderate'} physical presence",
                    "supporting_metrics": ["number_of_branches"],
                    "confidence": 0.85
                })
        
        return insights

    def _store_comprehensive_results(self, pdf_path: str, industry_analysis: Dict, processing_plan: Dict, 
                                   results: Dict, business_insights: List[Dict], processing_time: float) -> int:
        """
        Store comprehensive results with industry intelligence
        """
        print("💾 Storing comprehensive results...")
        
        # Store company
        company_info = {
            "company_name": industry_analysis["company_name"],
            "sector": industry_analysis["detected_industry"]
        }
        company_id = self._store_company(company_info)
        
        # Store document
        cursor = self.db.cursor()
        cursor.execute("""
            INSERT INTO documents 
            (company_id, filename, total_pages, pages_analyzed, pages_processed, processing_time, toc_pages, financial_sections, processing_strategy) 
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            company_id,
            Path(pdf_path).name,
            industry_analysis["total_pages"],
            len(processing_plan["pages_to_process"]),
            len(results.get("processed_pages", [])),
            processing_time,
            json.dumps([]),
            json.dumps(processing_plan["target_metrics"]),
            f"industry_intelligent_{processing_plan['strategy']}"
        ))
        
        document_id = cursor.lastrowid
        
        # Store industry analysis
        cursor.execute("""
            INSERT INTO industry_analysis 
            (document_id, detected_industry, industry_confidence, target_metrics, extraction_strategy)
            VALUES (?, ?, ?, ?, ?)
        """, (
            document_id,
            industry_analysis["detected_industry"],
            0.85,
            json.dumps(processing_plan["target_metrics"]),
            processing_plan["strategy"]
        ))
        
        # Store metrics
        if results.get("metrics"):
            metrics_data = []
            for m in results["metrics"]:
                metrics_data.append((
                    document_id, m["page_number"], m["metric"], m["value"],
                    m.get("unit", "unknown"), m.get("period", "unknown"),
                    m["confidence"], m["extraction_method"], m.get("source_text", "")
                ))
            
            cursor.executemany("""
                INSERT INTO financial_metrics 
                (document_id, page_number, metric_name, value, unit, period, confidence, extraction_method, source_text) 
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, metrics_data)
        
        # Store business insights
        for insight in business_insights:
            cursor.execute("""
                INSERT INTO business_intelligence 
                (document_id, universal_concept, insight_text, supporting_metrics, confidence)
                VALUES (?, ?, ?, ?, ?)
            """, (
                document_id,
                insight["concept"],
                insight["insight"],
                json.dumps(insight["supporting_metrics"]),
                insight["confidence"]
            ))
        
        self.db.commit()
        print(f"  ✅ Comprehensive results stored for document ID {document_id}")
        return document_id

    def _store_company(self, company_info: Dict) -> int:
        """Store company information"""
        cursor = self.db.cursor()
        name = company_info.get("company_name", "Unknown")
        sector = company_info.get("sector", "other")
        
        cursor.execute("SELECT id FROM companies WHERE name = ?", (name,))
        result = cursor.fetchone()
        
        if result:
            return result[0]
        
        cursor.execute("INSERT INTO companies (name, sector, sector_confidence) VALUES (?, ?, ?)", (name, sector, 0.85))
        self.db.commit()
        return cursor.lastrowid

    def query_company_intelligence(self, document_id: int) -> Dict:
        """
        Query comprehensive company intelligence
        """
        # Get company and industry info
        company_info = self.db.execute("""
            SELECT c.name, c.sector, ia.detected_industry, ia.target_metrics
            FROM companies c
            JOIN documents d ON c.id = d.company_id
            JOIN industry_analysis ia ON d.id = ia.document_id
            WHERE d.id = ?
        """, (document_id,)).fetchone()
        
        if not company_info:
            return {"error": "Document not found"}
        
        company_name, sector, detected_industry, target_metrics = company_info
        target_metrics = json.loads(target_metrics) if target_metrics else []
        
        # Get all metrics
        metrics = self.db.execute("""
            SELECT metric_name, value, unit, period, confidence, extraction_method
            FROM financial_metrics
            WHERE document_id = ?
            ORDER BY confidence DESC
        """, (document_id,)).fetchall()
        
        # Get business insights
        insights = self.db.execute("""
            SELECT universal_concept, insight_text, supporting_metrics, confidence
            FROM business_intelligence
            WHERE document_id = ?
            ORDER BY confidence DESC
        """, (document_id,)).fetchall()
        
        # Organize metrics by type
        universal_metrics = {}
        industry_metrics = {}
        
        for metric, value, unit, period, confidence, method in metrics:
            metric_data = {
                "value": value,
                "unit": unit,
                "period": period,
                "confidence": confidence,
                "method": method
            }
            
            if any(universal in metric.lower() for universal in self.knowledge_base.universal_metrics.keys()):
                universal_metrics[metric] = metric_data
            else:
                industry_metrics[metric] = metric_data
        
        # Format insights
        formatted_insights = []
        for concept, insight_text, supporting_metrics, confidence in insights:
            formatted_insights.append({
                "concept": concept,
                "insight": insight_text,
                "supporting_metrics": json.loads(supporting_metrics) if supporting_metrics else [],
                "confidence": confidence
            })
        
        return {
            "company_profile": {
                "name": company_name,
                "detected_industry": detected_industry,
                "sector": sector,
                "target_metrics_found": len(metrics),
                "total_target_metrics": len(target_metrics)
            },
            "universal_metrics": universal_metrics,
            "industry_specific_metrics": industry_metrics,
            "business_intelligence": formatted_insights,
            "coverage_analysis": {
                "universal_coverage": len(universal_metrics),
                "industry_coverage": len(industry_metrics),
                "total_metrics": len(metrics)
            }
        }
