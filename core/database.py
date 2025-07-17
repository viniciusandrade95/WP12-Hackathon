"""
Database operations for intelligent RAG system
core/database.py
"""

import sqlite3
import json
from pathlib import Path
from typing import Dict, List, Optional
from datetime import datetime

class DatabaseManager:
    """
    Manages all database operations for the intelligent RAG system
    """
    
    def __init__(self, db_path: str = "intelligent_rag.db"):
        self.db_path = db_path
        self.db = sqlite3.connect(db_path, check_same_thread=False)
        self._setup_database()
    
    def _setup_database(self):
        """
        Create all necessary tables for the enhanced system
        """
        cursor = self.db.cursor()
        
        # Companies table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS companies (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT UNIQUE NOT NULL,
                sector TEXT,
                detected_industry TEXT,
                industry_confidence REAL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Documents table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS documents (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                company_id INTEGER,
                filename TEXT NOT NULL,
                total_pages INTEGER,
                pages_analyzed INTEGER,
                pages_processed INTEGER,
                processing_time REAL,
                processing_strategy TEXT,
                extraction_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (company_id) REFERENCES companies (id)
            )
        """)
        
        # Industry analysis table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS industry_analysis (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                document_id INTEGER,
                detected_industry TEXT,
                industry_confidence REAL,
                target_metrics TEXT,  -- JSON array of target metrics
                layout_analysis TEXT,  -- JSON object of layout analysis
                extraction_strategy TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (document_id) REFERENCES documents (id)
            )
        """)
        
        # Financial metrics table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS financial_metrics (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                document_id INTEGER,
                page_number INTEGER,
                metric_name TEXT NOT NULL,
                metric_type TEXT,  -- 'universal', 'industry_specific', 'other'
                value REAL NOT NULL,
                unit TEXT,
                period TEXT,
                confidence REAL,
                extraction_method TEXT,
                source_text TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (document_id) REFERENCES documents (id)
            )
        """)
        
        # Business intelligence table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS business_intelligence (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                document_id INTEGER,
                concept TEXT NOT NULL,  -- 'Financial Health', 'Operational Efficiency', etc.
                insight_text TEXT NOT NULL,
                supporting_metrics TEXT,  -- JSON array of metric names
                confidence REAL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (document_id) REFERENCES documents (id)
            )
        """)
        
        # Page metadata table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS page_metadata (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                document_id INTEGER,
                page_number INTEGER,
                page_type TEXT,
                priority_score REAL,
                contains_tables BOOLEAN,
                industry_relevance_score REAL,
                processed BOOLEAN DEFAULT FALSE,
                FOREIGN KEY (document_id) REFERENCES documents (id)
            )
        """)
        
        # Processing performance table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS processing_performance (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                document_id INTEGER,
                method_name TEXT,
                attempts INTEGER,
                successes INTEGER,
                success_rate REAL,
                avg_processing_time REAL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (document_id) REFERENCES documents (id)
            )
        """)
        
        self.db.commit()
        print("✅ Database initialized successfully")
    
    def store_company(self, company_info: Dict) -> int:
        """
        Store or update company information
        """
        cursor = self.db.cursor()
        
        company_name = company_info.get("company_name", "Unknown")
        sector = company_info.get("sector", "other")
        detected_industry = company_info.get("detected_industry", {})
        
        industry_name = detected_industry.get("industry", "other") if isinstance(detected_industry, dict) else str(detected_industry)
        industry_confidence = detected_industry.get("confidence", 0.0) if isinstance(detected_industry, dict) else 0.0
        
        # Check if company exists
        cursor.execute("SELECT id FROM companies WHERE name = ?", (company_name,))
        result = cursor.fetchone()
        
        if result:
            # Update existing company
            company_id = result[0]
            cursor.execute("""
                UPDATE companies 
                SET sector = ?, detected_industry = ?, industry_confidence = ?
                WHERE id = ?
            """, (sector, industry_name, industry_confidence, company_id))
        else:
            # Insert new company
            cursor.execute("""
                INSERT INTO companies (name, sector, detected_industry, industry_confidence)
                VALUES (?, ?, ?, ?)
            """, (company_name, sector, industry_name, industry_confidence))
            company_id = cursor.lastrowid
        
        self.db.commit()
        return company_id
    
    def store_document(self, company_id: int, filename: str, processing_info: Dict) -> int:
        """
        Store document processing information
        """
        cursor = self.db.cursor()
        
        cursor.execute("""
            INSERT INTO documents 
            (company_id, filename, total_pages, pages_analyzed, pages_processed, 
             processing_time, processing_strategy)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            company_id,
            filename,
            processing_info.get("total_pages", 0),
            processing_info.get("pages_analyzed", 0),
            processing_info.get("pages_processed", 0),
            processing_info.get("processing_time", 0.0),
            processing_info.get("strategy", "unknown")
        ))
        
        document_id = cursor.lastrowid
        self.db.commit()
        return document_id
    
    def store_industry_analysis(self, document_id: int, industry_analysis: Dict) -> int:
        """
        Store industry analysis results
        """
        cursor = self.db.cursor()
        
        detected_industry = industry_analysis.get("detected_industry", {})
        industry_name = detected_industry.get("industry", "other") if isinstance(detected_industry, dict) else str(detected_industry)
        industry_confidence = detected_industry.get("confidence", 0.0) if isinstance(detected_industry, dict) else 0.0
        
        cursor.execute("""
            INSERT INTO industry_analysis 
            (document_id, detected_industry, industry_confidence, target_metrics, 
             layout_analysis, extraction_strategy)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (
            document_id,
            industry_name,
            industry_confidence,
            json.dumps(industry_analysis.get("target_metrics", [])),
            json.dumps(industry_analysis.get("layout_analysis", {})),
            industry_analysis.get("extraction_strategy", "standard")
        ))
        
        analysis_id = cursor.lastrowid
        self.db.commit()
        return analysis_id
    
    def store_metrics(self, document_id: int, metrics: List[Dict]) -> int:
        """
        Store extracted financial metrics
        """
        cursor = self.db.cursor()
        
        metrics_data = []
        for metric in metrics:
            metrics_data.append((
                document_id,
                metric.get("page_number", 0),
                metric.get("metric", "unknown"),
                metric.get("metric_type", "other"),
                float(metric.get("value", 0)),
                metric.get("unit", "unknown"),
                str(metric.get("period", "unknown")),
                float(metric.get("confidence", 0.0)),
                metric.get("extraction_method", "unknown"),
                metric.get("source_text", "")
            ))
        
        if metrics_data:
            cursor.executemany("""
                INSERT INTO financial_metrics 
                (document_id, page_number, metric_name, metric_type, value, unit, 
                 period, confidence, extraction_method, source_text)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, metrics_data)
        
        self.db.commit()
        return len(metrics_data)
    
    def store_business_insights(self, document_id: int, insights: List[Dict]) -> int:
        """
        Store business intelligence insights
        """
        cursor = self.db.cursor()
        
        insights_data = []
        for insight in insights:
            insights_data.append((
                document_id,
                insight.get("concept", "General"),
                insight.get("insight", ""),
                json.dumps(insight.get("supporting_metrics", [])),
                float(insight.get("confidence", 0.0))
            ))
        
        if insights_data:
            cursor.executemany("""
                INSERT INTO business_intelligence 
                (document_id, concept, insight_text, supporting_metrics, confidence)
                VALUES (?, ?, ?, ?, ?)
            """, insights_data)
        
        self.db.commit()
        return len(insights_data)
    
    def store_comprehensive_results(self, pdf_path: str, industry_analysis: Dict, 
                                   processing_plan: Dict, results: Dict, 
                                   insights: List[Dict], processing_time: float) -> int:
        """
        Store all processing results in one transaction
        """
        try:
            self.db.execute("BEGIN TRANSACTION")
            
            # Store company
            company_info = {
                "company_name": industry_analysis.get("company_name", "Unknown"),
                "detected_industry": industry_analysis.get("detected_industry", {}),
                "sector": industry_analysis.get("detected_industry", {}).get("industry", "other")
            }
            company_id = self.store_company(company_info)
            
            # Store document
            processing_info = {
                "total_pages": industry_analysis.get("total_pages", 0),
                "pages_analyzed": len(processing_plan.get("pages_to_process", [])),
                "pages_processed": len(results.get("processed_pages", [])),
                "processing_time": processing_time,
                "strategy": processing_plan.get("strategy", "unknown")
            }
            document_id = self.store_document(company_id, Path(pdf_path).name, processing_info)
            
            # Store industry analysis
            self.store_industry_analysis(document_id, industry_analysis)
            
            # Store metrics
            if results.get("metrics"):
                self.store_metrics(document_id, results["metrics"])
            
            # Store business insights
            if insights:
                self.store_business_insights(document_id, insights)
            
            self.db.execute("COMMIT")
            print(f"✅ Comprehensive results stored for document ID {document_id}")
            return document_id
            
        except Exception as e:
            self.db.execute("ROLLBACK")
            print(f"❌ Error storing results: {e}")
            raise
    
    def get_company_intelligence(self, document_id: int) -> Dict:
        """
        Get comprehensive company intelligence for a document
        """
        cursor = self.db.cursor()
        
        # Get company and document info
        cursor.execute("""
            SELECT c.name, c.detected_industry, c.industry_confidence,
                   d.filename, d.total_pages, d.pages_processed, d.processing_time,
                   ia.target_metrics, ia.layout_analysis
            FROM companies c
            JOIN documents d ON c.id = d.company_id
            LEFT JOIN industry_analysis ia ON d.id = ia.document_id
            WHERE d.id = ?
        """, (document_id,))
        
        company_data = cursor.fetchone()
        if not company_data:
            return {"error": "Document not found"}
        
        (company_name, detected_industry, industry_confidence, filename, 
         total_pages, pages_processed, processing_time, target_metrics, layout_analysis) = company_data
        
        # Get metrics grouped by type
        cursor.execute("""
            SELECT metric_name, metric_type, value, unit, period, confidence, extraction_method
            FROM financial_metrics
            WHERE document_id = ?
            ORDER BY confidence DESC, metric_type, metric_name
        """, (document_id,))
        
        metrics_data = cursor.fetchall()
        
        # Get business insights
        cursor.execute("""
            SELECT concept, insight_text, supporting_metrics, confidence
            FROM business_intelligence
            WHERE document_id = ?
            ORDER BY confidence DESC
        """, (document_id,))
        
        insights_data = cursor.fetchall()
        
        # Organize metrics by type
        universal_metrics = {}
        industry_metrics = {}
        other_metrics = {}
        
        for metric_name, metric_type, value, unit, period, confidence, method in metrics_data:
            metric_info = {
                "value": value,
                "unit": unit,
                "period": period,
                "confidence": confidence,
                "method": method
            }
            
            if metric_type == "universal":
                universal_metrics[metric_name] = metric_info
            elif metric_type == "industry_specific":
                industry_metrics[metric_name] = metric_info
            else:
                other_metrics[metric_name] = metric_info
        
        # Format insights
        formatted_insights = []
        for concept, insight_text, supporting_metrics, confidence in insights_data:
            formatted_insights.append({
                "concept": concept,
                "insight": insight_text,
                "supporting_metrics": json.loads(supporting_metrics) if supporting_metrics else [],
                "confidence": confidence
            })
        
        # Compile comprehensive intelligence
        intelligence = {
            "company_profile": {
                "name": company_name,
                "detected_industry": detected_industry or "other",
                "industry_confidence": industry_confidence or 0.0,
                "filename": filename,
                "total_pages": total_pages,
                "pages_processed": pages_processed,
                "processing_time": processing_time
            },
            "universal_metrics": universal_metrics,
            "industry_specific_metrics": industry_metrics,
            "other_metrics": other_metrics,
            "business_intelligence": formatted_insights,
            "coverage_analysis": {
                "total_metrics": len(metrics_data),
                "universal_coverage": len(universal_metrics),
                "industry_coverage": len(industry_metrics),
                "other_coverage": len(other_metrics),
                "insights_generated": len(formatted_insights