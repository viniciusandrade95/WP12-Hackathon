"""
GICS-based industry knowledge base for intelligent document analysis
"""

class GICSKnowledgeBase:
    def __init__(self):
        self.universal_metrics = {
            "total_revenue": ["total sales", "net sales", "turnover", "revenue"],
            "operating_costs": ["operating expenses", "cost of goods sold", "cogs"],
            "net_income": ["profit for the year", "profit after tax", "earnings"],
            "employee_count": ["number of employees", "headcount", "ftes"],
            "total_assets": ["total assets"],
            "cash_flow_from_operations": ["operating cash flow"]
        }
        
        self.industry_schemas = {
            "airlines": {
                "display_name": "Airlines",
                "sector": "Industrials",
                "key_metrics": {
                    "fleet_size": {
                        "synonyms": ["number of aircraft", "fleet size", "aircraft fleet"],
                        "unit": "aircraft",
                        "importance": "high",
                        "description": "Total number of aircraft in operation"
                    },
                    "passengers_carried": {
                        "synonyms": ["passengers carried", "passenger numbers", "passenger traffic"],
                        "unit": "millions",
                        "importance": "high",
                        "description": "Total passengers transported annually"
                    },
                    "load_factor": {
                        "synonyms": ["load factor", "passenger load factor", "seat load factor"],
                        "unit": "percentage",
                        "importance": "critical",
                        "description": "Percentage of available seats filled"
                    },
                    "ancillary_revenue": {
                        "synonyms": ["ancillary revenue", "non-ticket revenue"],
                        "unit": "millions_eur",
                        "importance": "medium",
                        "description": "Revenue from non-flight services"
                    }
                },
                "business_questions": [
                    "How efficiently do they operate their fleet?",
                    "Are their planes full?",
                    "What's their cost structure per passenger?"
                ]
            },
            "banking": {
                "display_name": "Banking",
                "sector": "Financials",
                "key_metrics": {
                    "net_interest_margin": {
                        "synonyms": ["net interest margin", "nim"],
                        "unit": "percentage",
                        "importance": "critical",
                        "description": "Profitability of lending operations"
                    },
                    "number_of_branches": {
                        "synonyms": ["number of branches", "branch network", "agencies"],
                        "unit": "count",
                        "importance": "high",
                        "description": "Physical presence and reach"
                    },
                    "deposits": {
                        "synonyms": ["customer deposits", "total deposits", "deposit base"],
                        "unit": "millions_eur",
                        "importance": "high",
                        "description": "Customer funds held by the bank"
                    },
                    "loan_portfolio": {
                        "synonyms": ["loan portfolio", "total loans", "advances"],
                        "unit": "millions_eur",
                        "importance": "high",
                        "description": "Total loans outstanding"
                    }
                },
                "business_questions": [
                    "How profitable is their lending business?",
                    "What's their market presence?",
                    "How well capitalized are they?"
                ]
            },
            "technology": {
                "display_name": "Technology",
                "sector": "Information Technology",
                "key_metrics": {
                    "annual_recurring_revenue": {
                        "synonyms": ["annual recurring revenue", "arr"],
                        "unit": "millions_eur",
                        "importance": "critical",
                        "description": "Predictable yearly subscription revenue"
                    },
                    "active_users": {
                        "synonyms": ["active users", "monthly active users", "user base"],
                        "unit": "millions",
                        "importance": "high",
                        "description": "Number of engaged users"
                    },
                    "churn_rate": {
                        "synonyms": ["churn rate", "customer churn", "attrition"],
                        "unit": "percentage",
                        "importance": "critical",
                        "description": "Rate of customer loss"
                    }
                },
                "business_questions": [
                    "Is their recurring revenue growing?",
                    "Are they retaining customers?",
                    "What's their growth trajectory?"
                ]
            }
        }
    
    def detect_industry(self, text: str, company_name: str = "") -> dict:
        """Detect industry with confidence scoring"""
        text_lower = text.lower()
        company_lower = company_name.lower()
        
        industry_indicators = {
            "airlines": ["aircraft", "flights", "passengers", "aviation", "airline", "fleet"],
            "banking": ["deposits", "loans", "branches", "bank", "credit", "capital"],
            "technology": ["software", "saas", "users", "platform", "digital", "cloud"]
        }
        
        scores = {}
        for industry, keywords in industry_indicators.items():
            score = sum(text_lower.count(keyword) for keyword in keywords)
            score += sum(company_lower.count(keyword) * 2 for keyword in keywords)
            scores[industry] = score
        
        if not scores or max(scores.values()) < 3:
            return {"industry": "other", "confidence": 0.0}
        
        detected = max(scores, key=scores.get)
        confidence = min(scores[detected] / 10, 1.0)
        
        return {"industry": detected, "confidence": confidence}
    
    def get_industry_info(self, industry: str) -> dict:
        """Get comprehensive industry information"""
        if industry not in self.industry_schemas:
            return {"metrics": {}, "questions": []}
        
        return self.industry_schemas[industry]
    
    def get_critical_metrics(self, industry: str) -> list:
        """Get the most important metrics for an industry"""
        if industry not in self.industry_schemas:
            return []
        
        metrics = self.industry_schemas[industry]["key_metrics"]
        critical = [name for name, info in metrics.items() 
                   if info["importance"] == "critical"]
        high = [name for name, info in metrics.items() 
                if info["importance"] == "high"]
        
        return critical + high[:3]  # Critical + top 3 high importance
