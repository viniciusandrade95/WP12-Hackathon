# Dual-Agent Evidence Chain Verification System
"""
Merges dual-agent verification with evidence chain tracking
for robust financial data extraction and validation
"""

from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple
from enum import Enum
import json

class VerificationStatus(Enum):
    VERIFIED = "verified"
    DISPUTED = "disputed"
    UNCERTAIN = "uncertain"
    REJECTED = "rejected"

@dataclass
class Evidence:
    """Structured evidence for a metric extraction"""
    source_quote: str           # Exact text from document
    page_number: int           # Location in document
    confidence: float          # Agent's confidence (0-1)
    reasoning: str             # Why agent believes this is correct
    assumptions: List[str]     # Any assumptions made
    context_window: str        # Surrounding text for context

@dataclass
class MetricClaim:
    """A claim about a metric with full evidence chain"""
    metric_name: str
    value: float
    unit: str
    period: str
    evidence: Evidence
    extraction_method: str
    agent_id: str

@dataclass
class VerificationResult:
    """Result of dual-agent verification process"""
    original_claim: MetricClaim
    verification_claim: Optional[MetricClaim]
    status: VerificationStatus
    consensus_value: Optional[float]
    confidence_score: float
    conflict_analysis: Optional[str]
    resolution_reasoning: str

class ExtractionAgent:
    """Primary extraction agent with evidence chain requirements"""
    
    def __init__(self, llm_client, agent_id: str):
        self.llm_client = llm_client
        self.agent_id = agent_id
    
    def extract_with_evidence(self, text: str, page_num: int, industry: str) -> List[MetricClaim]:
        """Extract metrics with mandatory evidence chain"""
        
        prompt = self._create_evidence_based_prompt(industry)
        response = self.llm_client.extract_metrics(text, page_num, prompt, 120, industry)
        
        claims = []
        for raw_metric in response:
            # Validate that evidence is provided
            if not self._has_valid_evidence(raw_metric):
                continue
            
            claim = MetricClaim(
                metric_name=raw_metric["metric_name"],
                value=raw_metric["value"],
                unit=raw_metric["unit"],
                period=raw_metric["period"],
                evidence=Evidence(
                    source_quote=raw_metric["source_quote"],
                    page_number=page_num,
                    confidence=raw_metric["confidence"],
                    reasoning=raw_metric["reasoning"],
                    assumptions=raw_metric.get("assumptions", []),
                    context_window=raw_metric.get("context_window", "")
                ),
                extraction_method="llm_evidence_extraction",
                agent_id=self.agent_id
            )
            claims.append(claim)
        
        return claims
    
    def _create_evidence_based_prompt(self, industry: str) -> str:
        """Create prompt that mandates evidence chain"""
        return f"""
Extract {industry} metrics with MANDATORY evidence chain. For each metric, you MUST provide:

REQUIRED RESPONSE FORMAT (JSON):
{{
    "metric_name": "exact_name",
    "value": numeric_value,
    "unit": "unit_type",
    "period": "time_period",
    "source_quote": "EXACT text from document containing this number",
    "confidence": 0.0_to_1.0,
    "reasoning": "Why you believe this extraction is correct",
    "assumptions": ["list", "of", "assumptions"],
    "context_window": "surrounding text for context"
}}

CRITICAL RULES:
1. source_quote MUST be exact text from document (copy-paste)
2. If you cannot find exact source text, DO NOT extract the metric
3. reasoning must explain your interpretation step-by-step
4. confidence must reflect certainty of extraction accuracy
5. Include context_window showing text around the metric

REJECT if:
- No clear source text
- Ambiguous numbers
- Uncertain about meaning

Extract industry-specific metrics for {industry}: [relevant metrics here]
"""
    
    def _has_valid_evidence(self, raw_metric: dict) -> bool:
        """Validate that extraction includes proper evidence"""
        required_fields = ["source_quote", "confidence", "reasoning"]
        return all(
            field in raw_metric and 
            raw_metric[field] and 
            len(str(raw_metric[field]).strip()) > 10
            for field in required_fields
        )

class VerificationAgent:
    """Independent verification agent that challenges extraction claims"""
    
    def __init__(self, llm_client, agent_id: str):
        self.llm_client = llm_client
        self.agent_id = agent_id
    
    def verify_claim(self, claim: MetricClaim, full_text: str) -> MetricClaim:
        """Independently verify a metric claim with evidence"""
        
        verification_prompt = f"""
VERIFICATION TASK: Independently verify this metric claim.

CLAIM TO VERIFY:
- Metric: {claim.metric_name}
- Value: {claim.value} {claim.unit}
- Period: {claim.period}
- Original Source Quote: "{claim.evidence.source_quote}"
- Original Reasoning: "{claim.evidence.reasoning}"

YOUR TASK:
1. Search the provided text for this metric independently
2. Find your own source quote and evidence
3. Determine if the original claim is accurate

RESPONSE FORMAT (JSON):
{{
    "verification_status": "verified|disputed|uncertain",
    "your_source_quote": "your exact text found",
    "your_value": numeric_value_you_found,
    "your_confidence": 0.0_to_1.0,
    "your_reasoning": "your independent analysis",
    "agreement_analysis": "comparison with original claim",
    "conflict_points": ["specific disagreements if any"]
}}

TEXT TO SEARCH:
{full_text[:8000]}

Be thorough and independent. Challenge the original claim rigorously.
"""
        
        response = self.llm_client.extract_metrics(
            full_text, claim.evidence.page_number, verification_prompt, 90, "verification"
        )
        
        if not response:
            return None
        
        verification_data = response[0]  # Assume single verification response
        
        return MetricClaim(
            metric_name=claim.metric_name,
            value=verification_data.get("your_value", claim.value),
            unit=claim.unit,
            period=claim.period,
            evidence=Evidence(
                source_quote=verification_data.get("your_source_quote", ""),
                page_number=claim.evidence.page_number,
                confidence=verification_data.get("your_confidence", 0.0),
                reasoning=verification_data.get("your_reasoning", ""),
                assumptions=verification_data.get("conflict_points", []),
                context_window=""
            ),
            extraction_method="verification_check",
            agent_id=self.agent_id
        )

class ConsensusEngine:
    """Resolves conflicts between extraction and verification agents"""
    
    def resolve_conflict(self, original: MetricClaim, verification: Optional[MetricClaim]) -> VerificationResult:
        """Resolve conflicts between dual agents using evidence strength"""
        
        if not verification:
            return VerificationResult(
                original_claim=original,
                verification_claim=None,
                status=VerificationStatus.UNCERTAIN,
                consensus_value=original.value,
                confidence_score=original.evidence.confidence * 0.5,  # Penalty for no verification
                conflict_analysis="Verification failed",
                resolution_reasoning="Using original claim with reduced confidence"
            )
        
        # Calculate agreement metrics
        value_agreement = self._calculate_value_agreement(original.value, verification.value)
        evidence_strength = self._evaluate_evidence_strength(original.evidence, verification.evidence)
        
        if value_agreement > 0.95 and evidence_strength["both_strong"]:
            return VerificationResult(
                original_claim=original,
                verification_claim=verification,
                status=VerificationStatus.VERIFIED,
                consensus_value=original.value,
                confidence_score=min(original.evidence.confidence, verification.evidence.confidence),
                conflict_analysis="Strong agreement between agents",
                resolution_reasoning="Both agents found consistent evidence"
            )
        
        elif value_agreement < 0.8 or evidence_strength["conflict"]:
            return VerificationResult(
                original_claim=original,
                verification_claim=verification,
                status=VerificationStatus.DISPUTED,
                consensus_value=None,
                confidence_score=0.3,
                conflict_analysis=f"Value agreement: {value_agreement:.2%}, Evidence conflict detected",
                resolution_reasoning="Significant disagreement between agents - manual review required"
            )
        
        else:
            # Weighted consensus based on evidence quality
            consensus_value = self._weighted_consensus(original, verification, evidence_strength)
            return VerificationResult(
                original_claim=original,
                verification_claim=verification,
                status=VerificationStatus.UNCERTAIN,
                consensus_value=consensus_value,
                confidence_score=0.6,
                conflict_analysis="Moderate agreement with some uncertainty",
                resolution_reasoning="Using weighted consensus of both agents"
            )
    
    def _calculate_value_agreement(self, val1: float, val2: float) -> float:
        """Calculate agreement between two values"""
        if val1 == 0 and val2 == 0:
            return 1.0
        if val1 == 0 or val2 == 0:
            return 0.0
        
        ratio = min(val1, val2) / max(val1, val2)
        return ratio
    
    def _evaluate_evidence_strength(self, ev1: Evidence, ev2: Evidence) -> Dict[str, bool]:
        """Evaluate strength and consistency of evidence"""
        return {
            "both_strong": ev1.confidence > 0.7 and ev2.confidence > 0.7,
            "source_overlap": len(set(ev1.source_quote.split()) & set(ev2.source_quote.split())) > 3,
            "conflict": ev1.confidence > 0.8 and ev2.confidence > 0.8 and 
                       len(set(ev1.source_quote.split()) & set(ev2.source_quote.split())) < 2
        }
    
    def _weighted_consensus(self, original: MetricClaim, verification: MetricClaim, 
                           evidence_strength: Dict[str, bool]) -> float:
        """Calculate weighted consensus value based on evidence strength"""
        w1 = original.evidence.confidence
        w2 = verification.evidence.confidence
        
        if evidence_strength["source_overlap"]:
            # Boost confidence if sources overlap
            w1 *= 1.2
            w2 *= 1.2
        
        total_weight = w1 + w2
        if total_weight == 0:
            return (original.value + verification.value) / 2
        
        return (original.value * w1 + verification.value * w2) / total_weight

class DualAgentVerificationSystem:
    """Main system orchestrating dual-agent evidence chain verification"""
    
    def __init__(self, llm_client):
        self.extraction_agent = ExtractionAgent(llm_client, "extractor_v1")
        self.verification_agent = VerificationAgent(llm_client, "verifier_v1")
        self.consensus_engine = ConsensusEngine()
        self.verification_log = []
    
    def extract_and_verify(self, text: str, page_num: int, industry: str) -> List[VerificationResult]:
        """Complete extraction and verification pipeline"""
        
        # Phase 1: Primary extraction with evidence chain
        print(f"🔍 Phase 1: Primary extraction (page {page_num})")
        original_claims = self.extraction_agent.extract_with_evidence(text, page_num, industry)
        
        if not original_claims:
            print("   No claims extracted")
            return []
        
        print(f"   Extracted {len(original_claims)} claims")
        
        # Phase 2: Independent verification
        print("🔍 Phase 2: Independent verification")
        verification_results = []
        
        for claim in original_claims:
            print(f"   Verifying: {claim.metric_name}")
            
            verification_claim = self.verification_agent.verify_claim(claim, text)
            result = self.consensus_engine.resolve_conflict(claim, verification_claim)
            
            verification_results.append(result)
            
            # Log for analysis
            self.verification_log.append({
                "page": page_num,
                "metric": claim.metric_name,
                "status": result.status.value,
                "confidence": result.confidence_score,
                "original_value": claim.value,
                "consensus_value": result.consensus_value
            })
            
            print(f"     Status: {result.status.value} (confidence: {result.confidence_score:.2f})")
        
        return verification_results
    
    def get_verification_summary(self) -> Dict:
        """Get summary statistics of verification process"""
        if not self.verification_log:
            return {}
        
        total = len(self.verification_log)
        verified = sum(1 for log in self.verification_log if log["status"] == "verified")
        disputed = sum(1 for log in self.verification_log if log["status"] == "disputed")
        
        avg_confidence = sum(log["confidence"] for log in self.verification_log) / total
        
        return {
            "total_extractions": total,
            "verified_count": verified,
            "disputed_count": disputed,
            "verification_rate": verified / total if total > 0 else 0,
            "average_confidence": avg_confidence,
            "requires_review": disputed
        }

# Example usage
if __name__ == "__main__":
    # Initialize system
    from utils.api_client import LLMClient
    llm_client = LLMClient("api-key", "base-url")
    verification_system = DualAgentVerificationSystem(llm_client)
    
    # Process document with dual-agent verification
    sample_text = "Total revenue for 2024 was €2.4 billion, representing growth of 15%..."
    results = verification_system.extract_and_verify(sample_text, page_num=1, industry="airlines")
    
    # Review results
    for result in results:
        if result.status == VerificationStatus.VERIFIED:
            print(f"✅ {result.original_claim.metric_name}: {result.consensus_value}")
        elif result.status == VerificationStatus.DISPUTED:
            print(f"⚠️  {result.original_claim.metric_name}: Needs review - {result.conflict_analysis}")
    
    # Get summary
    summary = verification_system.get_verification_summary()
    print(f"Verification rate: {summary['verification_rate']:.1%}")