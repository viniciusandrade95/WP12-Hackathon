# test_fixed_complete.py
"""
COMPLETE FIXED TEST with patterns that work for your data format
"""

import re
from typing import Dict, List

def extract_with_regex_fixed(text: str, page_num: int) -> List[Dict]:
    """
    FIXED regex extraction specifically for your data format
    """
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
        # More flexible pattern to catch variations
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
    # Look for lines like "Some Metric    1,234"
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

def test_complete_fixed():
    """Complete test with fixed patterns"""
    
    sample_text = """
    Income Statement
    Mar 31, 2025   Mar 31, 2024   Mar 31, 2023
    €'m            €'m            €'m
    Scheduled Revenue      9,230      9,145      6,930
    Ancillary Revenue      4,719      4,299      3,845
    Total Revenue         13,949     13,444     10,775
    Fuel                   5,220      5,143      4,026
    Ex-Fuel Costs          7,171      6,240      5,306
    Total Operating Costs 12,391     11,383      9,332
    Net Finance and Other Income  224   62       (34)
    Foreign Exchange           3        5        34
    Profit Before Tax      1,785      2,128      1,443
    Tax Expense            (173)      (211)      (129)
    Profit After Tax       1,612      1,917      1,314
    
    Balance Sheet
    Mar 31, 2025   Mar 31, 2024   Mar 31, 2023
    €'m            €'m            €'m
    Non-Current Assets    11,497     11,349     10,494
    Gross Cash             3,987      4,120      4,675
    Current Assets         2,023      1,707      1,237
    Total Assets          17,507     17,176     16,406
    Current Liabilities    8,153      6,401      7,422
    Non-Current Liabilities 2,317     3,161      3,341
    Shareholder Equity     7,037      7,614      5,643
    Total Liabilities & Equity 17,507 17,176    16,406
    Net Cash               1,304      1,373        559
    """
    
    print("🧪 TESTING COMPLETE FIXED EXTRACTION")
    print("=" * 60)
    
    # Test the extraction
    metrics = extract_with_regex_fixed(sample_text, 1)
    
    print(f"\n📊 EXTRACTION RESULTS:")
    print(f"   Total metrics found: {len(metrics)}")
    
    # Show all found metrics
    print(f"\n📋 ALL FOUND METRICS:")
    for i, metric in enumerate(metrics, 1):
        print(f"   {i:2d}. {metric['metric']}: {metric['value']:,.0f} {metric['unit']}")
    
    # Check for key metrics
    expected = {
        'Total Revenue': 13949,
        'Profit After Tax': 1612,
        'Total Assets': 17507,
        'Shareholder Equity': 7037,
        'Net Cash': 1304
    }
    
    print(f"\n🎯 KEY METRICS VALIDATION:")
    found_count = 0
    for exp_name, exp_value in expected.items():
        found = False
        for metric in metrics:
            if exp_name.lower() in metric['metric'].lower() and abs(metric['value'] - exp_value) < 10:
                print(f"   ✅ {exp_name}: {metric['value']:,.0f} (expected {exp_value:,})")
                found = True
                found_count += 1
                break
        
        if not found:
            print(f"   ❌ {exp_name}: NOT FOUND (expected {exp_value:,})")
            
            # Debug: show similar metrics
            similar = [m for m in metrics if any(word in m['metric'].lower() 
                      for word in exp_name.lower().split())]
            if similar:
                print(f"      🔍 Similar found: {[m['metric'] for m in similar]}")
    
    print(f"\n📈 SUCCESS SCORE: {found_count}/{len(expected)} key metrics found")
    
    # Determine result
    if found_count >= 4:
        status = "PASSED"
        emoji = "✅"
        action = "Ready to use! Replace your broken files."
    elif found_count >= 2:
        status = "PARTIAL PASS"  
        emoji = "⚠️"
        action = "Should work, may need minor tweaks."
    else:
        status = "FAILED"
        emoji = "❌"
        action = "Needs more debugging."
    
    print(f"\n{'='*60}")
    print(f"🏁 FINAL RESULT")
    print(f"{'='*60}")
    print(f"{emoji} STATUS: {status}")
    print(f"📊 FOUND: {found_count}/5 core metrics")
    print(f"📋 ACTION: {action}")
    
    if found_count >= 2:
        print(f"\n🚀 NEXT STEPS:")
        print(f"   1. The fixed regex patterns work!")
        print(f"   2. Copy these patterns into your document processor")
        print(f"   3. Replace the _extract_with_regex method")
        print(f"   4. Test with your actual PDF")
        return True
    else:
        print(f"\n🔧 DEBUG INFO:")
        print(f"   - Check if your PDF text format matches the sample")
        print(f"   - The patterns may need adjustment for your specific data")
        return False

if __name__ == "__main__":
    success = test_complete_fixed()
    exit(0 if success else 1)