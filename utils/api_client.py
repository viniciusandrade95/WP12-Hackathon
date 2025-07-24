# utils/api_client_fixed.py
"""
FIXED API Client with robust JSON parsing and fallback handling
"""

import requests
import json
import re
import time
from typing import Dict, List, Optional

class LLMClient:
    """
    Fixed LLM client that actually works reliably
    """
    
    def __init__(self, api_key: str, base_url: str):
        self.api_key = api_key
        self.base_url = base_url
        self.headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json"
        }
    
    def extract_metrics(self, text: str, page_num: int, prompt: str, 
                       timeout: int = 90, context: str = "general") -> List[Dict]:
        """
        Extract metrics with robust error handling and parsing
        """
        try:
            # Make API call
            response = self._make_api_call(text, prompt, timeout)
            if not response:
                return []
            
            # Parse response with multiple strategies
            metrics = self._parse_response_robust(response, page_num)
            
            print(f"        📊 API returned {len(metrics)} metrics")
            return metrics
            
        except Exception as e:
            print(f"        ❌ API call failed: {e}")
            return []
    
    def _make_api_call(self, text: str, prompt: str, timeout: int) -> Optional[str]:
        """Make the actual API call"""
        try:
            data = {
                "model": "mistral-small3.2:latest",
                "messages": [
                    {
                        "role": "system", 
                        "content": "You are a precise financial data extractor. Return ONLY valid JSON arrays."
                    },
                    {
                        "role": "user", 
                        "content": f"{prompt}\n\nText to analyze:\n{text[:4000]}"
                    }
                ],
                "temperature": 0.0,
                "max_tokens": 2000
            }
            
            response = requests.post(
                self.base_url,
                headers=self.headers,
                json=data,
                timeout=timeout
            )
            
            if response.status_code != 200:
                print(f"        ❌ API error {response.status_code}: {response.text}")
                return None
            
            result = response.json()
            content = result['choices'][0]['message']['content'].strip()
            
            return content
            
        except requests.exceptions.Timeout:
            print(f"        ⏰ API timeout after {timeout}s")
            return None
        except Exception as e:
            print(f"        ❌ API request failed: {e}")
            return None
    
    def _parse_response_robust(self, content: str, page_num: int) -> List[Dict]:
        """
        Parse LLM response with multiple fallback strategies
        """
        if not content:
            return []
        
        # Strategy 1: Direct JSON array parsing
        json_array = self._extract_json_array(content)
        if json_array:
            return self._convert_to_standard_format(json_array, page_num)
        
        # Strategy 2: Find JSON objects line by line
        json_objects = self._extract_json_objects(content)
        if json_objects:
            return self._convert_to_standard_format(json_objects, page_num)
        
        # Strategy 3: Regex extraction from text
        regex_metrics = self._extract_from_text_patterns(content, page_num)
        if regex_metrics:
            return regex_metrics
        
        print(f"        ⚠️ No valid data found in response")
        return []
    
    def _extract_json_array(self, content: str) -> Optional[List[Dict]]:
        """Try to extract JSON array from response"""
        try:
            # Find JSON array boundaries
            start = content.find('[')
            end = content.rfind(']')
            
            if start == -1 or end == -1 or start >= end:
                return None
            
            json_str = content[start:end + 1]
            
            # Clean common issues
            json_str = json_str.replace("'", '"')  # Single to double quotes
            json_str = re.sub(r',\s*}', '}', json_str)  # Remove trailing commas in objects
            json_str = re.sub(r',\s*]', ']', json_str)  # Remove trailing commas in arrays
            
            # Parse JSON
            data = json.loads(json_str)
            
            if isinstance(data, list):
                return data
            elif isinstance(data, dict):
                return [data]  # Single object to array
            
        except json.JSONDecodeError as e:
            print(f"        ❌ JSON parsing failed: {e}")
        except Exception as e:
            print(f"        ❌ Array extraction failed: {e}")
        
        return None
    
    def _extract_json_objects(self, content: str) -> List[Dict]:
        """Extract individual JSON objects from lines"""
        objects = []
        
        lines = content.split('\n')
        for line in lines:
            line = line.strip()
            
            # Skip empty lines and non-JSON lines
            if not line or not (line.startswith('{') and line.endswith('}')):
                continue
            
            try:
                # Clean the line
                line = line.replace("'", '"')
                line = re.sub(r',\s*}', '}', line)
                
                obj = json.loads(line)
                if isinstance(obj, dict):
                    objects.append(obj)
                    
            except json.JSONDecodeError:
                continue
        
        return objects
    
    def _extract_from_text_patterns(self, content: str, page_num: int) -> List[Dict]:
        """Extract metrics using regex patterns when JSON fails"""
        metrics = []
        
        # Pattern 1: "Revenue: 13949 million EUR"
        pattern1 = r'([A-Za-z\s]{3,40}):\s*([\d,]+(?:\.\d+)?)\s*(million|billion|thousand)?\s*(EUR|USD|GBP)?'
        matches1 = re.findall(pattern1, content, re.IGNORECASE)
        
        for match in matches1:
            try:
                name = match[0].strip()
                value = float(match[1].replace(',', ''))
                scale = match[2].lower() if match[2] else ''
                currency = match[3] if match[3] else 'EUR'
                
                # Apply scaling
                if scale == 'billion':
                    value *= 1000
                elif scale == 'thousand':
                    value /= 1000
                
                unit = f"{scale} {currency}" if scale else currency
                
                metrics.append({
                    'name': name,
                    'value': value,
                    'unit': unit,
                    'period': '2024'
                })
            except:
                continue
        
        # Pattern 2: Look for numbers followed by descriptions
        pattern2 = r'([\d,]+(?:\.\d+)?)\s*(million|billion)?\s*([A-Za-z\s]{3,40})'
        matches2 = re.findall(pattern2, content, re.IGNORECASE)
        
        for match in matches2:
            try:
                value = float(match[0].replace(',', ''))
                scale = match[1].lower() if match[1] else ''
                name = match[2].strip()
                
                if scale == 'billion':
                    value *= 1000
                elif scale == 'thousand':
                    value /= 1000
                
                unit = f"{scale} EUR" if scale else 'EUR'
                
                metrics.append({
                    'name': name,
                    'value': value,
                    'unit': unit,
                    'period': '2024'
                })
            except:
                continue
        
        return metrics[:10]  # Limit to avoid spam
    
    def _convert_to_standard_format(self, data: List[Dict], page_num: int) -> List[Dict]:
        """Convert various JSON formats to standard format"""
        metrics = []
        
        for item in data:
            if not isinstance(item, dict):
                continue
            
            # Try different field name variations
            name = (item.get('name') or 
                   item.get('metric_name') or 
                   item.get('metric') or 
                   item.get('description', ''))
            
            value = (item.get('value') or 
                    item.get('amount') or 
                    item.get('number') or 0)
            
            # Handle string values
            if isinstance(value, str):
                # Remove currency symbols and commas
                value_clean = re.sub(r'[€$£,]', '', value.strip())
                try:
                    value = float(value_clean)
                except ValueError:
                    continue
            
            # Skip invalid entries
            if not name or not isinstance(value, (int, float)) or value == 0:
                continue
            
            unit = str(item.get('unit', 'unknown'))
            period = str(item.get('period', '2024'))
            
            metrics.append({
                'name': name,
                'value': float(value),
                'unit': unit,
                'period': period
            })
        
        return metrics
    
    def test_extraction(self, sample_text: str) -> Dict:
        """Test the extraction pipeline"""
        print("🧪 Testing extraction pipeline...")
        
        test_prompt = """
        Extract financial metrics from this text.
        Return JSON array like: [{"name": "Revenue", "value": 1000, "unit": "million EUR", "period": "2024"}]
        """
        
        try:
            # Test API call
            response = self._make_api_call(sample_text, test_prompt, 30)
            if not response:
                return {'status': 'api_failed', 'response': None}
            
            # Test parsing
            metrics = self._parse_response_robust(response, 1)
            
            return {
                'status': 'success',
                'raw_response': response[:500],
                'metrics_found': len(metrics),
                'sample_metrics': metrics[:3]
            }
            
        except Exception as e:
            return {'status': 'error', 'error': str(e)}