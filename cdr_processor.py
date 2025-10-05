#!/usr/bin/env python3
"""
CDR Processor
Validates and enriches Call Detail Records
"""

from datetime import datetime
from typing import Dict, Optional
from pydantic import BaseModel, Field, validator
import re


class CDRInput(BaseModel):
    """Input model for CDR submission"""
    call_id: str = Field(..., description="Unique call identifier")
    caller_number: str = Field(..., description="Calling party number")
    called_number: str = Field(..., description="Called party number")
    start_time: str = Field(..., description="Call start time (ISO format)")
    end_time: str = Field(..., description="Call end time (ISO format)")
    duration_seconds: int = Field(..., ge=0, description="Call duration in seconds")
    carrier_id: str = Field(..., description="Carrier identifier")
    call_type: str = Field(..., description="Call type: local, national, international")
    country_code: Optional[str] = Field(None, description="Destination country code")
    
    @validator('caller_number', 'called_number')
    def validate_phone_number(cls, v):
        """Validate phone number format"""
        # Simple validation - starts with + and has digits
        if not re.match(r'^\+\d{7,15}$', v):
            raise ValueError('Phone number must start with + and contain 7-15 digits')
        return v
    
    @validator('call_type')
    def validate_call_type(cls, v):
        """Validate call type"""
        valid_types = ['local', 'national', 'international']
        if v.lower() not in valid_types:
            raise ValueError(f'Call type must be one of: {", ".join(valid_types)}')
        return v.lower()
    
    @validator('start_time', 'end_time')
    def validate_datetime(cls, v):
        """Validate datetime format"""
        try:
            datetime.fromisoformat(v.replace('Z', '+00:00'))
        except ValueError:
            raise ValueError('Invalid datetime format. Use ISO format: YYYY-MM-DDTHH:MM:SSZ')
        return v


class CDRProcessor:
    """Process and enrich CDR data"""
    
    def __init__(self):
        self.country_codes = self._load_country_codes()
    
    def _load_country_codes(self) -> Dict:
        """Load country code mappings"""
        return {
            'US': 'United States',
            'GB': 'United Kingdom',
            'DE': 'Germany',
            'FR': 'France',
            'CA': 'Canada',
            'AU': 'Australia',
            'JP': 'Japan',
            'CN': 'China',
            'IN': 'India',
            'BR': 'Brazil',
            'MX': 'Mexico',
            'IT': 'Italy',
            'ES': 'Spain',
            'NL': 'Netherlands',
            'SE': 'Sweden'
        }
    
    def process(self, cdr_input: CDRInput) -> Dict:
        """
        Process and enrich CDR
        
        Args:
            cdr_input: Validated CDR input
        
        Returns:
            Enriched CDR dictionary
        """
        # Parse timestamps
        start_dt = datetime.fromisoformat(cdr_input.start_time.replace('Z', '+00:00'))
        end_dt = datetime.fromisoformat(cdr_input.end_time.replace('Z', '+00:00'))
        
        # Validate duration
        calculated_duration = int((end_dt - start_dt).total_seconds())
        if abs(calculated_duration - cdr_input.duration_seconds) > 5:  # Allow 5 second tolerance
            print(f"Warning: Duration mismatch for call {cdr_input.call_id}")
        
        # Enrich with country name
        country_name = None
        if cdr_input.country_code:
            country_name = self.country_codes.get(cdr_input.country_code.upper(), 'Unknown')
        
        # Extract number prefixes for analysis
        caller_prefix = self._extract_prefix(cdr_input.caller_number)
        called_prefix = self._extract_prefix(cdr_input.called_number)
        
        # Build enriched CDR
        enriched_cdr = {
            'call_id': cdr_input.call_id,
            'caller_number': cdr_input.caller_number,
            'called_number': cdr_input.called_number,
            'start_time': cdr_input.start_time,
            'end_time': cdr_input.end_time,
            'duration_seconds': cdr_input.duration_seconds,
            'carrier_id': cdr_input.carrier_id,
            'call_type': cdr_input.call_type,
            'country_code': cdr_input.country_code,
            'country_name': country_name,
            'caller_prefix': caller_prefix,
            'called_prefix': called_prefix,
            'timestamp': datetime.now().isoformat()
        }
        
        return enriched_cdr
    
    def _extract_prefix(self, phone_number: str) -> str:
        """Extract country/area prefix from phone number"""
        # Remove + and get first 1-3 digits
        digits = phone_number.replace('+', '')
        if len(digits) >= 3:
            return digits[:3]
        return digits
    
    def validate_cdr(self, cdr_data: Dict) -> tuple:
        """
        Validate CDR data
        
        Returns:
            (is_valid, error_message)
        """
      try:
            CDRInput(**cdr_data)
            return (True, None)
        except Exception as e:
            return (False, str(e))
    
    def detect_anomalies(self, cdr: Dict) -> list:
        """
        Detect potential anomalies in CDR
        
        Returns:
            List of anomaly warnings
        """
        anomalies = []
        
        # Check for unusually long calls (> 4 hours)
        if cdr['duration_seconds'] > 14400:
            anomalies.append('Unusually long call duration')
        
        # Check for very short calls (< 10 seconds)
        if cdr['duration_seconds'] < 10:
            anomalies.append('Very short call duration')
        
        # Check for same caller/called number
        if cdr['caller_number'] == cdr['called_number']:
            anomalies.append('Caller and called numbers are identical')
        
        return anomalies


# Example usage
if __name__ == '__main__':
    processor = CDRProcessor()
    
    print("\n" + "="*60)
    print("CDR Processor Test")
    print("="*60 + "\n")
    
    # Test CDR
    test_cdr = {
        "call_id": "test_001",
        "caller_number": "+14155551234",
        "called_number": "+442071234567",
        "start_time": "2025-01-05T10:30:00Z",
        "end_time": "2025-01-05T10:35:30Z",
        "duration_seconds": 330,
        "carrier_id": "carrier_001",
        "call_type": "international",
        "country_code": "GB"
    }
    
    # Validate
    is_valid, error = processor.validate_cdr(test_cdr)
    print(f"Validation: {'✓ PASS' if is_valid else '✗ FAIL'}")
    if error:
        print(f"Error: {error}")
    
    # Process
    if is_valid:
        cdr_input = CDRInput(**test_cdr)
        enriched = processor.process(cdr_input)
        
        print("\nEnriched CDR:")
        print(f"  Call ID: {enriched['call_id']}")
        print(f"  Type: {enriched['call_type']}")
        print(f"  Duration: {enriched['duration_seconds']}s")
        print(f"  Destination: {enriched['country_name']} ({enriched['country_code']})")
        print(f"  Carrier: {enriched['carrier_id']}")
        
        # Check anomalies
        anomalies = processor.detect_anomalies(enriched)
        if anomalies:
            print(f"\nAnomalies detected:")
            for anomaly in anomalies:
                print(f"  ⚠ {anomaly}")
        else:
            print("\n✓ No anomalies detected")
