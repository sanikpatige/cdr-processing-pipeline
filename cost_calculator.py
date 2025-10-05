#!/usr/bin/env python3
"""
Cost Calculator for CDR Processing
Calculates call costs based on duration, type, and carrier rates
"""

from typing import Dict
import json


class CostCalculator:
    """Calculate call costs based on carrier rates and call parameters"""
    
    def __init__(self, rate_table_path: str = "rate_tables.json"):
        self.rates = self._load_rate_tables(rate_table_path)
    
    def _load_rate_tables(self, path: str) -> Dict:
        """Load carrier rate tables from JSON file"""
        try:
            with open(path, 'r') as f:
                return json.load(f)
        except FileNotFoundError:
            # Return default rates if file doesn't exist
            return self._default_rate_tables()
    
    def _default_rate_tables(self) -> Dict:
        """Default rate tables for testing"""
        return {
            "carriers": {
                "carrier_001": {
                    "name": "Premium Carrier A",
                    "local": 0.01,
                    "national": 0.02,
                    "international": {
                        "US": 0.03,
                        "GB": 0.04,
                        "DE": 0.04,
                        "FR": 0.04,
                        "AU": 0.05,
                        "JP": 0.06,
                        "default": 0.08
                    }
                },
                "carrier_002": {
                    "name": "Budget Carrier B",
                    "local": 0.008,
                    "national": 0.015,
                    "international": {
                        "US": 0.025,
                        "GB": 0.035,
                        "DE": 0.035,
                        "FR": 0.035,
                        "AU": 0.045,
                        "JP": 0.055,
                        "default": 0.07
                    }
                },
                "carrier_003": {
                    "name": "Standard Carrier C",
                    "local": 0.009,
                    "national": 0.018,
                    "international": {
                        "US": 0.028,
                        "GB": 0.038,
                        "DE": 0.038,
                        "FR": 0.038,
                        "AU": 0.048,
                        "JP": 0.058,
                        "default": 0.075
                    }
                }
            },
            "markup": 1.5  # 50% markup for revenue calculation
        }
    
    def calculate_cost(self, duration_seconds: int, call_type: str, 
                      country_code: str, carrier_id: str) -> Dict:
        """
        Calculate call cost, revenue, and profit
        
        Args:
            duration_seconds: Call duration in seconds
            call_type: 'local', 'national', or 'international'
            country_code: ISO country code (for international calls)
            carrier_id: Carrier identifier
        
        Returns:
            Dictionary with cost, revenue, and profit_margin
        """
        # Convert seconds to minutes (round up)
        duration_minutes = (duration_seconds + 59) // 60  # Round up to nearest minute
        
        # Get carrier rates
        carrier_rates = self.rates['carriers'].get(carrier_id)
        if not carrier_rates:
            carrier_rates = self.rates['carriers']['carrier_001']  # Default carrier
        
        # Calculate base cost per minute
        if call_type == 'local':
            rate_per_minute = carrier_rates['local']
        elif call_type == 'national':
            rate_per_minute = carrier_rates['national']
        elif call_type == 'international':
            intl_rates = carrier_rates['international']
            rate_per_minute = intl_rates.get(country_code, intl_rates['default'])
        else:
            rate_per_minute = carrier_rates['local']  # Default to local
        
        # Calculate costs
        cost = round(duration_minutes * rate_per_minute, 4)
        revenue = round(cost * self.rates['markup'], 4)
        profit_margin = round(revenue - cost, 4)
        
        return {
            'cost': cost,
            'revenue': revenue,
            'profit_margin': profit_margin,
            'duration_minutes': duration_minutes,
            'rate_per_minute': rate_per_minute
        }
    
    def get_carrier_info(self, carrier_id: str) -> Dict:
        """Get carrier information"""
        carrier = self.rates['carriers'].get(carrier_id, {})
        return {
            'carrier_id': carrier_id,
            'name': carrier.get('name', 'Unknown'),
            'has_rates': carrier_id in self.rates['carriers']
        }
    
    def get_all_carriers(self) -> list:
        """Get list of all configured carriers"""
        return [
            {
                'carrier_id': cid,
                'name': info['name']
            }
            for cid, info in self.rates['carriers'].items()
        ]


# Example usage
if __name__ == '__main__':
    calculator = CostCalculator()
    
    print("\n" + "="*60)
    print("Cost Calculator Test")
    print("="*60 + "\n")
    
    # Test scenarios
    test_calls = [
        (300, 'local', 'US', 'carrier_001', "5 min local call"),
        (600, 'national', 'US', 'carrier_001', "10 min national call"),
        (900, 'international', 'GB', 'carrier_002', "15 min to UK"),
        (1800, 'international', 'JP', 'carrier_003', "30 min to Japan"),
    ]
    
    for duration, call_type, country, carrier, description in test_calls:
        result = calculator.calculate_cost(duration, call_type, country, carrier)
        print(f"{description}:")
        print(f"  Duration: {duration}s ({result['duration_minutes']} minutes)")
        print(f"  Carrier: {carrier}")
        print(f"  Cost: ${result['cost']:.4f}")
        print(f"  Revenue: ${result['revenue']:.4f}")
        print(f"  Profit: ${result['profit_margin']:.4f}")
        print()
