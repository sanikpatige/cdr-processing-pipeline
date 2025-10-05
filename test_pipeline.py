#!/usr/bin/env python3
"""
Test script for CDR Processing Pipeline
Generates sample CDRs and displays analytics
"""

import requests
import random
from datetime import datetime, timedelta
import time

API_URL = "http://localhost:8000"

# Sample phone numbers
US_NUMBERS = ["+14155551234", "+13105559876", "+12125558888", "+17185559999"]
UK_NUMBERS = ["+442071234567", "+441234567890", "+447911123456"]
DE_NUMBERS = ["+493012345678", "+491761234567"]
FR_NUMBERS = ["+33123456789", "+33612345678"]
AU_NUMBERS = ["+61212345678", "+61412345678"]
JP_NUMBERS = ["+81312345678", "+81901234567"]

COUNTRY_NUMBERS = {
    'US': US_NUMBERS,
    'GB': UK_NUMBERS,
    'DE': DE_NUMBERS,
    'FR': FR_NUMBERS,
    'AU': AU_NUMBERS,
    'JP': JP_NUMBERS
}

CARRIERS = ["carrier_001", "carrier_002", "carrier_003"]

def generate_call_time(days_ago=0):
    """Generate realistic call times"""
    base_time = datetime.now() - timedelta(days=days_ago)
    # Random hour between 8 AM and 8 PM
    hour = random.randint(8, 20)
    minute = random.randint(0, 59)
    second = random.randint(0, 59)
    
    start = base_time.replace(hour=hour, minute=minute, second=second, microsecond=0)
    # Call duration between 1 and 30 minutes
    duration = random.randint(60, 1800)
    end = start + timedelta(seconds=duration)
    
    return start, end, duration

def generate_sample_cdrs(count=50):
    """Generate sample CDRs with realistic data"""
    
    print(f"\n{'='*70}")
    print(f"Generating {count} sample CDRs...")
    print(f"{'='*70}\n")
    
    success_count = 0
    
    call_types = ['local', 'national', 'international']
    call_type_weights = [40, 30, 30]  # 40% local, 30% national, 30% international
    
    for i in range(count):
        try:
            # Generate call type
            call_type = random.choices(call_types, weights=call_type_weights)[0]
            
            # Generate caller (always US for simplicity)
            caller = random.choice(US_NUMBERS)
            
            # Generate called number based on call type
            if call_type == 'local':
                called = random.choice(US_NUMBERS)
                country_code = 'US'
            elif call_type == 'national':
                called = random.choice(US_NUMBERS)
                country_code = 'US'
            else:  # international
                country_code = random.choice(list(COUNTRY_NUMBERS.keys()))
                called = random.choice(COUNTRY_NUMBERS[country_code])
            
            # Generate call times (spread across last 7 days)
            days_ago = random.randint(0, 7)
            start_time, end_time, duration = generate_call_time(days_ago)
            
            # Select carrier
            carrier = random.choice(CARRIERS)
            
            # Create CDR
            cdr_data = {
                "call_id": f"call_{1000 + i}",
                "caller_number": caller,
                "called_number": called,
                "start_time": start_time.isoformat() + "Z",
                "end_time": end_time.isoformat() + "Z",
                "duration_seconds": duration,
                "carrier_id": carrier,
                "call_type": call_type,
                "country_code": country_code if call_type == 'international' else None
            }
            
            # Submit to API
            response = requests.post(f"{API_URL}/cdr", json=cdr_data)
            
            if response.status_code == 201:
                result = response.json()
                success_count += 1
                
                print(f"✓ [{i+1}/{count}] Call {result['call_id']}")
                print(f"   {call_type.upper()}: {caller} → {called}")
                print(f"   Duration: {duration}s | Carrier: {carrier}")
                print(f"   Cost: ${result['cost']:.4f} | Revenue: ${result['revenue']:.4f}")
                print()
                
                time.sleep(0.1)  # Small delay
            else:
                print(f"✗ [{i+1}/{count}] Failed: {response.status_code}")
                print(f"   Error: {response.text}")
        
        except Exception as e:
            print(f"✗ [{i+1}/{count}] Error: {e}")
    
    print(f"\n{'='*70}")
    print(f"✓ Successfully created {success_count}/{count} CDRs")
    print(f"{'='*70}\n")
    
    return success_count

def display_analytics():
    """Display comprehensive analytics"""
    
    print(f"\n{'='*70}")
    print("📊 CDR ANALYTICS DASHBOARD")
    print(f"{'='*70}\n")
    
    try:
        # Overall Summary
        print("📈 Overall Summary")
        print("-" * 70)
        response = requests.get(f"{API_URL}/analytics/summary")
        summary = response.json()
        
        print(f"Total Calls: {summary['total_calls']}")
        print(f"Total Duration: {summary['total_duration_hours']} hours")
        print(f"Average Call Duration: {summary['average_call_duration']} seconds")
        print(f"\nFinancial Summary:")
        print(f"  Total Cost: ${summary['total_cost']}")
        print(f"  Total Revenue: ${summary['total_revenue']}")
        print(f"  Total Profit: ${summary['total_profit']}")
        
        print(f"\nCall Type Distribution:")
        for call_type, count in summary['call_types'].items():
            percentage = (count / summary['total_calls'] * 100) if summary['total_calls'] > 0 else 0
            bar = "█" * int(percentage / 2)
            print(f"  {call_type.capitalize():12s}: {count:3d} ({percentage:.1f}%) {bar}")
        
        # Cost Analysis
        print(f"\n{'='*70}")
        print("💰 Cost Analysis")
        print("-" * 70)
        response = requests.get(f"{API_URL}/analytics/costs")
        costs = response.json()
        
        print(f"Average Cost per Call: ${costs['average_cost_per_call']:.4f}")
        print(f"Average Revenue per Call: ${costs['average_revenue_per_call']:.4f}")
        
        print("\nCost Breakdown by Call Type:")
        for call_type, data in costs['cost_by_type'].items():
            print(f"\n  {call_type.upper()}:")
            print(f"    Calls: {data['calls']}")
            print(f"    Total Cost: ${data['cost']}")
            print(f"    Total Revenue: ${data['revenue']}")
            print(f"    Profit: ${data['profit']}")
        
        # Carrier Statistics
        print(f"\n{'='*70}")
        print("🚚 Carrier Performance")
        print("-" * 70)
        response = requests.get(f"{API_URL}/analytics/carriers")
        carrier_stats = response.json()
        
        print(f"Total Carriers: {carrier_stats['total_carriers']}\n")
        
        for carrier in carrier_stats['carrier_stats']:
            print(f"  {carrier['carrier_id']}:")
            print(f"    Calls: {carrier['total_calls']}")
            print(f"    Duration: {carrier['total_duration']}s")
            print(f"    Total Cost: ${carrier['total_cost']}")
            print(f"    Avg Cost/min: ${carrier['average_cost_per_minute']:.4f}")
            print()
        
        # Geographic Distribution
        print(f"{'='*70}")
        print("🌍 Geographic Distribution")
        print("-" * 70)
        response = requests.get(f"{API_URL}/analytics/geographic")
        geo_stats = response.json()
        
        if geo_stats['total_countries'] > 0:
            print(f"Total Countries: {geo_stats['total_countries']}")
            print(f"Total International Calls: {geo_stats['total_international_calls']}\n")
            
            print("Top Countries:")
            for country in geo_stats['top_countries'][:5]:
                print(f"  {country['country_code']} - {country['country_name']}:")
                print(f"    Calls: {country['call_count']} ({country['percentage']}%)")
                print(f"    Duration: {country['total_duration']}s")
                print()
        else:
            print("No international calls found")
        
        # Traffic Analysis
        print(f"{'='*70}")
        print("📊 Traffic Analysis (Daily)")
        print("-" * 70)
        response = requests.get(f"{API_URL}/analytics/traffic?period=daily")
        traffic = response.json()
        
        if traffic['data']:
            print("\nDaily Call Volume:")
            for day_data in traffic['data'][-7:]:  # Last 7 days
                print(f"  {day_data['period']}: {day_data['call_count']} calls, "
                      f"{day_data['total_duration']}s, ${day_data['total_cost']}")
        
        print(f"\n{'='*70}\n")
        
    except Exception as e:
        print(f"Error fetching analytics: {e}")

def test_export():
    """Test data export functionality"""
    
    print(f"\n{'='*70}")
    print("📥 Testing Data Export")
    print(f"{'='*70}\n")
    
    try:
        # Export as JSON
        print("Exporting as JSON...")
        response = requests.get(f"{API_URL}/export?format=json")
        if response.status_code == 200:
            data = response.json()
            print(f"✓ JSON export successful: {data['count']} CDRs")
        
        # Export as CSV
        print("\nExporting as CSV...")
        response = requests.get(f"{API_URL}/export?format=csv")
        if response.status_code == 200:
            print(f"✓ CSV export successful: {len(response.content)} bytes")
            
            # Save to file
            with open('cdrs_export.csv', 'wb') as f:
                f.write(response.content)
            print("✓ Saved to: cdrs_export.csv")
        
        print()
        
    except Exception as e:
        print(f"Error during export: {e}")

def test_queries():
    """Test various query filters"""
    
    print(f"\n{'='*70}")
    print("🔍 Testing Query Filters")
    print(f"{'='*70}\n")
    
    try:
        # Query by carrier
        print("Querying CDRs for carrier_001...")
        response = requests.get(f"{API_URL}/cdr?carrier_id=carrier_001&limit=5")
        if response.status_code == 200:
            data = response.json()
            print(f"✓ Found {data['count']} CDRs for carrier_001\n")
        
        # Query international calls
        print("Querying international calls...")
        response = requests.get(f"{API_URL}/cdr?call_type=international&limit=5")
        if response.status_code == 200:
            data = response.json()
            print(f"✓ Found {data['count']} international calls\n")
        
        # Query by country
        print("Querying calls to UK...")
        response = requests.get(f"{API_URL}/cdr?country_code=GB&limit=5")
        if response.status_code == 200:
            data = response.json()
            print(f"✓ Found {data['count']} calls to UK\n")
        
    except Exception as e:
        print(f"Error during queries: {e}")

def main():
    print("\n" + "="*70)
    print("📞 CDR Processing Pipeline - Test Suite")
    print("="*70)
    print("\nMake sure the API server is running: python app.py")
    print("\nThis will:")
    print("  1. Generate 50 sample CDRs")
    print("  2. Display comprehensive analytics")
    print("  3. Test export functionality")
    print("  4. Test query filters")
    print("\nPress Enter to start or Ctrl+C to cancel...")
    input()
    
    # Generate sample data
    cdrs_created = generate_sample_cdrs(50)
    
    if cdrs_created > 0:
        # Display analytics
        display_analytics()
        
        # Test export
        test_export()
        
        # Test queries
        test_queries()
        
        print("\n✅ All tests completed!")
        print("\nNext steps:")
        print("  1. View interactive API docs: http://localhost:8000/docs")
        print("  2. Query CDRs: curl 'http://localhost:8000/cdr?limit=10'")
        print("  3. View analytics: curl 'http://localhost:8000/analytics/summary'")
        print("  4. Export data: curl 'http://localhost:8000/export?format=csv' -o cdrs.csv")
        print()
    else:
        print("\n❌ No CDRs were created. Check if the server is running.")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\nTest cancelled by user.")
