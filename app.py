#!/usr/bin/env python3
"""
Real-Time CDR Processing Pipeline
FastAPI backend for Call Detail Record processing and analytics
"""

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse, StreamingResponse
from typing import Optional, List, Dict
from datetime import datetime, timedelta
import sqlite3
import json
import io
import pandas as pd

from cdr_processor import CDRInput, CDRProcessor
from cost_calculator import CostCalculator

# Initialize FastAPI app
app = FastAPI(
    title="CDR Processing Pipeline",
    description="Real-time Call Detail Record processing and analytics",
    version="1.0.0"
)

# Initialize components
cdr_processor = CDRProcessor()
cost_calculator = CostCalculator()


# Database Management
class Database:
    """SQLite database manager for CDR storage"""
    
    def __init__(self, db_path: str = "cdrs.db"):
        self.db_path = db_path
        self.init_database()
    
    def get_connection(self):
        """Get database connection"""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn
    
    def init_database(self):
        """Initialize database tables"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS cdrs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                call_id TEXT UNIQUE NOT NULL,
                caller_number TEXT NOT NULL,
                called_number TEXT NOT NULL,
                start_time TEXT NOT NULL,
                end_time TEXT NOT NULL,
                duration_seconds INTEGER NOT NULL,
                carrier_id TEXT NOT NULL,
                call_type TEXT NOT NULL,
                country_code TEXT,
                country_name TEXT,
                caller_prefix TEXT,
                called_prefix TEXT,
                cost REAL NOT NULL,
                revenue REAL NOT NULL,
                profit_margin REAL NOT NULL,
                duration_minutes INTEGER NOT NULL,
                rate_per_minute REAL NOT NULL,
                timestamp TEXT NOT NULL
            )
        """)
        
        # Create indexes for common queries
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_call_id ON cdrs(call_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_timestamp ON cdrs(timestamp)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_carrier ON cdrs(carrier_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_call_type ON cdrs(call_type)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_country ON cdrs(country_code)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_start_time ON cdrs(start_time)")
        
        conn.commit()
        conn.close()
        
        print("✓ Database initialized successfully")
    
    def insert_cdr(self, cdr_data: Dict, cost_data: Dict) -> Dict:
        """Insert a new CDR"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute("""
                INSERT INTO cdrs (
                    call_id, caller_number, called_number, start_time, end_time,
                    duration_seconds, carrier_id, call_type, country_code, country_name,
                    caller_prefix, called_prefix, cost, revenue, profit_margin,
                    duration_minutes, rate_per_minute, timestamp
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                cdr_data['call_id'],
                cdr_data['caller_number'],
                cdr_data['called_number'],
                cdr_data['start_time'],
                cdr_data['end_time'],
                cdr_data['duration_seconds'],
                cdr_data['carrier_id'],
                cdr_data['call_type'],
                cdr_data.get('country_code'),
                cdr_data.get('country_name'),
                cdr_data.get('caller_prefix'),
                cdr_data.get('called_prefix'),
                cost_data['cost'],
                cost_data['revenue'],
                cost_data['profit_margin'],
                cost_data['duration_minutes'],
                cost_data['rate_per_minute'],
                cdr_data['timestamp']
            ))
            
            cdr_id = cursor.lastrowid
            conn.commit()
            
            # Build response
            result = {**cdr_data, **cost_data, 'id': cdr_id}
            return result
            
        except sqlite3.IntegrityError:
            raise ValueError(f"CDR with call_id {cdr_data['call_id']} already exists")
        finally:
            conn.close()
    
    def get_cdrs(self, limit: int = 100, offset: int = 0,
                 carrier_id: Optional[str] = None,
                 country_code: Optional[str] = None,
                 call_type: Optional[str] = None,
                 start_date: Optional[str] = None,
                 end_date: Optional[str] = None) -> List[Dict]:
        """Get CDRs with optional filters"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        query = "SELECT * FROM cdrs WHERE 1=1"
        params = []
        
        if carrier_id:
            query += " AND carrier_id = ?"
            params.append(carrier_id)
        
        if country_code:
            query += " AND country_code = ?"
            params.append(country_code)
        
        if call_type:
            query += " AND call_type = ?"
            params.append(call_type)
        
        if start_date:
            query += " AND start_time >= ?"
            params.append(start_date)
        
        if end_date:
            query += " AND start_time <= ?"
            params.append(end_date)
        
        query += " ORDER BY timestamp DESC LIMIT ? OFFSET ?"
        params.extend([limit, offset])
        
        cursor.execute(query, params)
        rows = cursor.fetchall()
        conn.close()
        
        return [dict(row) for row in rows]
    
    def get_cdr_by_call_id(self, call_id: str) -> Optional[Dict]:
        """Get a specific CDR by call_id"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        cursor.execute("SELECT * FROM cdrs WHERE call_id = ?", (call_id,))
        row = cursor.fetchone()
        conn.close()
        
        return dict(row) if row else None
    
    def delete_cdr(self, call_id: str) -> bool:
        """Delete a CDR"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        cursor.execute("DELETE FROM cdrs WHERE call_id = ?", (call_id,))
        deleted = cursor.rowcount > 0
        
        conn.commit()
        conn.close()
        
        return deleted
    
    def get_all_cdrs(self) -> List[Dict]:
        """Get all CDRs for export"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        cursor.execute("SELECT * FROM cdrs ORDER BY timestamp DESC")
        rows = cursor.fetchall()
        conn.close()
        
        return [dict(row) for row in rows]


# Analytics Engine
class Analytics:
    """Analytics calculator for CDR data"""
    
    def __init__(self, db: Database):
        self.db = db
    
    def get_summary(self) -> Dict:
        """Get overall summary statistics"""
        cdrs = self.db.get_all_cdrs()
        
        if not cdrs:
            return {
                "total_calls": 0,
                "total_duration_seconds": 0,
                "total_cost": 0,
                "total_revenue": 0,
                "total_profit": 0
            }
        
        total_calls = len(cdrs)
        total_duration = sum(c['duration_seconds'] for c in cdrs)
        total_cost = sum(c['cost'] for c in cdrs)
        total_revenue = sum(c['revenue'] for c in cdrs)
        total_profit = sum(c['profit_margin'] for c in cdrs)
        
        # Call type distribution
        call_types = {}
        for cdr in cdrs:
            call_type = cdr['call_type']
            call_types[call_type] = call_types.get(call_type, 0) + 1
        
        return {
            "total_calls": total_calls,
            "total_duration_seconds": total_duration,
            "total_duration_hours": round(total_duration / 3600, 2),
            "average_call_duration": round(total_duration / total_calls, 1) if total_calls > 0 else 0,
            "total_cost": round(total_cost, 2),
            "total_revenue": round(total_revenue, 2),
            "total_profit": round(total_profit, 2),
            "call_types": call_types,
            "time_period": "all_time"
        }
    
    def get_cost_analysis(self) -> Dict:
        """Get detailed cost analysis"""
        cdrs = self.db.get_all_cdrs()
        
        if not cdrs:
            return {"total_calls": 0}
        
        # By call type
        cost_by_type = {}
        for cdr in cdrs:
            call_type = cdr['call_type']
            if call_type not in cost_by_type:
                cost_by_type[call_type] = {
                    'calls': 0,
                    'cost': 0,
                    'revenue': 0,
                    'profit': 0
                }
            cost_by_type[call_type]['calls'] += 1
            cost_by_type[call_type]['cost'] += cdr['cost']
            cost_by_type[call_type]['revenue'] += cdr['revenue']
            cost_by_type[call_type]['profit'] += cdr['profit_margin']
        
        # Round values
        for call_type in cost_by_type:
            cost_by_type[call_type]['cost'] = round(cost_by_type[call_type]['cost'], 2)
            cost_by_type[call_type]['revenue'] = round(cost_by_type[call_type]['revenue'], 2)
            cost_by_type[call_type]['profit'] = round(cost_by_type[call_type]['profit'], 2)
        
        return {
            "total_calls": len(cdrs),
            "cost_by_type": cost_by_type,
            "average_cost_per_call": round(sum(c['cost'] for c in cdrs) / len(cdrs), 4),
            "average_revenue_per_call": round(sum(c['revenue'] for c in cdrs) / len(cdrs), 4)
        }
    
    def get_carrier_stats(self) -> Dict:
        """Get per-carrier statistics"""
        cdrs = self.db.get_all_cdrs()
        
        if not cdrs:
            return {"total_carriers": 0, "carrier_stats": []}
        
        carrier_data = {}
        
        for cdr in cdrs:
            carrier = cdr['carrier_id']
            if carrier not in carrier_data:
                carrier_data[carrier] = {
                    'carrier_id': carrier,
                    'total_calls': 0,
                    'total_duration': 0,
                    'total_cost': 0,
                    'total_revenue': 0
                }
            
            carrier_data[carrier]['total_calls'] += 1
            carrier_data[carrier]['total_duration'] += cdr['duration_seconds']
            carrier_data[carrier]['total_cost'] += cdr['cost']
            carrier_data[carrier]['total_revenue'] += cdr['revenue']
        
        # Calculate averages and format
        carrier_stats = []
        for carrier, data in carrier_data.items():
            stats = {
                'carrier_id': carrier,
                'total_calls': data['total_calls'],
                'total_duration': data['total_duration'],
                'total_cost': round(data['total_cost'], 2),
                'total_revenue': round(data['total_revenue'], 2),
                'average_cost_per_minute': round(
                    data['total_cost'] / (data['total_duration'] / 60), 4
                ) if data['total_duration'] > 0 else 0
            }
            carrier_stats.append(stats)
        
        # Sort by total calls
        carrier_stats.sort(key=lambda x: x['total_calls'], reverse=True)
        
        return {
            "total_carriers": len(carrier_stats),
            "carrier_stats": carrier_stats
        }
    
    def get_geographic_distribution(self) -> Dict:
        """Get geographic call distribution"""
        cdrs = self.db.get_all_cdrs()
        
        if not cdrs:
            return {"total_countries": 0, "top_countries": []}
        
        country_data = {}
        
        for cdr in cdrs:
            country_code = cdr.get('country_code')
            if country_code:
                if country_code not in country_data:
                    country_data[country_code] = {
                        'country_code': country_code,
                        'country_name': cdr.get('country_name', 'Unknown'),
                        'call_count': 0,
                        'total_duration': 0
                    }
                
                country_data[country_code]['call_count'] += 1
                country_data[country_code]['total_duration'] += cdr['duration_seconds']
        
        # Calculate percentages
        total_international = sum(d['call_count'] for d in country_data.values())
        
        top_countries = []
        for data in country_data.values():
            country_stat = {
                **data,
                'percentage': round((data['call_count'] / total_international * 100), 1) if total_international > 0 else 0
            }
            top_countries.append(country_stat)
        
        # Sort by call count
        top_countries.sort(key=lambda x: x['call_count'], reverse=True)
        
        return {
            "total_countries": len(top_countries),
            "total_international_calls": total_international,
            "top_countries": top_countries[:10]  # Top 10
        }
    
    def get_traffic_analysis(self, period: str = 'hourly') -> Dict:
        """Get traffic analysis by time period"""
        cdrs = self.db.get_all_cdrs()
        
        if not cdrs:
            return {"period": period, "data": []}
        
        # Group by time period
        traffic = {}
        
        for cdr in cdrs:
            try:
                dt = datetime.fromisoformat(cdr['start_time'].replace('Z', '+00:00'))
                
                if period == 'hourly':
                    key = dt.strftime('%Y-%m-%d %H:00')
                elif period == 'daily':
                    key = dt.strftime('%Y-%m-%d')
                elif period == 'monthly':
                    key = dt.strftime('%Y-%m')
                else:
                    key = dt.strftime('%Y-%m-%d')
                
                if key not in traffic:
                    traffic[key] = {
                        'period': key,
                        'call_count': 0,
                        'total_duration': 0,
                        'total_cost': 0
                    }
                
                traffic[key]['call_count'] += 1
                traffic[key]['total_duration'] += cdr['duration_seconds']
                traffic[key]['total_cost'] += cdr['cost']
            except:
                continue
        
        # Format and sort
        traffic_data = [
            {
                **data,
                'total_cost': round(data['total_cost'], 2)
            }
            for data in traffic.values()
        ]
        traffic_data.sort(key=lambda x: x['period'])
        
        return {
            "period": period,
            "data": traffic_data
        }


# Initialize database and analytics
db = Database()
analytics = Analytics(db)


# API Endpoints

@app.get("/")
def root():
    """Root endpoint with API information"""
    return {
        "name": "CDR Processing Pipeline",
        "version": "1.0.0",
        "description": "Real-time Call Detail Record processing and analytics",
        "docs": "/docs",
        "features": [
            "Real-time CDR ingestion",
            "Cost calculation",
            "Multi-carrier analytics",
            "Geographic distribution",
            "Traffic analysis"
        ]
    }


@app.post("/cdr", status_code=201)
def create_cdr(cdr_input: CDRInput):
    """
    Submit a single Call Detail Record
    
    - **call_id**: Unique call identifier
    - **caller_number**: Calling party number (E.164 format)
    - **called_number**: Called party number (E.164 format)
    - **start_time**: Call start time (ISO format)
    - **end_time**: Call end time (ISO format)
    - **duration_seconds**: Call duration in seconds
    - **carrier_id**: Carrier identifier
    - **call_type**: Call type (local/national/international)
    - **country_code**: Destination country code (for international)
    
    Returns processed CDR with cost calculation
    """
    try:
        # Process CDR
        enriched_cdr = cdr_processor.process(cdr_input)
        
        # Calculate costs
        cost_data = cost_calculator.calculate_cost(
            duration_seconds=enriched_cdr['duration_seconds'],
            call_type=enriched_cdr['call_type'],
            country_code=enriched_cdr.get('country_code', 'US'),
            carrier_id=enriched_cdr['carrier_id']
        )
        
        # Store in database
        result = db.insert_cdr(enriched_cdr, cost_data)
        
        return result
        
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error processing CDR: {str(e)}")


@app.post("/cdr/batch", status_code=201)
def create_cdrs_batch(cdrs: List[CDRInput]):
    """
    Batch import multiple CDRs
    
    Accepts a list of CDR objects for bulk processing
    """
    try:
        results = []
        errors = []
        
        for i, cdr_input in enumerate(cdrs):
            try:
                enriched_cdr = cdr_processor.process(cdr_input)
                cost_data = cost_calculator.calculate_cost(
                    duration_seconds=enriched_cdr['duration_seconds'],
                    call_type=enriched_cdr['call_type'],
                    country_code=enriched_cdr.get('country_code', 'US'),
                    carrier_id=enriched_cdr['carrier_id']
                )
                result = db.insert_cdr(enriched_cdr, cost_data)
                results.append(result)
            except Exception as e:
                errors.append({
                    "index": i,
                    "call_id": cdr_input.call_id,
                    "error": str(e)
                })
        
        return {
            "message": f"Processed {len(results)} CDRs successfully",
            "success_count": len(results),
            "error_count": len(errors),
            "errors": errors if errors else None
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error in batch processing: {str(e)}")


@app.get("/cdr")
def get_cdrs(
    limit: int = Query(100, ge=1, le=1000, description="Maximum number of CDRs"),
    offset: int = Query(0, ge=0, description="Offset for pagination"),
    carrier_id: Optional[str] = Query(None, description="Filter by carrier"),
    country_code: Optional[str] = Query(None, description="Filter by country"),
    call_type: Optional[str] = Query(None, description="Filter by call type"),
    start_date: Optional[str] = Query(None, description="Filter by start date (ISO format)"),
    end_date: Optional[str] = Query(None, description="Filter by end date (ISO format)")
):
    """
    Query CDRs with optional filters
    
    - **limit**: Maximum number of CDRs to return (default: 100)
    - **offset**: Pagination offset
    - **carrier_id**: Filter by specific carrier
    - **country_code**: Filter by destination country
    - **call_type**: Filter by call type (local/national/international)
    - **start_date**: Filter by start date
    - **end_date**: Filter by end date
    """
    try:
        cdrs = db.get_cdrs(
            limit=limit,
            offset=offset,
            carrier_id=carrier_id,
            country_code=country_code,
            call_type=call_type,
            start_date=start_date,
            end_date=end_date
        )
        
        return {
            "count": len(cdrs),
            "limit": limit,
            "offset": offset,
            "cdrs": cdrs
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching CDRs: {str(e)}")


@app.get("/cdr/{call_id}")
def get_cdr(call_id: str):
    """Get a specific CDR by call_id"""
    cdr = db.get_cdr_by_call_id(call_id)
    if not cdr:
        raise HTTPException(status_code=404, detail=f"CDR with call_id {call_id} not found")
    return cdr


@app.delete("/cdr/{call_id}")
def delete_cdr(call_id: str):
    """Delete a CDR"""
    success = db.delete_cdr(call_id)
    if not success:
        raise HTTPException(status_code=404, detail=f"CDR with call_id {call_id} not found")
    return {"message": f"CDR {call_id} deleted successfully"}


@app.get("/analytics/summary")
def get_analytics_summary():
    """
    Get overall analytics summary
    
    Returns:
    - Total calls
    - Total duration
    - Total costs and revenue
    - Call type distribution
    """
    return analytics.get_summary()


@app.get("/analytics/costs")
def get_cost_analysis():
    """
    Get detailed cost analysis
    
    Returns cost breakdown by call type
    """
    return analytics.get_cost_analysis()


@app.get("/analytics/carriers")
def get_carrier_stats():
    """
    Get per-carrier statistics
    
    Returns call volume, costs, and performance by carrier
    """
    return analytics.get_carrier_stats()


@app.get("/analytics/geographic")
def get_geographic_distribution():
    """
    Get geographic call distribution
    
    Returns top countries by call volume
    """
    return analytics.get_geographic_distribution()


@app.get("/analytics/traffic")
def get_traffic_analysis(
    period: str = Query("daily", regex="^(hourly|daily|monthly)$", description="Time period for analysis")
):
    """
    Get traffic analysis by time period
    
    - **period**: Time period - 'hourly', 'daily', or 'monthly' (default: daily)
    """
    return analytics.get_traffic_analysis(period)


@app.get("/export")
def export_data(
    format: str = Query("json", regex="^(json|csv)$", description="Export format"),
    start_date: Optional[str] = Query(None, description="Start date filter"),
    end_date: Optional[str] = Query(None, description="End date filter")
):
    """
    Export CDR data
    
    - **format**: Export format - 'json' or 'csv' (default: json)
    - **start_date**: Optional start date filter
    - **end_date**: Optional end date filter
    """
    try:
        cdrs = db.get_all_cdrs()
        
        # Apply date filters if provided
        if start_date or end_date:
            filtered_cdrs = []
            for cdr in cdrs:
                cdr_date = cdr['start_time']
                if start_date and cdr_date < start_date:
                    continue
                if end_date and cdr_date > end_date:
                    continue
                filtered_cdrs.append(cdr)
            cdrs = filtered_cdrs
        
        if format == "json":
            return JSONResponse(content={"cdrs": cdrs, "count": len(cdrs)})
        
        elif format == "csv":
            df = pd.DataFrame(cdrs)
            csv_buffer = io.StringIO()
            df.to_csv(csv_buffer, index=False)
            csv_buffer.seek(0)
            
            filename = f"cdrs_{datetime.now().strftime('%Y%m%d')}.csv"
            
            return StreamingResponse(
                iter([csv_buffer.getvalue()]),
                media_type="text/csv",
                headers={"Content-Disposition": f"attachment; filename={filename}"}
            )
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error exporting data: {str(e)}")


@app.get("/health")
def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "total_cdrs": len(db.get_all_cdrs())
    }


@app.get("/stats")
def get_system_stats():
    """Get system statistics"""
    cdrs = db.get_all_cdrs()
    
    return {
        "total_cdrs": len(cdrs),
        "database_size_mb": 0,  # Placeholder
        "carriers_configured": len(cost_calculator.get_all_carriers()),
        "uptime": "operational"
    }


# Run the application
if __name__ == "__main__":
    import uvicorn
    
    print("\n" + "="*70)
    print("📞 Starting CDR Processing Pipeline")
    print("="*70)
    print(f"📍 API URL: http://localhost:8000")
    print(f"📚 Interactive docs: http://localhost:8000/docs")
    print(f"📖 Alternative docs: http://localhost:8000/redoc")
    print(f"💰 Cost Calculator: Enabled")
    print("="*70 + "\n")
    
    uvicorn.run(app, host="0.0.0.0", port=8000)
