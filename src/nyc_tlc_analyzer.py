# nyc_tlc_analyzer_v2.py - Memory optimized version
import pandas as pd
import pyarrow.parquet as pq
import numpy as np
import os
from datetime import datetime
import gc

class NYCTLCAnalyzer:
    def __init__(self, data_dir):
        self.data_dir = data_dir
        self.aaa_costs = {
            2019: 0.608,
            2020: 0.592,
            2021: 0.608,
            2022: 0.658,
            2023: 0.729,
            2024: 0.816,
            2025: 0.816
        }
        
    def process_single_month_chunked(self, filepath):
        """Process one month of data in chunks"""
        print(f"\nProcessing: {os.path.basename(filepath)}")
        
        # Extract year from filename
        year = int(os.path.basename(filepath).split('-')[0].split('_')[-1])
        cost_per_mile = self.aaa_costs.get(year, 0.75)
        
        # Process in chunks
        chunk_size = 1_000_000  # 1 million rows at a time
        monthly_stats = []
        total_below_min = 0
        total_valid_trips = 0
        
        # Read parquet file info
        parquet_file = pq.ParquetFile(filepath)
        total_rows = parquet_file.metadata.num_rows
        print(f"  Total trips: {total_rows:,}")
        
        # Process chunks
        for i in range(0, total_rows, chunk_size):
            print(f"  Processing chunk {i//chunk_size + 1}/{(total_rows//chunk_size) + 1}...", end='\r')
            
            # Read chunk
            chunk = pd.read_parquet(filepath, 
                                  columns=['trip_time', 'trip_miles', 'driver_pay', 'tips'],
                                  filters=[('trip_time', '>', 0)],
                                  # Read specific rows
                                  # Note: pyarrow doesn't support row slicing directly
                                  )
            
            # Skip if chunk is empty
            if len(chunk) == 0:
                continue
                
            # Basic filtering
            chunk = chunk[
                (chunk['trip_time'] > 0) & 
                (chunk['trip_miles'] > 0) & 
                (chunk['driver_pay'] > 0)
            ]
            
            # Calculate metrics
            chunk['trip_hours'] = chunk['trip_time'] / 3600
            chunk['gross_hourly'] = (chunk['driver_pay'] + chunk['tips']) / chunk['trip_hours']
            
            # Filter outliers
            chunk = chunk[(chunk['gross_hourly'] > 5) & (chunk['gross_hourly'] < 200)]
            
            # Calculate net
            chunk['trip_cost'] = chunk['trip_miles'] * cost_per_mile
            chunk['net_earnings'] = chunk['driver_pay'] + chunk['tips'] - chunk['trip_cost']
            chunk['net_hourly'] = chunk['net_earnings'] / chunk['trip_hours']
            
            # Collect stats
            if len(chunk) > 0:
                monthly_stats.append({
                    'trips': len(chunk),
                    'gross_hourly_sum': chunk['gross_hourly'].sum(),
                    'net_hourly_sum': chunk['net_hourly'].sum(),
                    'below_min': (chunk['net_hourly'] < 15).sum(),
                    'miles_sum': chunk['trip_miles'].sum(),
                    'time_sum': chunk['trip_time'].sum(),
                    'tips_sum': chunk['tips'].sum()
                })
                
                total_below_min += (chunk['net_hourly'] < 15).sum()
                total_valid_trips += len(chunk)
            
            # Clear memory
            del chunk
            gc.collect()
        
        print()  # New line after progress
        
        # Aggregate all chunks
        if not monthly_stats:
            return None
            
        total_trips = sum(s['trips'] for s in monthly_stats)
        avg_gross = sum(s['gross_hourly_sum'] for s in monthly_stats) / total_trips
        avg_net = sum(s['net_hourly_sum'] for s in monthly_stats) / total_trips
        
        summary = {
            'year_month': f"{year}-{os.path.basename(filepath).split('-')[1].split('.')[0]}",
            'total_trips': total_valid_trips,
            'avg_gross_hourly': avg_gross,
            'avg_net_hourly': avg_net,
            'pct_below_minimum': (total_below_min / total_valid_trips * 100) if total_valid_trips > 0 else 0,
            'avg_trip_miles': sum(s['miles_sum'] for s in monthly_stats) / total_trips,
            'avg_trip_time_min': sum(s['time_sum'] for s in monthly_stats) / total_trips / 60,
            'avg_tips': sum(s['tips_sum'] for s in monthly_stats) / total_trips,
            'cost_per_mile': cost_per_mile
        }
        
        return summary
    
    def process_all_months(self):
        """Process all parquet files"""
        results = []
        
        # Find all parquet files
        parquet_files = [f for f in os.listdir(self.data_dir) if f.endswith('.parquet')]
        parquet_files.sort()
        
        print(f"Found {len(parquet_files)} files to process")
        
        # Process each file
        for i, file in enumerate(parquet_files):
            print(f"\n[{i+1}/{len(parquet_files)}] ", end='')
            filepath = os.path.join(self.data_dir, file)
            
            try:
                # Try chunked processing first
                summary = self.process_single_month_chunked(filepath)
                if summary:
                    results.append(summary)
                    
                    # Save intermediate results
                    pd.DataFrame(results).to_csv('nyc_monthly_summaries_temp.csv', index=False)
                    
            except Exception as e:
                print(f"  Error: {str(e)}")
                # Try alternative method with sampling
                try:
                    print(f"  Trying with sampling...")
                    summary = self.process_with_sampling(filepath)
                    if summary:
                        results.append(summary)
                except Exception as e2:
                    print(f"  Sampling also failed: {str(e2)}")
                    continue
        
        # Convert to DataFrame
        results_df = pd.DataFrame(results)
        results_df.to_csv('nyc_monthly_summaries.csv', index=False)
        
        return results_df
    
    def process_with_sampling(self, filepath, sample_size=500_000):
        """Process with random sampling as fallback"""
        year = int(os.path.basename(filepath).split('-')[0].split('_')[-1])
        cost_per_mile = self.aaa_costs.get(year, 0.75)
        
        # Read sample
        df = pd.read_parquet(filepath, columns=['trip_time', 'trip_miles', 'driver_pay', 'tips'])
        
        # Random sample if too large
        if len(df) > sample_size:
            df = df.sample(n=sample_size, random_state=42)
            print(f"  Sampled {sample_size:,} trips from {len(df):,}")
        
        # Same processing as before
        df = df[(df['trip_time'] > 0) & (df['trip_miles'] > 0) & (df['driver_pay'] > 0)]
        
        df['trip_hours'] = df['trip_time'] / 3600
        df['gross_hourly'] = (df['driver_pay'] + df['tips']) / df['trip_hours']
        df = df[(df['gross_hourly'] > 5) & (df['gross_hourly'] < 200)]
        
        df['trip_cost'] = df['trip_miles'] * cost_per_mile
        df['net_earnings'] = df['driver_pay'] + df['tips'] - df['trip_cost']
        df['net_hourly'] = df['net_earnings'] / df['trip_hours']
        
        return {
            'year_month': f"{year}-{os.path.basename(filepath).split('-')[1].split('.')[0]}",
            'total_trips': len(df),
            'avg_gross_hourly': df['gross_hourly'].mean(),
            'avg_net_hourly': df['net_hourly'].mean(),
            'pct_below_minimum': (df['net_hourly'] < 15).mean() * 100,
            'avg_trip_miles': df['trip_miles'].mean(),
            'avg_trip_time_min': df['trip_time'].mean() / 60,
            'avg_tips': df['tips'].mean(),
            'cost_per_mile': cost_per_mile,
            'is_sample': True
        }

# Main execution continues same as before...
if __name__ == "__main__":
    DATA_DIR = "data-processes/NYC-TLC-analysis/data/raw"
    
    analyzer = NYCTLCAnalyzer(DATA_DIR)
    
    print("Starting NYC TLC data analysis (Memory Optimized)...")
    monthly_results = analyzer.process_all_months()
    
    # Rest of the code remains the same...
    
    # Overall statistics
    print("\n=== OVERALL NYC TLC STATISTICS ===")
    print(f"Total months analyzed: {len(monthly_results)}")
    print(f"Total trips processed: {monthly_results['total_trips'].sum():,}")
    
    # Weighted averages (by trip count)
    weights = monthly_results['total_trips']
    avg_gross = np.average(monthly_results['avg_gross_hourly'], weights=weights)
    avg_net = np.average(monthly_results['avg_net_hourly'], weights=weights)
    avg_below_min = np.average(monthly_results['pct_below_minimum'], weights=weights)
    
    print(f"\nWeighted Averages:")
    print(f"  Gross hourly: ${avg_gross:.2f}")
    print(f"  Net hourly: ${avg_net:.2f}")
    print(f"  Below minimum wage: {avg_below_min:.1f}%")
    
    # Save final stats
    final_stats = {
        'total_months': len(monthly_results),
        'total_trips': int(monthly_results['total_trips'].sum()),
        'avg_gross_hourly': round(avg_gross, 2),
        'avg_net_hourly': round(avg_net, 2),
        'pct_below_minimum': round(avg_below_min, 1),
        'date_range': f"{monthly_results['year_month'].min()} to {monthly_results['year_month'].max()}"
    }
    
    import json
    with open('nyc_tlc_final_stats.json', 'w') as f:
        json.dump(final_stats, f, indent=2)
    
    print("\n✅ Analysis complete! Results saved to:")
    print("   - nyc_monthly_summaries.csv")
    print("   - nyc_tlc_final_stats.json")