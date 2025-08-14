# debug_data.py
import pandas as pd
import numpy as np

# Load original data
df = pd.read_csv('reddit_earnings_data_20250727_193003.csv')

print("=== ORIGINAL DATA INFO ===")
print(f"Total rows: {len(df)}")
print(f"\nColumns: {df.columns.tolist()}")
print(f"\nFirst 5 rows:")
print(df.head())

print("\n=== DATA STATISTICS ===")
for col in ['weekly_earnings', 'hourly_rate', 'hours_worked', 'miles_driven']:
    if col in df.columns:
        non_null = df[col].notna().sum()
        if non_null > 0:
            print(f"\n{col}:")
            print(f"  Non-null values: {non_null}")
            print(f"  Min: {df[col].min()}")
            print(f"  Max: {df[col].max()}")
            print(f"  Mean: {df[col].mean():.2f}")
            print(f"  Values: {df[col].dropna().head(10).tolist()}")

# Check why validation might be failing
print("\n=== POTENTIAL ISSUES ===")

# Weekly earnings check
if 'weekly_earnings' in df.columns:
    weekly = df['weekly_earnings'].dropna()
    print(f"\nWeekly earnings outside 100-5000 range:")
    print(f"  < 100: {(weekly < 100).sum()} posts")
    print(f"  > 5000: {(weekly > 5000).sum()} posts")
    
# Hourly rate check  
if 'hourly_rate' in df.columns:
    hourly = df['hourly_rate'].dropna()
    print(f"\nHourly rate outside 5-100 range:")
    print(f"  < 5: {(hourly < 5).sum()} posts")
    print(f"  > 100: {(hourly > 100).sum()} posts")