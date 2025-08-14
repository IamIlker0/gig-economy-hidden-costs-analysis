# validate_reddit_data_fixed.py
import pandas as pd
import numpy as np

def validate_and_clean(df, verbose=True):
    """Fixed validation - don't require weekly_earnings"""
    
    initial_count = len(df)
    df_clean = df.copy()
    
    # 1. En az bir finansal veri olmalı
    financial_cols = ['hourly_rate', 'weekly_earnings', 'daily_earnings', 
                      'hours_worked', 'miles_driven', 'gas_expense']
    has_any_data = df_clean[financial_cols].notna().any(axis=1)
    
    df_clean = df_clean[has_any_data]
    
    # 2. Hourly rate filtresi (ana verimiz bu)
    if 'hourly_rate' in df_clean.columns:
        # $3-$100 arası kabul et
        hourly_mask = (
            df_clean['hourly_rate'].isna() | 
            ((df_clean['hourly_rate'] >= 3) & (df_clean['hourly_rate'] <= 100))
        )
        df_clean = df_clean[hourly_mask]
    
    # 3. Hours worked filtresi
    if 'hours_worked' in df_clean.columns:
        # 0-100 saat/hafta kabul et
        hours_mask = (
            df_clean['hours_worked'].isna() | 
            ((df_clean['hours_worked'] > 0) & (df_clean['hours_worked'] <= 100))
        )
        df_clean = df_clean[hours_mask]
    
    # 4. Data quality score
    df_clean['data_quality_score'] = 0
    for col in financial_cols:
        if col in df_clean.columns:
            df_clean['data_quality_score'] += df_clean[col].notna().astype(int)
    
    # 5. Net hourly hesapla (eğer mümkünse)
    if 'gas_expense' in df_clean.columns and 'hours_worked' in df_clean.columns:
        mask = (df_clean['hours_worked'] > 0) & df_clean['gas_expense'].notna()
        df_clean.loc[mask, 'gas_per_hour'] = (
            df_clean.loc[mask, 'gas_expense'] / df_clean.loc[mask, 'hours_worked']
        )
        
        # Eğer hourly_rate varsa, net hesapla
        if 'hourly_rate' in df_clean.columns:
            mask = mask & df_clean['hourly_rate'].notna()
            df_clean.loc[mask, 'net_hourly_estimate'] = (
                df_clean.loc[mask, 'hourly_rate'] - df_clean.loc[mask, 'gas_per_hour']
            )
    
    if verbose:
        print(f"=== VALIDATION REPORT ===")
        print(f"Initial posts: {initial_count}")
        print(f"Final posts: {len(df_clean)}")
        print(f"Removed: {initial_count - len(df_clean)}")
        
        print(f"\nData quality distribution:")
        print(df_clean['data_quality_score'].value_counts().sort_index())
    
    return df_clean

# Ana analiz
df = pd.read_csv('reddit_earnings_data_20250727_193003.csv')
df_clean = validate_and_clean(df)

print("\n=== CLEANED DATA ANALYSIS ===")

# Hourly rate analizi (ana metrik)
if 'hourly_rate' in df_clean.columns:
    hourly = df_clean['hourly_rate'].dropna()
    print(f"\nHourly Rate Analysis ({len(hourly)} posts):")
    print(f"  Average: ${hourly.mean():.2f}/hour")
    print(f"  Median: ${hourly.median():.2f}/hour")
    print(f"  Min: ${hourly.min():.2f}")
    print(f"  Max: ${hourly.max():.2f}")
    print(f"  Below $15 (NYC min wage): {(hourly < 15).sum()} posts ({(hourly < 15).mean()*100:.1f}%)")
    print(f"  Below $20: {(hourly < 20).sum()} posts ({(hourly < 20).mean()*100:.1f}%)")

# Hours worked analizi
if 'hours_worked' in df_clean.columns:
    hours = df_clean['hours_worked'].dropna()
    print(f"\nHours Worked Analysis ({len(hours)} posts):")
    print(f"  Average: {hours.mean():.1f} hours/week")
    print(f"  Median: {hours.median():.1f} hours/week")

# Subreddit dağılımı
print(f"\nSubreddit Distribution:")
print(df_clean['subreddit'].value_counts())

# En yüksek ve en düşük hourly rate posts
if 'hourly_rate' in df_clean.columns:
    print(f"\nHighest Hourly Rates:")
    top_earners = df_clean.nlargest(5, 'hourly_rate')[['title', 'hourly_rate', 'subreddit']]
    for idx, row in top_earners.iterrows():
        print(f"  ${row['hourly_rate']:.2f}/hr - {row['title'][:60]}...")
    
    print(f"\nLowest Hourly Rates:")
    low_earners = df_clean.nsmallest(5, 'hourly_rate')[['title', 'hourly_rate', 'subreddit']]
    for idx, row in low_earners.iterrows():
        print(f"  ${row['hourly_rate']:.2f}/hr - {row['title'][:60]}...")

# Net hourly estimate (if calculated)
if 'net_hourly_estimate' in df_clean.columns:
    net = df_clean['net_hourly_estimate'].dropna()
    if len(net) > 0:
        print(f"\nEstimated Net Hourly (after gas):")
        print(f"  Average: ${net.mean():.2f}/hour")
        print(f"  Below $15: {(net < 15).sum()} posts")

# Save cleaned data
df_clean.to_csv('reddit_earnings_cleaned.csv', index=False)
print(f"\n✅ Saved {len(df_clean)} cleaned posts to reddit_earnings_cleaned.csv")

# Summary statistics for comparison
summary_stats = {
    'total_posts': len(df_clean),
    'avg_hourly_rate': hourly.mean() if 'hourly_rate' in df_clean.columns else None,
    'median_hourly_rate': hourly.median() if 'hourly_rate' in df_clean.columns else None,
    'pct_below_15': (hourly < 15).mean() * 100 if 'hourly_rate' in df_clean.columns else None,
    'avg_hours_worked': hours.mean() if 'hours_worked' in df_clean.columns else None
}

# Save summary
import json
with open('reddit_summary_stats.json', 'w') as f:
    json.dump(summary_stats, f, indent=2)
    
print("\n📊 Summary stats saved to reddit_summary_stats.json")