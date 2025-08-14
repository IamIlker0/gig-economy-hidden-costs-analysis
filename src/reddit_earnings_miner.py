import praw
import pandas as pd
import re
from datetime import datetime
import time
import os
from dotenv import load_dotenv

# Load credentials
load_dotenv()

class RedditGigEconomyMiner:
    def __init__(self):
        # Reddit connection
        self.reddit = praw.Reddit(
            client_id=os.getenv('CLIENT_ID'),
            client_secret=os.getenv('CLIENT_SECRET'),
            user_agent=os.getenv('USER_AGENT')
        )
        
        # Data extraction patterns
        self.patterns = {
            'weekly_earnings': [
                r'\$(\d{3,4})\s*(?:week|weekly|wk)',
                r'made\s*\$?(\d{3,4})\s*this\s*week',
                r'weekly\s*earnings?\s*:?\s*\$?(\d{3,4})'
            ],
            'daily_earnings': [
                r'\$(\d{2,3})\s*(?:day|daily|today)',
                r'made\s*\$?(\d{2,3})\s*today'
            ],
            'hourly_rate': [
                r'\$(\d{1,2}(?:\.\d{2})?)\s*(?:/hour|/hr|per\s*hour)',
                r'(\d{1,2}(?:\.\d{2})?)\s*an?\s*hour'
            ],
            'hours_worked': [
                r'(\d{1,3})\s*(?:hours?|hrs?)\s*(?:week|wk|weekly)?',
                r'worked?\s*(\d{1,3})\s*(?:hours?|hrs?)'
            ],
            'miles_driven': [
                r'(\d{3,4})\s*(?:miles?|mi\.?)',
                r'drove\s*(\d{3,4})\s*(?:miles?|mi\.?)'
            ],
            'gas_expense': [
                r'(?:gas|fuel).*?\$(\d{2,3})',
                r'\$(\d{2,3}).*?(?:gas|fuel)',
                r'spent?\s*\$?(\d{2,3})\s*on\s*gas'
            ]
        }
        
    def extract_data_from_text(self, text):
        """Extract all financial data from post text"""
        results = {}
        
        for data_type, patterns in self.patterns.items():
            for pattern in patterns:
                matches = re.findall(pattern, text, re.IGNORECASE)
                if matches:
                    # Take the first match
                    try:
                        value = float(matches[0])
                        results[data_type] = value
                        break
                    except:
                        continue
        
        return results
    
    def search_and_collect(self, subreddit_name, search_queries, limit=100):
        """Search subreddit and collect relevant posts"""
        print(f"\n🔍 Searching r/{subreddit_name}...")
        subreddit = self.reddit.subreddit(subreddit_name)
        
        all_posts = []
        
        for query in search_queries:
            print(f"   Query: '{query}'")
            try:
                # Search posts
                posts = subreddit.search(query, time_filter='year', limit=limit)
                
                for post in posts:
                    # Combine title and text
                    full_text = f"{post.title} {post.selftext}"
                    
                    # Extract data
                    extracted_data = self.extract_data_from_text(full_text)
                    
                    # Only save if we found earnings data
                    if any(key in extracted_data for key in ['weekly_earnings', 'daily_earnings', 'hourly_rate']):
                        post_data = {
                            'date': datetime.fromtimestamp(post.created_utc).strftime('%Y-%m-%d'),
                            'subreddit': subreddit_name,
                            'title': post.title[:100],  # First 100 chars
                            'url': f"https://reddit.com{post.permalink}",
                            'score': post.score,
                            'num_comments': post.num_comments,
                            **extracted_data
                        }
                        all_posts.append(post_data)
                        print(f"      ✓ Found earnings data in post")
                
                # Rate limiting
                time.sleep(2)
                
            except Exception as e:
                print(f"   ❌ Error with query '{query}': {e}")
                continue
        
        print(f"   Total posts collected: {len(all_posts)}")
        return all_posts

def main():
    # Initialize miner
    miner = RedditGigEconomyMiner()
    
    # Target subreddits
    subreddits = ['uberdrivers', 'lyftdrivers', 'doordash_drivers']
    
    # Search queries
    search_queries = [
        'weekly earnings',
        'made this week',
        'income breakdown',
        'after gas',
        'net earnings',
        'hourly rate'
    ]
    
    # Collect data
    all_data = []
    
    for subreddit in subreddits:
        posts = miner.search_and_collect(subreddit, search_queries, limit=50)
        all_data.extend(posts)
    
    # Convert to DataFrame
    df = pd.DataFrame(all_data)
    
    # Save to files
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    # Save as CSV
    df.to_csv(f'reddit_earnings_data_{timestamp}.csv', index=False)
    
    # Save as Excel with formatting
    with pd.ExcelWriter(f'reddit_earnings_data_{timestamp}.xlsx') as writer:
        df.to_excel(writer, sheet_name='Raw Data', index=False)
        
        # Summary statistics
        if len(df) > 0:
            summary = pd.DataFrame({
                'Metric': ['Total Posts', 'Avg Weekly Earnings', 'Avg Hourly Rate', 
                          'Avg Hours/Week', 'Avg Miles/Week'],
                'Value': [
                    len(df),
                    df['weekly_earnings'].mean() if 'weekly_earnings' in df else 'N/A',
                    df['hourly_rate'].mean() if 'hourly_rate' in df else 'N/A',
                    df['hours_worked'].mean() if 'hours_worked' in df else 'N/A',
                    df['miles_driven'].mean() if 'miles_driven' in df else 'N/A'
                ]
            })
            summary.to_excel(writer, sheet_name='Summary', index=False)
    
    print(f"\n✅ Data collection complete!")
    print(f"📊 Total posts collected: {len(df)}")
    print(f"💾 Saved to: reddit_earnings_data_{timestamp}.csv")

if __name__ == "__main__":
    main()