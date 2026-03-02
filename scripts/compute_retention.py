"""Pre-compute retention flags using pandas (faster than SQL for this operation)."""

import psycopg2
from psycopg2.extras import execute_values
import pandas as pd
import numpy as np

DB = dict(host='localhost', port=5432, user='portfolio', password='portfolio_dev', dbname='portfolio')
SCHEMA = 'flowboard'

conn = psycopg2.connect(**DB)

print("Loading data...")
users = pd.read_sql("SELECT user_id, signed_up_at, region, signup_source FROM flowboard.raw_users", conn)
events = pd.read_sql("SELECT user_id, event_timestamp FROM flowboard.raw_user_events", conn)
subs = pd.read_sql("SELECT user_id, plan, started_at FROM flowboard.raw_subscriptions", conn)

print(f"  Users: {len(users):,}, Events: {len(events):,}")

# Get latest plan per user
subs_sorted = subs.sort_values('started_at').groupby('user_id').last().reset_index()
user_plan = subs_sorted[['user_id', 'plan']].rename(columns={'plan': 'current_plan'})

# Merge events with signup dates
events = events.merge(users[['user_id', 'signed_up_at']], on='user_id')
events['days_since'] = (events['event_timestamp'] - events['signed_up_at']).dt.total_seconds() / 86400.0

print("Computing retention windows...")
retention_days = [1, 7, 14, 30, 60, 90]
windows = {
    1: (1, 8), 7: (7, 14), 14: (14, 21),
    30: (30, 37), 60: (60, 67), 90: (90, 97)
}

# For each user, compute retention in each window
user_retention = {}
for rd, (lo, hi) in windows.items():
    mask = (events['days_since'] >= lo) & (events['days_since'] < hi)
    active = events[mask].groupby('user_id').size().reset_index(name='cnt')
    active['retained'] = 1
    user_retention[rd] = set(active['user_id'])

# Build output dataframe
users['cohort_week'] = users['signed_up_at'].dt.to_period('W').apply(lambda p: p.start_time.date())
users = users.merge(user_plan, on='user_id', how='left')
users['current_plan'] = users['current_plan'].fillna('free')

rows = []
for _, u in users.iterrows():
    for rd in retention_days:
        rows.append({
            'user_id': u['user_id'],
            'cohort_week': u['cohort_week'],
            'region': u['region'],
            'signup_source': u['signup_source'],
            'plan_at_latest': u['current_plan'],
            'retention_day': rd,
            'is_retained': 1 if u['user_id'] in user_retention[rd] else 0,
        })

df = pd.DataFrame(rows)
print(f"  Generated {len(df):,} retention flag rows")

# Print retention summary
for rd in retention_days:
    subset = df[df['retention_day'] == rd]
    rate = subset['is_retained'].mean() * 100
    print(f"  Day {rd:>2}: {rate:.1f}% retained")

# Load to PostgreSQL
print("Loading to PostgreSQL...")
conn.close()
conn = psycopg2.connect(**DB)
conn.autocommit = True
cur = conn.cursor()
cur.execute("DROP VIEW IF EXISTS flowboard.int_users_retention_flags CASCADE")
cur.execute("DROP TABLE IF EXISTS flowboard.int_users_retention_flags CASCADE")
cur.execute("""
    CREATE TABLE flowboard.int_users_retention_flags (
        user_id VARCHAR(50),
        cohort_week DATE,
        region TEXT,
        signup_source TEXT,
        plan_at_latest TEXT,
        retention_day INTEGER,
        is_retained INTEGER
    )
""")

records = [tuple(r) for r in df[['user_id','cohort_week','region','signup_source','plan_at_latest','retention_day','is_retained']].values]
execute_values(cur, """
    INSERT INTO flowboard.int_users_retention_flags
    (user_id, cohort_week, region, signup_source, plan_at_latest, retention_day, is_retained)
    VALUES %s
""", records, page_size=5000)

print(f"Done! Loaded {len(records):,} rows")
cur.close()
conn.close()
