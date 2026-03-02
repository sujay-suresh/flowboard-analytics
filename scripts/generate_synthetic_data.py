"""
FlowBoard Analytics — Synthetic Data Generator
Generates realistic SaaS product analytics data and loads to PostgreSQL.

Tables created in schema 'flowboard':
  - raw_users          (8,000 rows)
  - raw_user_events    (~2,000,000 rows)
  - raw_subscriptions  (~12,000 rows)
  - raw_feature_usage  (~500,000 rows)
  - raw_invoices       (derived from subscriptions)
  - raw_conversations  (support conversations)
"""

import random
import math
import uuid
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
from faker import Faker
import psycopg2
from psycopg2.extras import execute_values

# ── Config ──────────────────────────────────────────────────────────────
SEED = 42
random.seed(SEED)
np.random.seed(SEED)
fake = Faker()
Faker.seed(SEED)

NUM_USERS = 8_000
NUM_EVENTS_TARGET = 2_000_000
NUM_FEATURE_USAGE_TARGET = 500_000
START_DATE = datetime(2024, 9, 1)
END_DATE = datetime(2026, 2, 28)
TOTAL_DAYS = (END_DATE - START_DATE).days  # ~18 months

REGIONS = {"US": 0.50, "UK": 0.30, "AU": 0.20}
SIGNUP_SOURCES = {
    "organic": 0.30,
    "google_ads": 0.25,
    "referral": 0.15,
    "product_hunt": 0.10,
    "content_marketing": 0.20,
}
PLANS = ["free", "pro", "enterprise"]
EVENT_TYPES = [
    "board_created",
    "member_invited",
    "task_created",
    "task_completed",
    "comment_added",
    "file_uploaded",
    "settings_changed",
]
FEATURES = [
    "kanban_board", "gantt_chart", "time_tracking", "file_storage",
    "team_chat", "automations", "custom_fields", "reporting",
    "api_access", "sso_login", "guest_access", "templates",
    "calendar_view", "dependencies", "workload_view",
]

DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "user": "portfolio",
    "password": "portfolio_dev",
    "dbname": "portfolio",
}
SCHEMA = "flowboard"


def weighted_choice(options: dict) -> str:
    keys = list(options.keys())
    weights = list(options.values())
    return random.choices(keys, weights=weights, k=1)[0]


def sigmoid_growth(day: int, total_days: int, peak_rate: float = 1.0) -> float:
    """Realistic S-curve signup growth."""
    midpoint = total_days * 0.4
    steepness = 8.0 / total_days
    return peak_rate / (1.0 + math.exp(-steepness * (day - midpoint)))


# ── 1. Generate Users ──────────────────────────────────────────────────
def generate_users() -> pd.DataFrame:
    print("Generating users...")

    # Distribute signups across days using sigmoid growth curve
    day_weights = [sigmoid_growth(d, TOTAL_DAYS) for d in range(TOTAL_DAYS)]
    total_weight = sum(day_weights)
    day_probs = [w / total_weight for w in day_weights]

    signup_days = np.random.choice(TOTAL_DAYS, size=NUM_USERS, p=day_probs)
    signup_days.sort()

    users = []
    for i, day in enumerate(signup_days):
        signup_dt = START_DATE + timedelta(
            days=int(day),
            hours=random.randint(0, 23),
            minutes=random.randint(0, 59),
            seconds=random.randint(0, 59),
        )
        region = weighted_choice(REGIONS)
        source = weighted_choice(SIGNUP_SOURCES)
        user_id = f"usr_{i+1:05d}"

        users.append({
            "user_id": user_id,
            "email": fake.unique.email(),
            "full_name": fake.name(),
            "region": region,
            "signup_source": source,
            "signed_up_at": signup_dt,
            "initial_plan": "free",
        })

    return pd.DataFrame(users)


# ── 2. Generate Events ────────────────────────────────────────────────
def generate_events(users_df: pd.DataFrame) -> pd.DataFrame:
    print("Generating user events...")

    # Power-law activity distribution: few power users, many light users
    n = len(users_df)
    # Pareto-distributed activity levels
    activity_levels = np.random.pareto(a=1.2, size=n) + 0.1
    activity_levels = activity_levels / activity_levels.sum()

    # Target event counts per user
    target_per_user = (activity_levels * NUM_EVENTS_TARGET).astype(int)
    target_per_user = np.maximum(target_per_user, 1)  # at least 1 event

    # CRITICAL INSIGHT: users who invite ≥2 members in week 1 are "activated"
    # They should be ~15% of users but have much higher retention/conversion
    activated_mask = np.random.random(n) < 0.15

    all_events = []

    for idx, user_row in users_df.iterrows():
        user_id = user_row["user_id"]
        signup_dt = user_row["signed_up_at"]
        num_events = int(target_per_user[idx])
        is_activated = activated_mask[idx]
        days_since_signup = (END_DATE - signup_dt).days

        if days_since_signup <= 0:
            continue

        events_for_user = []

        # First event is always board_created (within hours of signup)
        board_created_dt = signup_dt + timedelta(
            minutes=random.randint(5, 180)
        )
        events_for_user.append({
            "event_id": str(uuid.uuid4()),
            "user_id": user_id,
            "event_type": "board_created",
            "event_timestamp": board_created_dt,
            "event_properties": '{"board_name": "My First Board"}',
        })

        if is_activated:
            # Activated users: invite ≥2 members in week 1
            num_invites_week1 = random.randint(2, 5)
            for j in range(num_invites_week1):
                invite_dt = signup_dt + timedelta(
                    days=random.randint(0, 6),
                    hours=random.randint(0, 23),
                    minutes=random.randint(0, 59),
                )
                if invite_dt <= board_created_dt:
                    invite_dt = board_created_dt + timedelta(minutes=random.randint(10, 120))
                events_for_user.append({
                    "event_id": str(uuid.uuid4()),
                    "user_id": user_id,
                    "event_type": "member_invited",
                    "event_timestamp": invite_dt,
                    "event_properties": f'{{"member_email": "{fake.email()}"}}',
                })
        else:
            # Non-activated: 30% invite exactly 1, 70% invite 0
            if random.random() < 0.30:
                invite_dt = signup_dt + timedelta(
                    days=random.randint(0, 30),
                    hours=random.randint(0, 23),
                    minutes=random.randint(0, 59),
                )
                events_for_user.append({
                    "event_id": str(uuid.uuid4()),
                    "user_id": user_id,
                    "event_type": "member_invited",
                    "event_timestamp": invite_dt,
                    "event_properties": f'{{"member_email": "{fake.email()}"}}',
                })

        # Generate remaining events with time decay
        remaining = max(0, num_events - len(events_for_user))

        # Activated users have more sustained activity
        if is_activated:
            active_span = min(days_since_signup, max(60, days_since_signup))
        else:
            # Non-activated users trail off quickly
            active_span = min(days_since_signup, random.randint(7, 90))

        event_type_weights = {
            "task_created": 0.30,
            "task_completed": 0.22,
            "comment_added": 0.20,
            "file_uploaded": 0.10,
            "board_created": 0.05,
            "member_invited": 0.03,
            "settings_changed": 0.10,
        }
        et_keys = list(event_type_weights.keys())
        et_weights = list(event_type_weights.values())

        for _ in range(remaining):
            # Exponential decay for event timing
            day_offset = int(np.random.exponential(scale=active_span * 0.3))
            day_offset = min(day_offset, active_span)
            evt_dt = signup_dt + timedelta(
                days=day_offset,
                hours=random.randint(0, 23),
                minutes=random.randint(0, 59),
                seconds=random.randint(0, 59),
            )
            if evt_dt > END_DATE:
                evt_dt = END_DATE - timedelta(minutes=random.randint(1, 1440))
            if evt_dt < signup_dt:
                evt_dt = signup_dt + timedelta(minutes=random.randint(10, 300))

            evt_type = random.choices(et_keys, weights=et_weights, k=1)[0]
            events_for_user.append({
                "event_id": str(uuid.uuid4()),
                "user_id": user_id,
                "event_type": evt_type,
                "event_timestamp": evt_dt,
                "event_properties": "{}",
            })

        all_events.extend(events_for_user)

    df = pd.DataFrame(all_events)
    df = df.sort_values("event_timestamp").reset_index(drop=True)
    print(f"  Generated {len(df):,} events")
    return df


# ── 3. Generate Subscriptions ─────────────────────────────────────────
def generate_subscriptions(users_df: pd.DataFrame, events_df: pd.DataFrame) -> pd.DataFrame:
    print("Generating subscriptions...")

    # Determine which users get activated (invited ≥2 in week 1)
    week1_invites = events_df[events_df["event_type"] == "member_invited"].copy()
    week1_invites["signup"] = week1_invites["user_id"].map(
        users_df.set_index("user_id")["signed_up_at"]
    )
    week1_invites["days_after"] = (
        week1_invites["event_timestamp"] - week1_invites["signup"]
    ).dt.total_seconds() / 86400
    week1_invites = week1_invites[week1_invites["days_after"] <= 7]
    invites_per_user = week1_invites.groupby("user_id").size()
    activated_users = set(invites_per_user[invites_per_user >= 2].index)

    subscriptions = []
    sub_id = 0

    for _, user_row in users_df.iterrows():
        user_id = user_row["user_id"]
        signup_dt = user_row["signed_up_at"]
        region = user_row["region"]
        is_activated = user_id in activated_users

        # Conversion rates:
        # Activated: 11.3% convert to pro
        # Non-activated: 2.1% convert to pro
        if is_activated:
            convert_prob = 0.113
            enterprise_prob = 0.03  # some go straight to enterprise
        else:
            convert_prob = 0.021
            enterprise_prob = 0.003

        # Australia has higher Enterprise mix
        if region == "AU":
            enterprise_prob *= 2.5
            convert_prob *= 1.15

        # Everyone starts on free
        sub_id += 1
        free_start = signup_dt
        current_plan = "free"
        subs_for_user = []

        subs_for_user.append({
            "subscription_id": f"sub_{sub_id:06d}",
            "user_id": user_id,
            "plan": "free",
            "mrr": 0.00,
            "started_at": free_start,
            "ended_at": None,
            "change_type": "new",
        })

        # Decide upgrade
        days_avail = (END_DATE - signup_dt).days
        if days_avail < 14:
            continue

        if random.random() < convert_prob:
            # Upgrade to pro
            upgrade_day = random.randint(14, min(90, days_avail))
            upgrade_dt = signup_dt + timedelta(days=upgrade_day)

            # End free subscription
            subs_for_user[-1]["ended_at"] = upgrade_dt

            sub_id += 1
            subs_for_user.append({
                "subscription_id": f"sub_{sub_id:06d}",
                "user_id": user_id,
                "plan": "pro",
                "mrr": 29.00,
                "started_at": upgrade_dt,
                "ended_at": None,
                "change_type": "expansion",
            })
            current_plan = "pro"

            # Some pro users upgrade to enterprise
            remaining_days = (END_DATE - upgrade_dt).days
            if remaining_days > 30 and random.random() < enterprise_prob / convert_prob * 0.5:
                ent_day = random.randint(30, min(180, remaining_days))
                ent_dt = upgrade_dt + timedelta(days=ent_day)

                subs_for_user[-1]["ended_at"] = ent_dt
                sub_id += 1
                subs_for_user.append({
                    "subscription_id": f"sub_{sub_id:06d}",
                    "user_id": user_id,
                    "plan": "enterprise",
                    "mrr": 99.00,
                    "started_at": ent_dt,
                    "ended_at": None,
                    "change_type": "expansion",
                })
                current_plan = "enterprise"

            # Churn: some paid users cancel
            if current_plan in ("pro", "enterprise"):
                last_sub = subs_for_user[-1]
                remaining = (END_DATE - last_sub["started_at"]).days
                churn_rate = 0.15 if not is_activated else 0.05
                if remaining > 30 and random.random() < churn_rate:
                    churn_day = random.randint(30, min(remaining, 365))
                    churn_dt = last_sub["started_at"] + timedelta(days=churn_day)
                    last_sub["ended_at"] = churn_dt
                    last_sub["change_type"] = last_sub["change_type"]  # keep original

                    sub_id += 1
                    subs_for_user.append({
                        "subscription_id": f"sub_{sub_id:06d}",
                        "user_id": user_id,
                        "plan": "free",
                        "mrr": 0.00,
                        "started_at": churn_dt,
                        "ended_at": None,
                        "change_type": "churn",
                    })

        elif random.random() < enterprise_prob:
            # Direct to enterprise (rare)
            upgrade_day = random.randint(14, min(60, days_avail))
            upgrade_dt = signup_dt + timedelta(days=upgrade_day)
            subs_for_user[-1]["ended_at"] = upgrade_dt

            sub_id += 1
            subs_for_user.append({
                "subscription_id": f"sub_{sub_id:06d}",
                "user_id": user_id,
                "plan": "enterprise",
                "mrr": 99.00,
                "started_at": upgrade_dt,
                "ended_at": None,
                "change_type": "expansion",
            })

        subscriptions.extend(subs_for_user)

    df = pd.DataFrame(subscriptions)
    print(f"  Generated {len(df):,} subscription records")
    return df


# ── 4. Generate Feature Usage ─────────────────────────────────────────
def generate_feature_usage(users_df: pd.DataFrame, events_df: pd.DataFrame) -> pd.DataFrame:
    print("Generating feature usage...")

    # Get active date range per user from events
    user_dates = events_df.groupby("user_id")["event_timestamp"].agg(["min", "max"])

    records = []
    users_list = users_df["user_id"].tolist()

    # Power-law distribution for feature usage per user
    usage_counts = np.random.pareto(a=1.5, size=len(users_list)) + 1
    usage_counts = (usage_counts / usage_counts.sum() * NUM_FEATURE_USAGE_TARGET).astype(int)
    usage_counts = np.maximum(usage_counts, 1)

    pro_features = {"automations", "custom_fields", "reporting", "api_access", "workload_view"}
    enterprise_features = {"sso_login", "guest_access"}

    for idx, user_id in enumerate(users_list):
        if user_id not in user_dates.index:
            continue

        user_min = user_dates.loc[user_id, "min"]
        user_max = user_dates.loc[user_id, "max"]
        n_records = int(usage_counts[idx])
        span_seconds = max(1, int((user_max - user_min).total_seconds()))

        available_features = [f for f in FEATURES if f not in enterprise_features and f not in pro_features]
        # Add more features for random selection
        available_features += list(pro_features)

        for _ in range(n_records):
            feature = random.choice(available_features)
            offset = random.randint(0, span_seconds)
            ts = user_min + timedelta(seconds=offset)
            records.append({
                "usage_id": str(uuid.uuid4()),
                "user_id": user_id,
                "feature_name": feature,
                "used_at": ts,
                "duration_seconds": random.randint(5, 3600),
            })

    df = pd.DataFrame(records)
    df = df.sort_values("used_at").reset_index(drop=True)
    print(f"  Generated {len(df):,} feature usage records")
    return df


# ── 5. Generate Invoices (from subscriptions) ─────────────────────────
def generate_invoices(subscriptions_df: pd.DataFrame) -> pd.DataFrame:
    print("Generating invoices...")

    paid_subs = subscriptions_df[subscriptions_df["mrr"] > 0].copy()
    invoices = []
    inv_id = 0

    for _, sub in paid_subs.iterrows():
        start = sub["started_at"]
        end = sub["ended_at"] if pd.notna(sub["ended_at"]) else END_DATE
        month_start = start.replace(day=1)

        while month_start <= end:
            inv_id += 1
            invoice_dt = month_start + timedelta(days=random.randint(0, 2))
            if invoice_dt < start:
                invoice_dt = start
            status = "paid" if random.random() < 0.97 else "failed"
            invoices.append({
                "invoice_id": f"inv_{inv_id:07d}",
                "subscription_id": sub["subscription_id"],
                "user_id": sub["user_id"],
                "amount": sub["mrr"],
                "currency": "usd",
                "status": status,
                "issued_at": invoice_dt,
            })
            # Next month
            if month_start.month == 12:
                month_start = month_start.replace(year=month_start.year + 1, month=1)
            else:
                month_start = month_start.replace(month=month_start.month + 1)

    df = pd.DataFrame(invoices)
    print(f"  Generated {len(df):,} invoices")
    return df


# ── 6. Generate Intercom Conversations ────────────────────────────────
def generate_conversations(users_df: pd.DataFrame) -> pd.DataFrame:
    print("Generating support conversations...")

    topics = [
        "billing_inquiry", "feature_request", "bug_report",
        "onboarding_help", "account_issue", "integration_help",
        "plan_upgrade", "cancellation_request",
    ]
    statuses = ["open", "closed", "snoozed"]

    convos = []
    # ~20% of users contact support
    support_users = users_df.sample(frac=0.20, random_state=SEED)

    for _, user_row in support_users.iterrows():
        n_convos = random.choices([1, 2, 3, 4], weights=[0.5, 0.3, 0.15, 0.05], k=1)[0]
        signup_dt = user_row["signed_up_at"]
        days_avail = (END_DATE - signup_dt).days

        for _ in range(n_convos):
            if days_avail <= 1:
                break
            day_offset = random.randint(1, days_avail)
            created_dt = signup_dt + timedelta(
                days=day_offset,
                hours=random.randint(0, 23),
                minutes=random.randint(0, 59),
            )
            resolved_dt = None
            status = random.choices(statuses, weights=[0.1, 0.8, 0.1], k=1)[0]
            if status == "closed":
                resolved_dt = created_dt + timedelta(
                    hours=random.randint(1, 72),
                    minutes=random.randint(0, 59),
                )

            convos.append({
                "conversation_id": str(uuid.uuid4()),
                "user_id": user_row["user_id"],
                "topic": random.choice(topics),
                "status": status,
                "created_at": created_dt,
                "resolved_at": resolved_dt,
                "messages_count": random.randint(2, 15),
                "first_response_minutes": random.randint(5, 1440),
            })

    df = pd.DataFrame(convos)
    print(f"  Generated {len(df):,} conversations")
    return df


# ── 7. Remove post-churn events ───────────────────────────────────────
def clean_post_churn_events(events_df: pd.DataFrame, subs_df: pd.DataFrame) -> pd.DataFrame:
    """Remove events that occur after a user churned."""
    churn_records = subs_df[subs_df["change_type"] == "churn"][["user_id", "started_at"]]
    if churn_records.empty:
        return events_df

    churn_dates = churn_records.groupby("user_id")["started_at"].min().reset_index()
    churn_dates.columns = ["user_id", "churn_date"]

    merged = events_df.merge(churn_dates, on="user_id", how="left")
    mask = merged["churn_date"].isna() | (merged["event_timestamp"] <= merged["churn_date"])
    cleaned = merged[mask].drop(columns=["churn_date"])
    removed = len(events_df) - len(cleaned)
    if removed > 0:
        print(f"  Removed {removed:,} post-churn events")
    return cleaned


# ── 8. Load to PostgreSQL ─────────────────────────────────────────────
def load_to_postgres(dataframes: dict[str, pd.DataFrame]):
    print("\nLoading data to PostgreSQL...")
    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = True
    cur = conn.cursor()

    cur.execute(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}")

    for table_name, df in dataframes.items():
        full_table = f"{SCHEMA}.{table_name}"
        print(f"  Loading {full_table} ({len(df):,} rows)...")

        cur.execute(f"DROP TABLE IF EXISTS {full_table} CASCADE")

        # Build CREATE TABLE from DataFrame dtypes
        col_defs = []
        for col in df.columns:
            dtype = df[col].dtype
            if col.endswith("_id") and "uuid" not in str(df[col].iloc[0] if len(df) > 0 else ""):
                col_defs.append(f'"{col}" VARCHAR(50)')
            elif "uuid" in str(df[col].iloc[0] if len(df) > 0 else ""):
                col_defs.append(f'"{col}" VARCHAR(36)')
            elif pd.api.types.is_datetime64_any_dtype(dtype):
                col_defs.append(f'"{col}" TIMESTAMP')
            elif pd.api.types.is_float_dtype(dtype):
                col_defs.append(f'"{col}" NUMERIC(10,2)')
            elif pd.api.types.is_integer_dtype(dtype):
                col_defs.append(f'"{col}" INTEGER')
            elif pd.api.types.is_bool_dtype(dtype):
                col_defs.append(f'"{col}" BOOLEAN')
            else:
                col_defs.append(f'"{col}" TEXT')

        create_sql = f"CREATE TABLE {full_table} ({', '.join(col_defs)})"
        cur.execute(create_sql)

        # Insert data in batches
        if len(df) > 0:
            cols = df.columns.tolist()
            col_str = ", ".join(f'"{c}"' for c in cols)
            # Convert timestamps to Python datetime, handle NaT
            records = []
            for _, row in df.iterrows():
                vals = []
                for c in cols:
                    v = row[c]
                    if pd.isna(v):
                        vals.append(None)
                    elif isinstance(v, pd.Timestamp):
                        vals.append(v.to_pydatetime())
                    else:
                        vals.append(v)
                records.append(tuple(vals))

            template = f"({', '.join(['%s'] * len(cols))})"
            batch_size = 5000
            for i in range(0, len(records), batch_size):
                batch = records[i:i + batch_size]
                execute_values(
                    cur,
                    f"INSERT INTO {full_table} ({col_str}) VALUES %s",
                    batch,
                    template=template,
                )

        print(f"    ✓ {full_table} loaded")

    cur.close()
    conn.close()
    print("\nAll data loaded successfully!")


# ── Main ──────────────────────────────────────────────────────────────
def main():
    print("=" * 60)
    print("FlowBoard Analytics — Synthetic Data Generator")
    print("=" * 60)

    # Generate
    users_df = generate_users()
    events_df = generate_events(users_df)
    subs_df = generate_subscriptions(users_df, events_df)
    events_df = clean_post_churn_events(events_df, subs_df)
    feature_df = generate_feature_usage(users_df, events_df)
    invoices_df = generate_invoices(subs_df)
    conversations_df = generate_conversations(users_df)

    # Summary stats
    print("\n-- Summary --")
    print(f"  Users:         {len(users_df):>10,}")
    print(f"  Events:        {len(events_df):>10,}")
    print(f"  Subscriptions: {len(subs_df):>10,}")
    print(f"  Feature Usage: {len(feature_df):>10,}")
    print(f"  Invoices:      {len(invoices_df):>10,}")
    print(f"  Conversations: {len(conversations_df):>10,}")

    # Activation insight check
    week1_invites = events_df[events_df["event_type"] == "member_invited"].copy()
    week1_invites["signup"] = week1_invites["user_id"].map(
        users_df.set_index("user_id")["signed_up_at"]
    )
    week1_invites["days_after"] = (
        week1_invites["event_timestamp"] - week1_invites["signup"]
    ).dt.total_seconds() / 86400
    week1_only = week1_invites[week1_invites["days_after"] <= 7]
    activated = set(week1_only.groupby("user_id").size()[lambda x: x >= 2].index)
    paid_users = set(subs_df[subs_df["plan"].isin(["pro", "enterprise"])]["user_id"])

    activated_paid = len(activated & paid_users)
    activated_total = len(activated)
    non_activated = set(users_df["user_id"]) - activated
    non_activated_paid = len(non_activated & paid_users)
    non_activated_total = len(non_activated)

    act_rate = activated_paid / activated_total * 100 if activated_total > 0 else 0
    non_act_rate = non_activated_paid / non_activated_total * 100 if non_activated_total > 0 else 0
    print(f"\n-- Activation Insight --")
    print(f"  Activated users (>=2 invites in week 1): {activated_total:,}")
    print(f"  Activated -> Paid conversion: {act_rate:.1f}%")
    print(f"  Non-activated -> Paid conversion: {non_act_rate:.1f}%")
    print(f"  Conversion lift: {act_rate / non_act_rate:.1f}x" if non_act_rate > 0 else "")

    # Load to PostgreSQL
    load_to_postgres({
        "raw_users": users_df,
        "raw_user_events": events_df,
        "raw_subscriptions": subs_df,
        "raw_feature_usage": feature_df,
        "raw_invoices": invoices_df,
        "raw_conversations": conversations_df,
    })


if __name__ == "__main__":
    main()
