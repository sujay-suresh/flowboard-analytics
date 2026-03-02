# FlowBoard Analytics

## Project Type
dbt + Python SaaS product analytics pipeline (PostgreSQL backend). Models user activation, funnel conversion, MRR waterfall, and retention for a fictional project management tool.

## Stack
- **Database:** PostgreSQL — localhost:5432, user: portfolio, password: portfolio_dev, db: portfolio, schema: `flowboard`
- **dbt:** dbt-core + dbt-postgres. Python venv in `.venv/`
- **Python:** 3.8+ (Faker, NumPy, Pandas, psycopg2)
- **Packages:** dbt-utils (1.3.3), dbt-expectations (0.10.4), dbt-date (0.10.1)

## Commands
```bash
# Activate venv
source .venv/Scripts/activate    # Windows bash

# Data generation
python scripts/generate_synthetic_data.py

# dbt pipeline
dbt deps
dbt build --full-refresh
dbt test

# Docs
dbt docs generate && dbt docs serve
```

## Key Files
| File | Purpose |
|------|---------|
| `scripts/generate_synthetic_data.py` | Generates 8K users, 2M events, 12K subscriptions, 500K feature usage records |
| `macros/cents_to_dollars.sql` | Multi-adapter macro (Postgres, BigQuery, Fabric) |

## Data Architecture

### Sources (3 simulated APIs)
- **App:** raw_users (8K), raw_user_events (2M), raw_feature_usage (500K)
- **Stripe:** raw_subscriptions (12K), raw_invoices (derived)
- **Intercom:** raw_conversations (derived)

### Layers
| Layer | Materialization | Models |
|-------|----------------|--------|
| Staging | VIEW | 6 models: `stg_app__*`, `stg_stripe__*`, `stg_intercom__*` |
| Intermediate | VIEW/TABLE | 4 models: `int_users_funnel_stages`, `int_users_activation_scored`, `int_users_retention_flags`, `int_subscriptions_mrr` |
| Marts | TABLE | 7 models: `dim_users`, `dim_date`, `dim_features`, `dim_plans`, `fct_user_events` (incremental), `fct_subscriptions`, `fct_user_funnel_snapshot` |

### Star Schema
- **Facts:** `fct_user_events` (incremental, grain: event), `fct_subscriptions` (grain: subscription period), `fct_user_funnel_snapshot` (accumulating snapshot)
- **Dimensions:** `dim_users`, `dim_date` (2024-09 to 2026-03), `dim_features` (15 features, plan-gated), `dim_plans` (free/pro/enterprise)

## Business Logic

### Funnel Stages (5 milestones)
1. Signup (100%)
2. Board Created (action milestone)
3. Member Invited (collaboration signal)
4. Active in Week 2 (retention signal)
5. Upgraded to Paid (conversion goal)

### Activation Scoring (0-100 scale)
- Board created: 20 pts
- Members invited: 40 pts (heaviest — collaboration is key signal)
- Tasks created: 20 pts
- Active days: 20 pts
- Activated = invited >=2 members in week 1
- **Key insight:** Activated users convert at 11.3% vs 2.1% (5.4x lift)

### MRR Waterfall
- Monthly grain: new_mrr, expansion_mrr, churn_mrr, contraction_mrr
- Running total: `ending_mrr = cumsum(net_new_mrr)`

### Retention Flags
- 6 windows: D1, D7, D14, D30, D60, D90

## Testing
- **YAML tests:** unique, not_null, accepted_values, relationships on all mart models
- **4 custom SQL tests:**
  - `assert_funnel_milestones_chronological.sql` — signup < board < invite < week2 < paid
  - `assert_mrr_reconciliation.sql` — MRR waterfall sums check
  - `assert_no_events_before_signup.sql` — temporal integrity
  - `assert_retention_flags_boolean.sql` — flags are 0 or 1

## Synthetic Data
- Seed: 42 (deterministic)
- Date range: 2024-09-01 to 2026-02-28
- Sigmoid signup growth curve
- Power-law event distribution (Pareto a=1.2)
- 15% users activated (>=2 invites in week 1)
- Churn: 5% activated, 15% non-activated
- Plan tiers: Free (7 features), Pro (+4), Enterprise (+3)
- Regions: US 50%, UK 30%, AU 20%

## Conventions
- Staging: `stg_<source>__<entity>`
- Intermediate: `int_<entity>_<verb>`
- Marts: `dim_<entity>`, `fct_<entity>`
- No dashboard included — tables designed for BI tool connections
