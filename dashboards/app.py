"""
FlowBoard Analytics Dashboard
Streamlit app connecting to the dbt mart layer in PostgreSQL.
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import psycopg2

# ── Config ──────────────────────────────────────────────────────────────
st.set_page_config(page_title="FlowBoard Analytics", layout="wide")

DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "user": "portfolio",
    "password": "portfolio_dev",
    "dbname": "portfolio",
}


@st.cache_resource
def get_connection():
    return psycopg2.connect(**DB_CONFIG)


@st.cache_data(ttl=300)
def run_query(query: str) -> pd.DataFrame:
    conn = get_connection()
    try:
        return pd.read_sql(query, conn)
    except Exception:
        conn.reset()
        return pd.read_sql(query, conn)


# ── Sidebar ─────────────────────────────────────────────────────────────
st.sidebar.title("FlowBoard Analytics")
page = st.sidebar.radio(
    "Navigate",
    ["Product Health", "Funnel Analysis", "Cohort Retention", "Revenue"],
)

# Filters
regions = st.sidebar.multiselect(
    "Region", ["US", "UK", "AU"], default=["US", "UK", "AU"]
)
sources = st.sidebar.multiselect(
    "Signup Source",
    ["organic", "google_ads", "referral", "product_hunt", "content_marketing"],
    default=["organic", "google_ads", "referral", "product_hunt", "content_marketing"],
)

region_filter = ",".join(f"'{r}'" for r in regions) if regions else "'US','UK','AU'"
source_filter = ",".join(f"'{s}'" for s in sources) if sources else "'organic'"


# ── Page 1: Product Health ──────────────────────────────────────────────
if page == "Product Health":
    st.title("Product Health Overview")

    col1, col2, col3, col4 = st.columns(4)

    # Total users
    total_users = run_query(f"""
        SELECT count(*) as n FROM flowboard.dim_users
        WHERE region IN ({region_filter}) AND signup_source IN ({source_filter})
    """)
    col1.metric("Total Users", f"{total_users['n'].iloc[0]:,}")

    # Activation rate (reached milestone 3: invited member)
    activation = run_query(f"""
        SELECT
            round(100.0 * sum(case when max_funnel_stage >= 3 then 1 else 0 end) / count(*), 1) as rate
        FROM flowboard.fct_user_funnel_snapshot
        WHERE region IN ({region_filter}) AND signup_source IN ({source_filter})
    """)
    col2.metric("Activation Rate", f"{activation['rate'].iloc[0]}%")

    # DAU/WAU/MAU ratio (last 30 days)
    engagement = run_query("""
        SELECT
            count(distinct case when event_date = current_date - 1 then user_id end) as dau,
            count(distinct case when event_date >= current_date - 7 then user_id end) as wau,
            count(distinct case when event_date >= current_date - 30 then user_id end) as mau
        FROM flowboard.fct_user_events
    """)
    wau = engagement['wau'].iloc[0]
    mau = engagement['mau'].iloc[0]
    ratio = round(wau / mau * 100, 1) if mau > 0 else 0
    col3.metric("WAU/MAU Ratio", f"{ratio}%")

    # Current MRR
    mrr = run_query("""
        SELECT ending_mrr FROM flowboard.int_subscriptions_mrr
        ORDER BY report_month DESC LIMIT 1
    """)
    col4.metric("Current MRR", f"${mrr['ending_mrr'].iloc[0]:,.0f}")

    # Signups trend
    st.subheader("Weekly Signups")
    signups = run_query(f"""
        SELECT signup_week, count(*) as signups
        FROM flowboard.dim_users
        WHERE region IN ({region_filter}) AND signup_source IN ({source_filter})
        GROUP BY signup_week ORDER BY signup_week
    """)
    fig = px.area(signups, x="signup_week", y="signups", title="Weekly Signup Trend")
    fig.update_layout(xaxis_title="Week", yaxis_title="New Signups")
    st.plotly_chart(fig, use_container_width=True)

    # Activation by source
    st.subheader("Activation by Signup Source")
    act_source = run_query(f"""
        SELECT
            signup_source,
            count(*) as total,
            sum(case when is_activated then 1 else 0 end) as activated,
            round(100.0 * sum(case when is_activated then 1 else 0 end) / count(*), 1) as activation_rate
        FROM flowboard.dim_users
        WHERE region IN ({region_filter})
        GROUP BY signup_source ORDER BY activation_rate DESC
    """)
    fig = px.bar(act_source, x="signup_source", y="activation_rate",
                 color="signup_source", title="Activation Rate by Source",
                 text="activation_rate")
    fig.update_layout(yaxis_title="Activation Rate (%)", showlegend=False)
    st.plotly_chart(fig, use_container_width=True)


# ── Page 2: Funnel Analysis ────────────────────────────────────────────
elif page == "Funnel Analysis":
    st.title("Conversion Funnel")

    funnel_data = run_query(f"""
        SELECT
            count(*) as signed_up,
            sum(case when completed_board then 1 else 0 end) as created_board,
            sum(case when completed_invite then 1 else 0 end) as invited_member,
            sum(case when completed_week2 then 1 else 0 end) as active_week2,
            sum(case when completed_conversion then 1 else 0 end) as converted
        FROM flowboard.fct_user_funnel_snapshot
        WHERE region IN ({region_filter}) AND signup_source IN ({source_filter})
    """)

    stages = ["Signed Up", "Created Board", "Invited Member", "Active Week 2", "Converted"]
    values = [
        int(funnel_data["signed_up"].iloc[0]),
        int(funnel_data["created_board"].iloc[0]),
        int(funnel_data["invited_member"].iloc[0]),
        int(funnel_data["active_week2"].iloc[0]),
        int(funnel_data["converted"].iloc[0]),
    ]

    fig = go.Figure(go.Funnel(
        y=stages,
        x=values,
        textinfo="value+percent initial+percent previous",
        marker=dict(color=["#636EFA", "#EF553B", "#00CC96", "#AB63FA", "#FFA15A"]),
    ))
    fig.update_layout(title="User Conversion Funnel", height=500)
    st.plotly_chart(fig, use_container_width=True)

    # Funnel by region
    st.subheader("Funnel by Region")
    funnel_region = run_query(f"""
        SELECT
            region,
            count(*) as total,
            round(100.0 * sum(case when completed_board then 1 else 0 end) / count(*), 1) as board_rate,
            round(100.0 * sum(case when completed_invite then 1 else 0 end) / count(*), 1) as invite_rate,
            round(100.0 * sum(case when completed_week2 then 1 else 0 end) / count(*), 1) as week2_rate,
            round(100.0 * sum(case when completed_conversion then 1 else 0 end) / count(*), 1) as conversion_rate
        FROM flowboard.fct_user_funnel_snapshot
        WHERE signup_source IN ({source_filter})
        GROUP BY region ORDER BY region
    """)
    st.dataframe(funnel_region, use_container_width=True)

    # Activation insight
    st.subheader("Key Insight: Activation Impact")
    insight = run_query(f"""
        SELECT
            is_activated,
            count(*) as users,
            sum(case when completed_conversion then 1 else 0 end) as converted,
            round(100.0 * sum(case when completed_conversion then 1 else 0 end) / count(*), 1) as conversion_rate
        FROM flowboard.fct_user_funnel_snapshot
        WHERE region IN ({region_filter}) AND signup_source IN ({source_filter})
        GROUP BY is_activated
    """)
    insight["segment"] = insight["is_activated"].map({True: "Activated (2+ invites week 1)", False: "Not Activated"})

    fig = px.bar(insight, x="segment", y="conversion_rate", color="segment",
                 title="Conversion Rate: Activated vs Non-Activated Users",
                 text="conversion_rate")
    fig.update_layout(yaxis_title="Conversion Rate (%)", showlegend=False)
    st.plotly_chart(fig, use_container_width=True)

    st.info(
        "Users who invite 2+ team members within their first week convert at "
        f"**{insight[insight['is_activated']==True]['conversion_rate'].iloc[0]}%** vs "
        f"**{insight[insight['is_activated']==False]['conversion_rate'].iloc[0]}%** for non-activated users."
    )


# ── Page 3: Cohort Retention ───────────────────────────────────────────
elif page == "Cohort Retention":
    st.title("Cohort Retention Analysis")

    filter_by = st.radio("Segment by", ["Overall", "Region", "Signup Source"], horizontal=True)

    if filter_by == "Overall":
        retention = run_query(f"""
            SELECT
                cohort_week,
                retention_day,
                round(100.0 * avg(is_retained), 1) as retention_rate,
                count(*) as cohort_size
            FROM flowboard.int_users_retention_flags
            WHERE region IN ({region_filter}) AND signup_source IN ({source_filter})
            GROUP BY cohort_week, retention_day
            ORDER BY cohort_week, retention_day
        """)
    elif filter_by == "Region":
        retention = run_query(f"""
            SELECT
                region as segment,
                retention_day,
                round(100.0 * avg(is_retained), 1) as retention_rate
            FROM flowboard.int_users_retention_flags
            WHERE region IN ({region_filter}) AND signup_source IN ({source_filter})
            GROUP BY region, retention_day
            ORDER BY region, retention_day
        """)
    else:
        retention = run_query(f"""
            SELECT
                signup_source as segment,
                retention_day,
                round(100.0 * avg(is_retained), 1) as retention_rate
            FROM flowboard.int_users_retention_flags
            WHERE region IN ({region_filter}) AND signup_source IN ({source_filter})
            GROUP BY signup_source, retention_day
            ORDER BY signup_source, retention_day
        """)

    if filter_by == "Overall":
        # Heatmap
        pivot = retention.pivot_table(
            index="cohort_week", columns="retention_day", values="retention_rate"
        )
        # Only show cohorts with enough data
        pivot = pivot.dropna(thresh=3)
        if not pivot.empty:
            # Take last 20 cohorts for readability
            pivot = pivot.tail(20)
            fig = px.imshow(
                pivot,
                labels=dict(x="Retention Day", y="Cohort Week", color="Retention %"),
                title="Retention Heatmap by Signup Cohort",
                color_continuous_scale="YlOrRd_r",
                aspect="auto",
            )
            fig.update_layout(height=600)
            st.plotly_chart(fig, use_container_width=True)
    else:
        fig = px.line(
            retention, x="retention_day", y="retention_rate", color="segment",
            title=f"Retention Curve by {filter_by}",
            markers=True,
        )
        fig.update_layout(xaxis_title="Days Since Signup", yaxis_title="Retention Rate (%)")
        st.plotly_chart(fig, use_container_width=True)

    # Summary table
    st.subheader("Retention Summary")
    summary = run_query(f"""
        SELECT
            retention_day,
            round(100.0 * avg(is_retained), 1) as overall_rate,
            count(distinct user_id) as users
        FROM flowboard.int_users_retention_flags
        WHERE region IN ({region_filter}) AND signup_source IN ({source_filter})
        GROUP BY retention_day ORDER BY retention_day
    """)
    st.dataframe(summary, use_container_width=True)


# ── Page 4: Revenue ────────────────────────────────────────────────────
elif page == "Revenue":
    st.title("Revenue Analytics")

    # MRR metrics
    mrr_data = run_query("""
        SELECT * FROM flowboard.int_subscriptions_mrr ORDER BY report_month
    """)

    col1, col2, col3 = st.columns(3)
    latest = mrr_data.iloc[-1]
    col1.metric("Ending MRR", f"${latest['ending_mrr']:,.0f}")
    col2.metric("Net New MRR", f"${latest['net_new_mrr']:,.0f}")
    col3.metric("Churn MRR", f"-${latest['churn_mrr']:,.0f}")

    # MRR Waterfall
    st.subheader("MRR Waterfall")
    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=mrr_data["report_month"], y=mrr_data["new_mrr"],
        name="New MRR", marker_color="#00CC96"
    ))
    fig.add_trace(go.Bar(
        x=mrr_data["report_month"], y=mrr_data["expansion_mrr"],
        name="Expansion", marker_color="#636EFA"
    ))
    fig.add_trace(go.Bar(
        x=mrr_data["report_month"], y=-mrr_data["churn_mrr"],
        name="Churn", marker_color="#EF553B"
    ))
    fig.add_trace(go.Scatter(
        x=mrr_data["report_month"], y=mrr_data["ending_mrr"],
        name="Ending MRR", line=dict(color="white", width=2),
        yaxis="y2"
    ))
    fig.update_layout(
        barmode="relative",
        title="Monthly MRR Waterfall",
        yaxis_title="MRR Change ($)",
        yaxis2=dict(title="Ending MRR ($)", overlaying="y", side="right"),
        height=500,
    )
    st.plotly_chart(fig, use_container_width=True)

    # MRR trend
    st.subheader("MRR Growth")
    fig = px.area(mrr_data, x="report_month", y="ending_mrr",
                  title="Monthly Recurring Revenue (MRR)")
    fig.update_layout(yaxis_title="MRR ($)")
    st.plotly_chart(fig, use_container_width=True)

    # NRR by cohort
    st.subheader("Revenue by Region")
    rev_region = run_query("""
        SELECT
            region,
            count(distinct user_id) as paying_users,
            sum(current_mrr) as total_mrr,
            round(avg(current_mrr), 2) as arpu
        FROM flowboard.dim_users
        WHERE current_plan != 'free'
        GROUP BY region ORDER BY total_mrr DESC
    """)
    st.dataframe(rev_region, use_container_width=True)

    # ARPU trend by plan
    st.subheader("Plan Distribution")
    plan_dist = run_query("""
        SELECT
            current_plan,
            count(*) as users,
            sum(current_mrr) as total_mrr
        FROM flowboard.dim_users
        GROUP BY current_plan ORDER BY total_mrr DESC
    """)
    fig = px.pie(plan_dist, values="users", names="current_plan",
                 title="Users by Plan", hole=0.4)
    st.plotly_chart(fig, use_container_width=True)


# ── Footer ──────────────────────────────────────────────────────────────
st.sidebar.markdown("---")
st.sidebar.markdown("**FlowBoard Analytics** | Built with dbt + Streamlit")
