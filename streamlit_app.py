import streamlit as st
import datetime
import time
import pandas as pd
from snowflake.snowpark import Session

st.set_page_config(page_title="Live Dashboard", layout="wide")

# Create Snowflake session
connection_parameters = dict(st.secrets["snowflake"])
session = Session.builder.configs(connection_parameters).create()

# Get list of MadeMedia charities
made_media_charities = [
    "Armed Services Ymca",
    "United Service Organization",
    "No Kid Hungry",
    "American Lung Association",
    "Care"
]

# Compute time offsets
now = datetime.datetime.now() - datetime.timedelta(hours=4)
today = now.date()
yesterday = today - datetime.timedelta(days=1)
last_week = today - datetime.timedelta(days=7)
cutoff_display_time = now.strftime("%-I:%M %p")

# Time labels
interval_time_str = now.strftime('%H:%M:%S')
yesterday_str = f"{yesterday} {interval_time_str}"
lastweek_str = f"{last_week} {interval_time_str}"
yesterday_label = f"Yesterday @ {cutoff_display_time} ET"
lastweek_label = f"Last Week @ {cutoff_display_time} ET"

# Get historical counts
historical_query = f"""
    WITH base_data AS (
        SELECT DONOR_ACQ_DTM AS dt FROM FIVETRAN_DATABASE.ARMOR_SQL_2012_DBO.LOGGING_BY_GFDID
        WHERE DONOR_STATUS = 'Successful' AND GIFT_TYPE = 'Recurring' AND REPCODE NOT LIKE 'TEST%'
        UNION ALL
        SELECT DATEADD(HOUR, -4, PAYMENT_DATE) AS dt FROM MADEMEDIA.RAW.MADEMEDIA_PROD
        WHERE DONOR_STATUS = 'approved'
    )
    SELECT 
        COUNT_IF(DATE(dt) = DATE('{yesterday}')) AS count_yesterday,
        COUNT_IF(DATE(dt) = DATE('{last_week}')) AS count_last_week,
        COUNT_IF(DATE(dt) = DATE('{yesterday}') AND dt < TO_TIMESTAMP('{yesterday_str}')) AS count_yesterday_block,
        COUNT_IF(DATE(dt) = DATE('{last_week}') AND dt < TO_TIMESTAMP('{lastweek_str}')) AS count_last_week_block
    FROM base_data
"""
historical_df = session.sql(historical_query).to_pandas()

count_yesterday = historical_df["COUNT_YESTERDAY"].iloc[0]
count_last_week = historical_df["COUNT_LAST_WEEK"].iloc[0]
count_yesterday_block = historical_df["COUNT_YESTERDAY_BLOCK"].iloc[0]
count_last_week_block = historical_df["COUNT_LAST_WEEK_BLOCK"].iloc[0]

# Outages query and display
outage_query = f"""
    SELECT 
        OUTAGE_NAME,
        UPPER(STATUS) AS STATUS,
        INITCAP(SERVICE) AS SERVICE,
        GFD_STATUS,
        DATEADD('hour', -4, CREATED_DATETIME) AS CREATED_DATETIME,
        DATEADD('hour', -4, UPDATED_DATETIME) AS UPDATED_DATETIME,
        DESCRIPTION,
        URL
    FROM FIVETRAN_DATABASE.MAKE.OUTAGE_LOG
    WHERE GFD_STATUS <> 'No Impact'
      AND (CAST(DATEADD('hour', -4, CREATED_DATETIME) AS DATE) = '{today}'
           OR CAST(DATEADD('hour', -4, UPDATED_DATETIME) AS DATE) = '{today}')
      AND (OUTAGE_NAME, CREATED_DATETIME) IN (
            SELECT OUTAGE_NAME, MAX(CREATED_DATETIME) 
            FROM FIVETRAN_DATABASE.MAKE.OUTAGE_LOG 
            GROUP BY 1
      )
    ORDER BY UPDATED_DATETIME DESC, CREATED_DATETIME DESC
"""
outage_df = session.sql(outage_query).to_pandas()

if outage_df.empty:
    st.markdown("""
        <div style='background-color: #d4edda; color: #155724; padding: 20px; border-radius: 5px; text-align: center; margin-bottom: 20px;'>
            All systems are functioning as expected
        </div>
    """, unsafe_allow_html=True)
else:
    for _, row in outage_df.iterrows():
        if row['STATUS'] == "OUTAGE":
            bg_color = "#f8d7da"  # Red
            text_color = "#721c24"
        elif row['STATUS'] == "MAINTENANCE":
            bg_color = "#fff3cd"  # Yellow
            text_color = "#856404"
        else:
            bg_color = "#d4edda"  # Green
            text_color = "#155724"
        st.markdown(f"""
            <div style='background-color: {bg_color}; color: {text_color}; padding: 15px; border-radius: 5px; margin-bottom: 10px;'>
                <div style='justify-content: space-between;'>
                    <strong>{row['STATUS']}: </strong>{row['SERVICE']} - {row['OUTAGE_NAME']} 
                    <a href="{row['URL']}" target="_blank">more details...</a>
                </div>
            </div>
        """, unsafe_allow_html=True)

# Live data query
query = """
    SELECT INITCAP(CHARITY_NM) AS CHARITY_NM, COUNT(*) AS APPROVED_DONOR_COUNT
    FROM FIVETRAN_DATABASE.ARMOR_SQL_2012_DBO.LOGGING_BY_GFDID
    WHERE DONOR_STATUS = 'Successful' AND DONOR_ACQ_DTM >= CURRENT_DATE AND GIFT_TYPE = 'Recurring'
      AND REPCODE NOT LIKE 'TEST%'
    GROUP BY CHARITY_NM
    UNION ALL
    SELECT CASE 
        WHEN CHARITY_NAME = 'USO' THEN 'United Service Organizations'
        WHEN CHARITY_NAME = 'NKH' THEN 'No Kid Hungry'
        ELSE INITCAP(CHARITY_NAME) END AS CHARITY_NM,
        COUNT(*) AS APPROVED_DONOR_COUNT
    FROM MADEMEDIA.RAW.MADEMEDIA_PROD
    WHERE DONOR_STATUS = 'approved' AND DATEADD(HOUR, -4, PAYMENT_DATE) >= CURRENT_DATE
    GROUP BY CHARITY_NM
    ORDER BY APPROVED_DONOR_COUNT DESC
"""
df = session.sql(query).to_pandas()
total = df["APPROVED_DONOR_COUNT"].sum()
goal = count_last_week

# Top count display
st.markdown(f"""
    <div style='text-align: center; background-color: #d1ecf1; padding: 10px; border-radius: 5px; margin-bottom: 20px;'>
        <div style='margin: 0; font-size: 32px;'><b>{total:,}</b> Donors Acquired</div>
        <div style='margin: 0; font-size: 16px;'><i>Showing Total Count of Approved Recurring Donors Today, Refreshing Every 5 Minutes.</i></div>
        <div style='margin: 0; font-size: 16px;'><i>MadeMedia Charities Included, Goal set to same day last week.</i></div>
    </div>
""", unsafe_allow_html=True)

st.markdown(f"""
    <div style='display: flex; justify-content: space-around; text-align: center; margin-bottom: 15px;'>
        <div><div style='font-size: 14px;'>Yesterday Total</div><div style='font-size: 20px; font-weight: bold;'>{count_yesterday:,}</div></div>
        <div><div style='font-size: 14px;'>{yesterday_label}</div><div style='font-size: 20px; font-weight: bold;'>{count_yesterday_block:,}</div></div>
        <div><div style='font-size: 14px;'>Same Day Last Week</div><div style='font-size: 20px; font-weight: bold;'>{count_last_week:,}</div></div>
        <div><div style='font-size: 14px;'>{lastweek_label}</div><div style='font-size: 20px; font-weight: bold;'>{count_last_week_block:,}</div></div>
    </div>
""", unsafe_allow_html=True)

# Progress bar
progress_pct = (total / goal) * 100 if goal > 0 else 0
bar_color = "#d4edda" if progress_pct >= 100 else "#d1ecf1"
st.markdown(f"""
    <div style='background-color: #e9ecef; border-radius: 5px; height: 30px; width: 100%; margin-bottom: 20px; overflow: hidden; position: relative;'>
        <div style='height: 100%; width: {progress_pct:.1f}%; background-color: {bar_color}; transition: width 0.5s ease;'></div>
        <div style='position: absolute; top: 0; left: 0; right: 0; bottom: 0; display: flex; align-items: center; justify-content: center; font-weight: bold; color: #003366; font-size: 16px;'>
            {total:,} of {goal} ({progress_pct:.1f}%)
        </div>
    </div>
""", unsafe_allow_html=True)

# Display per charity with pagination
if df.empty:
    st.markdown("<div style='text-align: center; padding: 20px; font-size: 20px; color: #888;'>No donors acquired yet today.</div>", unsafe_allow_html=True)
else:
    per_page = 9
    total_pages = (len(df) + per_page - 1) // per_page
    if 'page' not in st.session_state:
        st.session_state.page = 0
        st.rerun()
    current_page = st.session_state.page
    start = current_page * per_page
    end = start + per_page
    display_df = df.iloc[start:end]

    for i in range(0, len(display_df), 3):
        cols = st.columns(3)
        for j, (_, row) in enumerate(display_df.iloc[i:i+3].iterrows()):
            with cols[j]:
                bg = "#fcf5fb" if row['CHARITY_NM'] in made_media_charities else "#f8f9fa"
                st.markdown(f"""
                    <div style='background-color: {bg}; border: 3px solid #ffffff; border-radius: 10px; height: 200px; padding: 20px; display: flex; flex-direction: column; justify-content: center; align-items: center; text-align: center; font-weight: 600; color: #003366; margin-bottom: 20px;'>
                        <div style='font-size: 20px; line-height: 1.2; max-height: 3.6em; overflow: hidden; text-overflow: ellipsis; display: -webkit-box; -webkit-line-clamp: 3; -webkit-box-orient: vertical; margin-bottom: 10px;'>
                            {row['CHARITY_NM']}
                        </div>
                        <div style='font-size: 36px; font-weight: bold;'>
                            {row['APPROVED_DONOR_COUNT']:,}
                        </div>
                    </div>
                """, unsafe_allow_html=True)

    time.sleep(15)
    st.session_state.page = (current_page + 1) % total_pages
    st.rerun()
