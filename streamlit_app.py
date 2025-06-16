import streamlit as st
import streamlit.components.v1 as components
from snowflake.snowpark import Session
import pandas as pd
import numpy as np
import altair as alt
import datetime
import pytz
from snowflake.snowpark.functions import col, lit, upper, lower
from snowflake.snowpark.exceptions import SnowparkSessionException
import json
import time
import re
import ast
from snowflake.snowpark import Session

st.set_page_config(page_title="Live Dashboard", layout="wide")

# Inject screen width to query params
if "width" not in st.query_params:
    components.html(
        """
        <script>
        const params = new URLSearchParams(window.location.search);
        if (!params.get("width")) {
            params.set("width", window.innerWidth);
            window.location.search = params.toString();
        }
        </script>
        """,
        height=0,
    )

# Mobile detection function
def is_mobile():
    width_param = st.query_params.get("width")
    try:
        return int(width_param) <= 768 if width_param else False
    except:
        return False

# Create Snowflake session from Streamlit secrets
connection_parameters = dict(st.secrets["snowflake"])  # make a mutable copy
connection_parameters["host"] = st.secrets["snowflake"]["host"]

session = Session.builder.configs(connection_parameters).create()

connection_parameters = st.secrets["snowflake"]
session = Session.builder.configs(connection_parameters).create()

import streamlit.components.v1 as components

# JavaScript to add screen width to the query params (reloads only if not set)
if "width" not in st.query_params:
    components.html(
        """
        <script>
        const params = new URLSearchParams(window.location.search);
        if (!params.get("width")) {
            params.set("width", window.innerWidth);
            window.location.search = params.toString();
        }
        </script>
        """,
        height=0,
    )


def is_mobile():
    # Default to desktop if screen_width is not set
    width = st.query_params.get("width")
    try:
        return int(width) <= 768  # Treat anything <=768px as mobile
    except:
        return False

  
# === Total $ Raised Today ===
total_query = """
    SELECT SUM(GIFT_AMT) AS TOTAL_RAISED
    FROM FIVETRAN_DATABASE.ARMOR_SQL_2012_DBO.LOGGING_BY_GFDID
    WHERE DONOR_STATUS = 'Successful'
      AND GIFT_TYPE = 'Recurring'
      AND DONOR_ACQ_DTM >= CURRENT_DATE
"""

total_df = session.sql(total_query).to_pandas()
total_raised = total_df["TOTAL_RAISED"].iloc[0] or 0


# Charities acquired through MadeMedia (case-sensitive as returned by query)
made_media_charities = [
    "Armed Services Ymca",
    "United Service Organization",
    "No Kid Hungry",
    "American Lung Association",
    "Care"
]

# Define the available tabs
tabs = [
    "Donor Search", "Search History", "Transactions", "Donors By Charity", "Donors By Office",
    "Charity Overview", "Multiple Failed Attempts", "Reconciliation", "Process Later List",
    "Live Dashboard", "Payment Processor Status", "Prepaid & Gift Monitoring", "Weekly Donor Trends",
    "Drillable Donor Explorer", "BIN Risk Analyzer"
]

# Initialize selected_tab in session state if it doesn't exist
if 'selected_tab' not in st.session_state:
    st.session_state['selected_tab'] = "Donor Search"

# Function to update the tab selection
def select_tab(tab_name):
    st.session_state['selected_tab'] = tab_name
    st.rerun()

# Display row 1
#cols1 = st.columns(len(row1))
#for i, tab_name in enumerate(row1):
#    if cols1[i].button(tab_name, use_container_width=True):
#        st.session_state['selected_tab'] = tab_name
#        st.rerun()

# Display row 2
#cols2 = st.columns(len(row2))
#for i, tab_name in enumerate(row2):
#    if cols2[i].button(tab_name, use_container_width=True):
#        st.session_state['selected_tab'] = tab_name
#        st.rerun()

# Display row 3
#cols3 = st.columns(len(row3))
#for i, tab_name in enumerate(row3):
#    if cols3[i].button(tab_name, use_container_width=True):
#        st.session_state['selected_tab'] = tab_name
#        st.rerun()
# Check the selected tab and render the appropriate content
selected_tab = st.session_state['selected_tab']

# Custom CSS to make all sidebar buttons the same width
st.markdown(
    """
    <style>
    .sidebar-button {
        width: 100%;
        height: 50px;
        display: inline-block;
        text-align: center;
        margin-bottom: 10px;
    }
    </style>
    """, unsafe_allow_html=True
)

# Sidebar Navigation (adds individual buttons for each tab in the sidebar)
#st.sidebar.markdown("### Navigate to:")
for tab_name in tabs:
    if st.sidebar.button(tab_name, key=f"sidebar_{tab_name}", use_container_width=True):
        st.session_state['selected_tab'] = tab_name
        st.rerun()

# Check the selected tab and render the appropriate content
selected_tab = st.session_state['selected_tab']

# Define the function to navigate to the "Donor Search" tab with a specific GFDID
def navigate_to_home_with_gfdid(gfdid):
    # Set the tab to "Donor Search"
    st.session_state['selected_tab'] = "Donor Search"
    # Set the GFDID to search for
    st.session_state['search_gfdid'] = gfdid
    # Rerun the app to reflect the changes
    st.rerun()

# Create a list of the last 7 days - CHANGED TO 21
seven_days = [(datetime.date.today() - datetime.timedelta(days=i)).isoformat() for i in range(21)]

# Check if the selected date is in session state; if not, set the default to today
if 'selected_date' not in st.session_state:
    st.session_state['selected_date'] = seven_days[0]

# Function to retrieve search history from Snowflake
def get_search_history():
    query = session.sql("""
        SELECT REPLACE(USER_NAME,'"','') AS USER_NAME, TIMESTAMP, GFDID, SEARCH_TYPE
        FROM FIVETRAN_DATABASE.ARMOR_SQL_2012_DBO.SEARCH_HISTORY
        ORDER BY TIMESTAMP DESC
    """)
    
    # Convert the result to a pandas DataFrame
    search_history_df = query.to_pandas()
    
    return search_history_df

# Function to add a new entry to the search history
def add_to_search_history(type, gfdid):
    # Get the current user's name
    user_name = session.get_current_user()
    
    # Insert the new search entry into the SEARCH_HISTORY table
    session.sql(f"""
        INSERT INTO FIVETRAN_DATABASE.ARMOR_SQL_2012_DBO.SEARCH_HISTORY (USER_NAME, TIMESTAMP, GFDID, SEARCH_TYPE)
        VALUES ('{user_name}', CURRENT_TIMESTAMP(), '{gfdid}', '{type}')
    """).collect()

# Query to retrieve donor information
def query_gfdid_information(gfd_id, session):
    query = session.table("FIVETRAN_DATABASE.ARMOR_SQL_2012_DBO.LOGGING_BY_GFDID").filter(
        (col("GFDID") == gfd_id)
    ).select(
        "DONOR_NM", 
        "DONOR_TN",
        "CHARITY_NM", 
        "PROGRAM_NM", 
        "SERVICE_NM", 
        "GIFT_AMT", 
        "GIFT_TYPE", 
        "DONOR_ACQ_DTM",
        "FINAL_ATTEMPT_DTM",
        "DONOR_STATUS",
        "FAILED_IDS",
        "FAILED_CNT",
        "SUCCESS_IDS",
        "SUCCESS_CNT",
        "REPCODE"
    )
    
    result_df = query.to_pandas()
    return result_df

# Query for Donor TPV
def query_donortpv(gfd_id, session):
    query = session.table("PHOENIX_DW.REPORT.DONORTPV").filter(
        (col("Gfd Id") == gfd_id)
    ).select(
        "Donor State",
        "Payment Status",
        "CC Bin",
        "CC Type",
        "CC Funding Type",
        "CC Card Level",
        "CC Bank Name",
        "Payment Submission Token CC",
        "Payment Frequency",
        "Appeal ID",
        "TPV Agent Name",
        "Rep Code",
        "Rep Name",
        "Device Name",
        "Office Code",
        "Booth Ind",
        "Cell Phone Seperated",
        "Home Phone"
    )
    
    result_df = query.to_pandas()
    return result_df

# Query for transaction details
def query_transactions(ids, session):
    if len(ids) == 0:
        return pd.DataFrame()  # Return an empty DataFrame if no IDs

    query = session.table("FIVETRAN_DATABASE.ARMOR_SQL_2012_DBO.LOGGING_BY_TRANSACTION").filter(
        col("LOG_ID").in_(ids)
    ).select(
        "LOG_ID",
        "TRANSACTION_DTM", 
        "CARD_EXP_DT", 
        "CARD_END_NO", 
        "ERROR_TXT",
        "TRANSACTION_ID",
        "REPCODE",
        "TOKEN",
        "BILLING_ADDRESS",
        "NEXT_DONATION_DT",
        "AUTHCODE",
        "BIN"
    )
    
    result_df = query.to_pandas()
    return result_df

# Function to parse ID strings into lists
def parse_ids(ids_str):
    try:
        ids = ast.literal_eval(ids_str)
        if isinstance(ids, list):
            return ids
        return []
    except Exception as e:
        st.error(f"Error parsing IDs: {str(e)}")
        return []

# Function to format donor name from list
def format_donor_name(donor_name_str):
    try:
        donor_name_list = ast.literal_eval(donor_name_str)
        if isinstance(donor_name_list, list):
            if len(donor_name_list) == 1:
                return donor_name_list[0]
            elif len(donor_name_list) == 2:
                return f"{donor_name_list[0]} & {donor_name_list[1]}"
            else:
                return ", ".join(donor_name_list)
        else:
            return donor_name_list
    except (ValueError, SyntaxError):
        return donor_name_str

# Query to retrieve GFDID based on donor name array
def query_gfdid_by_donor_name(donor_name, session):
    query = session.sql(f"""
        SELECT GFDID
        FROM FIVETRAN_DATABASE.ARMOR_SQL_2012_DBO.LOGGING_BY_GFDID
        WHERE ARRAY_CONTAINS('{donor_name}'::variant, DONOR_NM)
        GROUP BY GFDID
    """)
    
    result_df = query.to_pandas()
    return result_df

# Function to fetch donors with multiple failed attempts and unique charities
def fetch_donors_with_multiple_failed_attempts(session, charity_filter=None):
    query = f"""
        SELECT GFDID, DONOR_NM, CHARITY_NM, DONOR_ACQ_DTM, FAILED_CNT
        FROM FIVETRAN_DATABASE.ARMOR_SQL_2012_DBO.LOGGING_BY_GFDID
        WHERE DONOR_STATUS = 'Payment Failed' AND FAILED_CNT > 1 
        AND DONOR_ACQ_DTM > DATEADD(day, -10, CURRENT_DATE())
    """
    if charity_filter:
        query += f" AND CHARITY_NM = '{charity_filter}'"
    query += " ORDER BY donor_acq_dtm DESC"
    
    # Retrieve data as pandas DataFrame
    data_df = session.sql(query).to_pandas()
    
    # Get the list of unique charities for the dropdown
    charity_list = data_df['CHARITY_NM'].unique().tolist()
    charity_list.insert(0, "All Charities")  # Add "All Charities" option to reset the filter
    
    return data_df, charity_list

# Function to retrieve GFDID based on APITransactionID
def query_gfdid_by_transaction_id(transaction_id, session):
    query = session.table("FIVETRAN_DATABASE.ARMOR_SQL_2012_DBO.LOGGING_BY_TRANSACTION").filter(
        (col("TRANSACTION_ID") == transaction_id)
    ).select("GFDID").distinct()
    
    result_df = query.to_pandas()
    return result_df

# Query to retrieve a list of all GFDIDs marked as "Process Later"
def get_process_later_gfdids(session):
    # Pulling all GFDIDs from the view that are marked as Process Later
    query = session.table("FIVETRAN_DATABASE.ARMOR_SQL_2012_DBO.ACTIVE_PROCESS_LATER").select("GFDID")
    process_later_df = query.to_pandas()
    return set(process_later_df['GFDID'].values)

# Function to fetch and return "Process Later" donors from the past 30 days or a specific date
def fetch_process_later_donors(session, selected_date, default_ind):
    if default_ind == 1:
        # Query to fetch Process Later donors within the last 30 days (current view)
        query = """
            SELECT
                GFDID,
                DONOR_NM,
                CHARITY_NM,
                DONOR_ACQ_DTM,
                DR_STATUS AS DONOR_STATUS
            FROM FIVETRAN_DATABASE.ARMOR_SQL_2012_DBO.ACTIVE_PROCESS_LATER
            WHERE DONOR_ACQ_DTM > DATEADD(day, -21, CURRENT_DATE())
            ORDER BY DONOR_ACQ_DTM DESC
        """
    else:
        # Fetch 'Process Later' donors for the selected date
        query = f"""
            SELECT
                a.GFDID,
                b.DONOR_NM,
                b.CHARITY_NM,
                b.DONOR_ACQ_DTM,
                CASE WHEN C."Donor State" = 'Approved Net' then 'Successful' ELSE b.DONOR_STATUS END AS DONOR_STATUS
            FROM FIVETRAN_DATABASE.ARMOR_SQL_2012_DBO.DAILY_PROCESS_LATER AS a
            JOIN FIVETRAN_DATABASE.ARMOR_SQL_2012_DBO.LOGGING_BY_GFDID AS b
                ON a.GFDID = b.GFDID
            LEFT JOIN PHOENIX_DW.REPORT.DONORTPV AS c
                ON A.GFDID = C."Gfd Id"
            WHERE DATE(a.TRANSACTION_DTM) = '{selected_date}'
            ORDER BY b.DONOR_ACQ_DTM DESC
        """
    
    # Execute the query and ensure the DONOR_ACQ_DTM column is datetime
    df = session.sql(query).to_pandas()
    df['DONOR_ACQ_DTM'] = pd.to_datetime(df['DONOR_ACQ_DTM'], errors='coerce')
    return df

# Modified function to fetch and return recent transaction data grouped by charity
def fetch_recent_charity_transactions():
    # Fetch transactions data
    query = session.sql("""
        SELECT 
            GFDID,
            CHARITY_NM,
            DONOR_NM,
            DONOR_STATUS,
            DONOR_ACQ_DTM,
            FINAL_ATTEMPT_DTM
        FROM
            FIVETRAN_DATABASE.ARMOR_SQL_2012_DBO.LOGGING_BY_GFDID
        WHERE DONOR_ACQ_DTM >= dateadd("day", -1, current_date())
        ORDER BY DONOR_ACQ_DTM DESC
    """)
    
    transactions_df = query.to_pandas()
    
    # Retrieve the list of GFDIDs marked as "Process Later" from the view
    process_later_gfdids = get_process_later_gfdids(session)
    
    # Flag transactions as "Process Later" if they exist in the Process Later GFDID list
    transactions_df['DONOR_STATUS'] = transactions_df.apply(
        lambda x: 'Process Later' if x['GFDID'] in process_later_gfdids else x['DONOR_STATUS'], axis=1
    )

    # Sort by the most recent transaction date for each charity
    transactions_df = transactions_df.sort_values(by=["CHARITY_NM", "FINAL_ATTEMPT_DTM"], ascending=[True, False])
    
    return transactions_df

# Function to query the most recent 25 donors from Snowflake, ordered by acquisition date
# Function to fetch recent transactions with the same "Process Later" logic
def fetch_recent_transactions():
    query = session.table("FIVETRAN_DATABASE.ARMOR_SQL_2012_DBO.LOGGING_BY_GFDID").select(
        "GFDID", "DONOR_NM", "CHARITY_NM", "GIFT_AMT", "DONOR_STATUS", "FINAL_ATTEMPT_DTM"
    ).order_by(col("FINAL_ATTEMPT_DTM").desc()).limit(200)
    
    transactions_df = query.to_pandas()

    # Check each GFDID for "Process Later" status from the ACTIVE_PROCESS_LATER view
    process_later_query = session.table("FIVETRAN_DATABASE.ARMOR_SQL_2012_DBO.ACTIVE_PROCESS_LATER").select("GFDID")
    process_later_df = process_later_query.to_pandas()
    
    # Mark as "Process Later" if GFDID is in the ACTIVE_PROCESS_LATER view
    transactions_df['Process_Later'] = transactions_df['GFDID'].isin(process_later_df['GFDID'])
    transactions_df['DONOR_STATUS'] = transactions_df.apply(
        lambda x: 'Process Later' if x['Process_Later'] else x['DONOR_STATUS'], axis=1
    )
    
    return transactions_df

# Helper function to return transaction attempts by GFDID for live viewing
def fetch_gfdid_transactions(gfdid):
    # Query to fetch all transaction attempts for a given GFDID
    query = session.table("FIVETRAN_DATABASE.ARMOR_SQL_2012_DBO.LOGGING_BY_TRANSACTION").filter(
        col("GFDID") == gfdid
    ).select(
        "TRANSACTION_DTM", "ERROR_TXT", "LOG_ID"
    ).order_by(col("TRANSACTION_DTM").desc())
    
    return query.to_pandas()

transactions_df = fetch_recent_transactions()

# Main function to display Donor and TPV information side by side with equal height boxes
def display_gfdid_and_tpv_info(gfdid_df, tpv_info_df, donor_name, gfd_id):
    cell_phone = tpv_info_df["Cell Phone Seperated"].iloc[0] if not tpv_info_df.empty else "NO CELL PHONE"
    home_phone = tpv_info_df["Home Phone"].iloc[0] if not tpv_info_df.empty else "NO HOME PHONE"
    st.markdown(f"""
        <div style='text-align: center; padding-top: 10px; padding-bottom: 10px; background-color: #d1ecf1; border-radius: 5px; margin-bottom: 10px;'>
            <div style='margin: 0; font-size: 28px;'>{donor_name}</div>
            <span>GFD3V00000{gfd_id}</span>
            <div style='margin: 0; font-size: 14px; color: #555;'>Cell Phone: {cell_phone} - Home Phone: {home_phone}</div>
        </div>
    """, unsafe_allow_html=True)

    # Insight Database Section with check for empty tpv_info_df
    if tpv_info_df.empty:
        # Container div with grid layout for equal height columns
        st.markdown("""
            <div style='display: grid; padding-bottom: 10px; grid-template-columns: 1fr 1fr; gap: 10px; align-items: start;'>
                <div style='background-color: #f0f0f5; padding: 10px; border-radius: 5px; height: 100%; min-height: 250px;'>
                    <div style='text-align: center; bottom-padding: 5px;'><strong>Armor Database</strong></div>
                    <strong>Payment Status:</strong> {DONOR_STATUS}<br>
                    <strong>Acquired At:</strong> {DONOR_ACQ_DTM}<br>
                    <ul>
                        <li><strong>Charity Name:</strong> {CHARITY_NM}</li>
                        <li><strong>Program Name:</strong> {PROGRAM_NM}</li>
                        <li><strong>Service Name:</strong> {SERVICE_NM}</li>
                        <li><strong>Gift Amount:</strong> ${GIFT_AMT}</li>
                        <li><strong>Gift Type:</strong> {GIFT_TYPE}</li>
                    </ul>
                    <strong>Fundraiser ID:</strong> {REP_CODE}<br>
                    <strong>Total Failed Attempts:</strong> {FAILED_CNT}<br>
                    <strong>Last Attempt At:</strong> {FINAL_ATTEMPT_DTM}<br>
                </div>
                <div style='background-color: #f0f0f5; padding: 10px; border-radius: 5px; height: 100%; min-height: 250px;'>
                    <div style='text-align: center; bottom-padding: 5px;'><strong>Insight Database</strong><br><i>Data not yet available in Snowflake</i></div>
                </div>
            </div>
        """.format(
            DONOR_STATUS=gfdid_df['DONOR_STATUS'].iloc[0],
            DONOR_ACQ_DTM=gfdid_df['DONOR_ACQ_DTM'].iloc[0],
            CHARITY_NM=gfdid_df['CHARITY_NM'].iloc[0],
            PROGRAM_NM=gfdid_df['PROGRAM_NM'].iloc[0],
            SERVICE_NM=gfdid_df['SERVICE_NM'].iloc[0],
            GIFT_AMT=gfdid_df['GIFT_AMT'].iloc[0],
            GIFT_TYPE=gfdid_df['GIFT_TYPE'].iloc[0],
            FAILED_CNT=gfdid_df['FAILED_CNT'].iloc[0],
            FINAL_ATTEMPT_DTM=gfdid_df['FINAL_ATTEMPT_DTM'].iloc[0],
            REP_CODE=gfdid_df['REPCODE'].iloc[0]
        ), unsafe_allow_html=True)
        
    else:
        # Container div with grid layout for equal height columns
        st.markdown("""
            <div style='display: grid; padding-bottom: 10px; grid-template-columns: 1fr 1fr; gap: 10px; align-items: start;'>
                <div style='background-color: #f0f0f5; padding: 10px; border-radius: 5px; height: 100%; min-height: 250px;'>
                    <div style='text-align: center; bottom-padding: 5px;'><strong>Armor Database</strong></div>
                    <strong>Payment Status:</strong> {DONOR_STATUS}<br>
                    <strong>Acquired At:</strong> {DONOR_ACQ_DTM}<br>
                    <ul>
                        <li><strong>Charity Name:</strong> {CHARITY_NM}</li>
                        <li><strong>Program Name:</strong> {PROGRAM_NM}</li>
                        <li><strong>Service Name:</strong> {SERVICE_NM}</li>
                        <li><strong>Gift Amount:</strong> ${GIFT_AMT}</li>
                        <li><strong>Gift Type:</strong> {GIFT_TYPE}</li>
                    </ul>
                    <strong>Fundraiser ID:</strong> {REP_CODE}<br>
                    <strong>Total Failed Attempts:</strong> {FAILED_CNT}<br>
                    <strong>Last Attempt At:</strong> {FINAL_ATTEMPT_DTM}<br>
                </div>
                <div style='background-color: #f0f0f5; padding: 10px; border-radius: 5px; height: 100%; min-height: 250px;'>
                    <div style='text-align: center; bottom-padding: 5px;'><strong>Insight Database</strong></div>
                    <strong>Donor State:</strong> {Donor_State}<br>
                    <strong>Payment Frequency:</strong> {Payment_Frequency}<br>
                    <ul>
                        <li><strong>Type:</strong> {CC_Type}</li>
                        <li><strong>Funding Type:</strong> {CC_Funding_Type}</li>
                        <li><strong>Card Level:</strong> {CC_Card_Level}</li>
                        <li><strong>Bank:</strong> {CC_Bank_Name}</li>
                    </ul>
                    <strong>Is Booth:</strong> {Booth_Ind}<br>
                    <strong>Fundraiser ID:</strong> {Rep_Code}<br>
                    <strong>Fundraiser:</strong> {Rep_Name}<br>
                    <strong>TPV Agent:</strong> {TPV_Agent_Name}<br>
                </div>
            </div>
        """.format(
            DONOR_STATUS=gfdid_df['DONOR_STATUS'].iloc[0],
            DONOR_ACQ_DTM=gfdid_df['DONOR_ACQ_DTM'].iloc[0],
            CHARITY_NM=gfdid_df['CHARITY_NM'].iloc[0],
            PROGRAM_NM=gfdid_df['PROGRAM_NM'].iloc[0],
            SERVICE_NM=gfdid_df['SERVICE_NM'].iloc[0],
            GIFT_AMT=gfdid_df['GIFT_AMT'].iloc[0],
            GIFT_TYPE=gfdid_df['GIFT_TYPE'].iloc[0],
            FAILED_CNT=gfdid_df['FAILED_CNT'].iloc[0],
            FINAL_ATTEMPT_DTM=gfdid_df['FINAL_ATTEMPT_DTM'].iloc[0],
            REP_CODE=gfdid_df['REPCODE'].iloc[0],
            Donor_State=tpv_info_df['Donor State'].iloc[0],
            Payment_Status=tpv_info_df['Payment Status'].iloc[0],
            CC_Bin=tpv_info_df['CC Bin'].iloc[0],
            CC_Type=tpv_info_df['CC Type'].iloc[0],
            CC_Funding_Type=tpv_info_df['CC Funding Type'].iloc[0],
            CC_Card_Level=tpv_info_df['CC Card Level'].iloc[0],
            CC_Bank_Name=tpv_info_df['CC Bank Name'].iloc[0],
            Payment_Frequency=tpv_info_df['Payment Frequency'].iloc[0],
            Appeal_ID=tpv_info_df['Appeal ID'].iloc[0],
            Payment_Submission_Token_CC=tpv_info_df['Payment Submission Token CC'].iloc[0],
            TPV_Agent_Name=tpv_info_df['TPV Agent Name'].iloc[0],
            Rep_Code=tpv_info_df['Rep Code'].iloc[0],
            Rep_Name=tpv_info_df['Rep Name'].iloc[0],
            Device_Name=tpv_info_df['Device Name'].iloc[0],
            Office_Code=tpv_info_df['Office Code'].iloc[0],
            Booth_Ind=tpv_info_df['Booth Ind'].iloc[0]
        ), unsafe_allow_html=True)

# Display transactions (both successful and failed) in chronological order
def display_transactions(df, success_ids):
    # Tag each transaction as Success or Failed
    df['STATUS'] = df['LOG_ID'].apply(lambda x: 'Success' if x in success_ids else 'Failed')

    # Add the "Transactions" header
    st.markdown(f"""
        <div style='text-align: center; padding-top: 10px; padding-bottom: 10px; background-color: #d1ecf1; border-radius: 5px; margin-bottom: 10px;'>
            <div style='margin: 0; font-size: 23px;'>Transactions</div>
        </div>
    """, unsafe_allow_html=True)
    # Check if there are no successful transactions
    success_df = df[df['STATUS'] == 'Success']
    if success_df.empty:
        st.markdown("""
            <div style='background-color: #f0f0f5; padding: 10px; border-radius: 5px; margin-bottom: 10px; text-align: center;'>
                <strong>Note:</strong> No successful transactions found for this donor.
            </div>
        """, unsafe_allow_html=True)

    # Group Success transactions by TRANSACTION_ID
    success_df = df[df['STATUS'] == 'Success']
        
    success_grouped = success_df.groupby('TRANSACTION_ID').agg(
        min_timestamp=('TRANSACTION_DTM', 'min'),
        first_timestamp=('TRANSACTION_DTM', 'min'),
        last_timestamp=('TRANSACTION_DTM', 'max'),
        transaction_count=('TRANSACTION_DTM', 'count'),
        card_exp_dt=('CARD_EXP_DT', 'first'),
        card_end_no=('CARD_END_NO', 'first'),
        TOKEN=('TOKEN', 'first'),
        BILLING_ADDRESS=('BILLING_ADDRESS', 'first'),
        NEXT_DONATION_DT=('NEXT_DONATION_DT', 'first'),
        AUTHCODE=('AUTHCODE', 'first'),
        BIN=('BIN', 'first')
    ).reset_index()
    success_grouped['STATUS'] = 'Success'

    # Prepare Failed transactions (no grouping)
    failed_df = df[df['STATUS'] == 'Failed'].copy()
    failed_df['first_timestamp'] = failed_df['TRANSACTION_DTM']
    failed_df['last_timestamp'] = failed_df['TRANSACTION_DTM']
    failed_df['transaction_count'] = 1
    failed_df['card_exp_dt'] = failed_df['CARD_EXP_DT']
    failed_df['card_end_no'] = failed_df['CARD_END_NO']
    failed_df['min_timestamp'] = failed_df['TRANSACTION_DTM']
    
    # Combine all transactions and sort by min_timestamp
    combined_df = pd.concat([success_grouped, failed_df], ignore_index=True)
    combined_df = combined_df.sort_values(by='min_timestamp', ascending=False)

    # Group successful transactions and check for multiple unique transaction IDs
    if len(success_df['TRANSACTION_ID'].unique()) > 1:
        st.markdown("""
            <div style='background-color: #f8d7da; color: #721c24; padding: 10px; border-radius: 5px; margin-bottom: 10px; text-align: center;'>
                <strong>Alert:</strong> There are multiple successful payments for this donor<br>Please contact IT immediately
            </div>
        """, unsafe_allow_html=True)

    # Display each transaction in order
    for _, row in combined_df.iterrows():
        if row['STATUS'] == 'Success':
            # Display grouped Success transactions
            if row['transaction_count'] > 1:
                st.markdown(f"""
                    <div style='background-color: #d4edda; color: #155724; padding: 10px; border-radius: 5px; margin-bottom: 10px;'>
                        <div style='text-align: center; margin-bottom: 5px;'><strong>Successful Payment</strong><br><i><strong>At least One</strong> of the <strong>{row['transaction_count']} records </strong> was processed successfully</i></div>
                        <strong>Latest Record:</strong> {row['last_timestamp']}<br>
                        <strong>First Record:</strong> {row['first_timestamp']}<br>
                        <strong>Card Number:</strong> {row['card_end_no']}, <strong>Exp:</strong> {row['card_exp_dt']}<br>
                        <strong>Address:</strong> {row['BILLING_ADDRESS']}<br>
                        <strong>Transaction ID:</strong> {row['TRANSACTION_ID']} - <i>Auth Code: {row['AUTHCODE']}</i><br>
                        <strong>Token:</strong> {row['TOKEN']}<br>
                        <strong>BIN:</strong> {row['BIN']}<br>
                    </div>
                """, unsafe_allow_html=True)

                bin_meta = get_bin_metadata(row['BIN'])

                if bin_meta is not None:
                    st.markdown(f"""
                        <div style='
                            background-color: #eaf6ed;
                            padding: 10px;
                            border-radius: 5px;
                            margin-top: 0px;
                            margin-bottom: 10px;
                        '>
                            <div style='display: flex; justify-content: space-between; text-align: center; font-size: 13px;'>
                                <div>{bin_meta['CREDITCARDTYPE']}</div>
                                <div>{bin_meta['FUNDINGTYPE']}</div>
                                <div>{bin_meta['CARDLEVEL']}</div>
                                <div>{bin_meta['BANKNAME']}</div>
                                <div>{bin_meta['ISSUINGBANKCOUNTRY']}</div>
                            </div>
                        </div>
                    """, unsafe_allow_html=True)

            else:
                st.markdown(f"""
                    <div style='background-color: #d4edda; color: #155724; padding: 10px; border-radius: 5px; margin-bottom: 10px;'>
                        <div style='text-align: center; font-weight: bold; margin-bottom: 5px;'>Successful Payment</div>
                        <strong>Transaction Date:</strong> {row['first_timestamp']}<br>
                        <strong>Card Number:</strong> {row['card_end_no']}, <strong>Exp:</strong> {row['card_exp_dt']}<br>
                        <strong>Address:</strong> {row['BILLING_ADDRESS']}<br>
                        <strong>Transaction ID:</strong> {row['TRANSACTION_ID']} - <i>Auth Code: {row['AUTHCODE']}</i><br>
                        <strong>Token:</strong> {row['TOKEN']}<br>
                        <strong>BIN:</strong> {row['BIN']}<br>
                    </div>
                """, unsafe_allow_html=True)

                bin_meta = get_bin_metadata(row['BIN'])

                if bin_meta is not None:
                    st.markdown(f"""
                        <div style='
                            background-color: #eaf6ed;
                            padding: 10px;
                            border-radius: 5px;
                            margin-top: 0px;
                            margin-bottom: 10px;
                        '>
                            <div style='display: flex; justify-content: space-between; text-align: center; font-size: 13px;'>
                                <div>{bin_meta['CREDITCARDTYPE']}</div>
                                <div>{bin_meta['FUNDINGTYPE']}</div>
                                <div>{bin_meta['CARDLEVEL']}</div>
                                <div>{bin_meta['BANKNAME']}</div>
                                <div>{bin_meta['ISSUINGBANKCOUNTRY']}</div>
                            </div>
                        </div>
                    """, unsafe_allow_html=True)
                    
        elif row['ERROR_TXT'] == "Process Later":
            # Display the "Process Later" message with yellow background
            st.markdown(f"""
                <div style='background-color: #fff3cd; color: #856404; padding: 10px; border-radius: 5px; margin-bottom: 10px; max-width: 100%; word-break: break-word;'>
                    <div style='text-align: center;'>Donor marked as <strong>Process Later</strong> at {row['TRANSACTION_DTM']}</div>
                </div>
            """, unsafe_allow_html=True)
        else:
            # Failed transaction display with a collapsible section for error text, preserving special characters
            #full_error_text = row['ERROR_TXT'].replace("\r\n", "<br>").replace("<", "&lt;").replace(">", "&gt;")
            # Safely handle ERROR_TXT
            if row['ERROR_TXT'] is not None:
                full_error_text = row['ERROR_TXT'].replace("\r\n", "<br>").replace("<", "&lt;").replace(">", "&gt;")
            else:
                full_error_text = "No error text available."
            first_50_chars = full_error_text[:75] + '...'  # Take the first 50 characters and add ellipsis
            
            st.markdown(f"""
                <div style='background-color: #f8d7da; color: #721c24; padding: 10px; border-radius: 5px; margin-bottom: 10px; max-width: 100%; word-break: break-word;'>
                    <div style='text-align: center; font-weight: bold; margin-bottom: 5px;'>Failed Payment</div>
                    <strong>Transaction Date:</strong> {row['TRANSACTION_DTM']}<br>
                    <strong>Card Expiration Date:</strong> {row['CARD_EXP_DT']}<br>
                    <strong>Card Number:</strong> **** **** **** {row['card_end_no']}<br>
                    <strong>BIN:</strong> {row['BIN']}<br>
                    <strong>Transaction ID:</strong> {row['TRANSACTION_ID']}<br>
                    <details>
                        <summary style="cursor: pointer; color: #721c24;"><strong>Error Text:</strong> {first_50_chars}</summary>
                        <p style="color: #721c24;">{full_error_text}</p>
                    </details>
                </div>
            """, unsafe_allow_html=True)

            bin_meta = get_bin_metadata(row['BIN'])

            if bin_meta is not None:
                st.markdown(f"""
                    <div style='
                        background-color: #fcebec;
                        padding: 10px;
                        border-radius: 5px;
                        margin-top: 0px;
                        margin-bottom: 10px;
                    '>
                        <div style='display: flex; justify-content: space-between; text-align: center; font-size: 13px;'>
                            <div>{bin_meta['CREDITCARDTYPE']}</div>
                            <div>{bin_meta['FUNDINGTYPE']}</div>
                            <div>{bin_meta['CARDLEVEL']}</div>
                            <div>{bin_meta['BANKNAME']}</div>
                            <div>{bin_meta['ISSUINGBANKCOUNTRY']}</div>
                        </div>
                    </div>
                """, unsafe_allow_html=True)

# CODE FOR OUTAGES HERE
# Function to query today's outages from the Outage Log
def query_todays_outages(session):
    today = datetime.date.today().isoformat()
    query = f"""
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
        WHERE GFD_STATUS <> 'No Impact' AND
            (CAST(DATEADD('hour', -4, CREATED_DATETIME) AS DATE) = '{today}' OR CAST(DATEADD('hour', -4, UPDATED_DATETIME) AS DATE) = '{today}')
            AND (OUTAGE_NAME, CREATED_DATETIME) IN (SELECT OUTAGE_NAME, MAX(CREATED_DATETIME) AS CREATED_DATETIME FROM FIVETRAN_DATABASE.MAKE.OUTAGE_LOG GROUP BY 1)
        ORDER BY UPDATED_DATETIME DESC, CREATED_DATETIME DESC
    """
    return session.sql(query).to_pandas()

# Function to display outages or all-clear message
def display_todays_outages(outage_df):
    if outage_df.empty:
        # Display a green banner if no outages are found for today
        st.markdown("""
            <div style='background-color: #d4edda; color: #155724; padding: 20px; border-radius: 5px; text-align: center; margin-bottom: 20px;'>
                All systems are functioning as expected
            </div>
        """, unsafe_allow_html=True)
    else:
        # Display banners for each outage that occurred today
        for _, row in outage_df.iterrows():
            # Define background color based on outage status
            if row['STATUS'] == "OUTAGE":
                bg_color = "#f8d7da"  # Red for outages
                text_color = "#721c24"
            elif row['STATUS'] == "MAINTENANCE":
                bg_color = "#fff3cd"  # Yellow for maintenance
                text_color = "#856404"
            else:
                bg_color = "#d4edda"  # Green for resolved issues
                text_color = "#155724"
            
            # Display each outage in a banner format
            st.markdown(f"""
                <div style='background-color: {bg_color}; color: {text_color}; padding: 15px; border-radius: 5px; margin-bottom: 10px;'>
                    <div style='justify-content: space-between;'>
                        <strong>{row['STATUS']}: </strong>{row['SERVICE']} - {row['OUTAGE_NAME']} 
                        <a href="{row['URL']}" target="_blank">more details...</a>
                    </div>
                </div>
            """, unsafe_allow_html=True)

outage_df = query_todays_outages(session)
display_todays_outages(outage_df)

def sanitize_phone_number(phone_number):
    """Sanitize the phone number to keep only numeric characters."""
    return re.sub(r'\D', '', phone_number)

def query_gfdid_by_phone(phone_number, session):
    """Query the donortpv table for a GFDID based on the given phone number."""
    query = f"""
        SELECT DISTINCT "Gfd Id" AS GFDID
        FROM PHOENIX_DW.REPORT.DONORTPV
        WHERE REPLACE(REPLACE(REPLACE(REPLACE("Home Phone", '-', ''), '(', ''), ')', ''), ' ', '') = '{phone_number}'
           OR REPLACE(REPLACE(REPLACE(REPLACE("Cell Phone Seperated", '-', ''), '(', ''), ')', ''), ' ', '') = '{phone_number}'
    """
    result_df = session.sql(query).to_pandas()
    return result_df

# Function for Bin Join
def fetch_prepaid_or_giftcard_details(session):
    query = """
        SELECT 
            INITCAP(g.CHARITY_NM) AS CHARITY,
            g.GFDID,
            t.REPCODE AS FUNDRAISER,
            g.DONOR_ACQ_DTM as TRANSACTION_DTM,
            b.cardlevel
        FROM FIVETRAN_DATABASE.ARMOR_SQL_2012_DBO.LOGGING_BY_GFDID g
        LEFT JOIN FIVETRAN_DATABASE.ARMOR_SQL_2012_DBO.LOGGING_BY_TRANSACTION t 
            ON g.GFDID = t.GFDID
        JOIN FIVETRAN_DATABASE.ARMOR_SQL_2012_DBO.BIN b
            ON REPLACE(SUBSTRING(t.BIN, 1, 7), ' ', '') = TO_VARCHAR(b.BIN)
        WHERE g.DONOR_STATUS = 'Successful'
          AND t.SUCCESS_IND = 'TRUE'
          AND t.BIN IS NOT NULL
          AND g.donor_acq_dtm > CURRENT_DATE - 2
          AND t.BIN != ''
          AND LENGTH(REPLACE(SUBSTRING(t.BIN, 1, 7), ' ', '')) = 6
          AND REGEXP_LIKE(REPLACE(SUBSTRING(t.BIN, 1, 7), ' ', ''), '^[0-9]{6}$')
          AND LOWER(b.CARDLEVEL) LIKE ANY ('%prepaid%', '%gift%')
          AND b._FIVETRAN_DELETED = FALSE
        ORDER BY CHARITY, TRANSACTION_DTM DESC
    """
    return session.sql(query).to_pandas()

# Function to append bin data to transaction lookup
def get_bin_metadata(bin_value):
    if not bin_value:
        return None

    bin_cleaned = bin_value.replace(" ", "")[:6]
    if not bin_cleaned.isdigit():
        return None

    bin_int = int(bin_cleaned)

    query = f"""
        SELECT 
            CREDITCARDTYPE, 
            FUNDINGTYPE, 
            CARDLEVEL, 
            BANKNAME, 
            ISSUINGBANKCOUNTRY
        FROM FIVETRAN_DATABASE.ARMOR_SQL_2012_DBO.BIN
        WHERE BIN = {bin_int}
        AND _FIVETRAN_DELETED = FALSE
        LIMIT 1
    """
    result = session.sql(query).to_pandas()
    return result.iloc[0] if not result.empty else None

# CODE TO DISPLAY TABS, STARTING WITH DONOR SEARCH
if selected_tab == "Donor Search":
    # Streamlit Interface

    st.markdown(f"""
        <div style='text-align: center; background-color: #d1ecf1; padding: 10px; border-radius: 5px; margin-bottom: 20px;'>
            <div style='margin: 0; font-size: 32px;'>Donor Search</div>
        </div>
    """, unsafe_allow_html=True)
    
    col1, col2 = st.columns([2, 1])
    with col1:
        gfd_id = st.text_input(
            '', 
            '', 
            placeholder='Enter GFDID Or Last 7 Digits (GFD3V00000+)',
            label_visibility='collapsed'
        ).replace(" ", "")
        if gfd_id.startswith("GFD3V"):
            gfd_id = gfd_id[-7:]
    with col2:
        search_button = st.button('Search By GFDID', use_container_width=True)
    
    # Handle search by donor name
    col3, col4 = st.columns([2, 1])
    with col3:
        donor_name_search = st.text_input(
            '', 
            '', 
            placeholder='Enter Donor Full Name (Exact Match Required)',
            label_visibility='collapsed'
        ).strip()
    
        # Replace any whitespace (including tabs and multiple spaces) with a single space
        donor_name_search = re.sub(r'\s+', ' ', donor_name_search)
    
    with col4:
        name_search_button = st.button('Search By Name',  use_container_width=True)
    
    # Add a new input for APITransactionID
    col5, col6 = st.columns([2, 1])
    with col5:
        transaction_id_search = st.text_input(
            '', 
            '', 
            placeholder='Enter API Transaction ID',
            label_visibility='collapsed'
        ).strip()
    
    with col6:
        transaction_id_button = st.button('Search By Trans ID', use_container_width=True)

    # Add a new input for phone number search
    col7, col8 = st.columns([2, 1])
    with col7:
        phone_number_search = st.text_input(
            '',
            '',
            placeholder='Enter Phone Number (Home or Mobile)',
            label_visibility='collapsed'
        ).strip()
    
    with col8:
        phone_search_button = st.button('Search By Phone', use_container_width=True)
    
    if search_button:
        if not (gfd_id.isdigit() and len(gfd_id) == 7):
            st.error("Please enter a valid 7-digit GFDID number (numeric only).")
        else:
            try:
                gfdid_df = query_gfdid_information(gfd_id, session)
                tpv_info_df = query_donortpv(gfd_id, session)
                add_to_search_history('GFDID', gfd_id)
                
                # Within the main app logic, call the display_gfdid_and_tpv_info function with gfd_id
                if not gfdid_df.empty:
                    donor_name = format_donor_name(gfdid_df["DONOR_NM"].iloc[0])
                    display_gfdid_and_tpv_info(gfdid_df, tpv_info_df, donor_name, gfd_id)
    
                    # Get transaction information as before
                    success_ids = parse_ids(gfdid_df["SUCCESS_IDS"].iloc[0])
                    failed_ids = parse_ids(gfdid_df["FAILED_IDS"].iloc[0])
                    transaction_ids = success_ids + failed_ids
                    all_transactions = query_transactions(transaction_ids, session)
                    
                    if not all_transactions.empty:
                        display_transactions(all_transactions, success_ids)
                    else:
                        st.warning("No transaction records found.")
                else:
                    st.write(f"No information found for GFDID {gfd_id}.")
            except Exception as e:
                st.error(f"An error occurred: {str(e)}")
                
    if name_search_button:
        # Sanitize input, check if it’s valid
        if not donor_name_search:
            st.error("Please enter a donor name.")
        else:
            try:
                # Look up the GFDID by donor name
                gfdid_df = query_gfdid_by_donor_name(donor_name_search, session)
                add_to_search_history('Donor Name', donor_name_search)
                
                if not gfdid_df.empty:
                    # Loop through all found GFDIDs and display info
                    for gfd_id in gfdid_df['GFDID']:
                        #st.write(f"Found GFDID: {gfd_id}")
                        # Query and display information for the found GFDID
                        gfdid_info_df = query_gfdid_information(gfd_id, session)
                        tpv_info_df = query_donortpv(gfd_id, session)
    
                        donor_name_formatted = format_donor_name(gfdid_info_df["DONOR_NM"].iloc[0])
                        display_gfdid_and_tpv_info(gfdid_info_df, tpv_info_df, donor_name_formatted, gfd_id)
                        
                        # Handle transactions as before
                        success_ids = parse_ids(gfdid_info_df["SUCCESS_IDS"].iloc[0])
                        failed_ids = parse_ids(gfdid_info_df["FAILED_IDS"].iloc[0])
                        transaction_ids = success_ids + failed_ids
                        all_transactions = query_transactions(transaction_ids, session)
                        
                        if not all_transactions.empty:
                            display_transactions(all_transactions, success_ids)
                        else:
                            st.warning("No transaction records found.")
                else:
                    st.write("No GFDID found for this donor name.")
            except Exception as e:
                st.error(f"An error occurred: {str(e)}")
    
    # Handle APITransactionID search
    if transaction_id_button:
        if not transaction_id_search:
            st.error("Please enter a valid API Transaction ID.")
        else:
            try:
                # Look up the GFDID by API Transaction ID
                gfdid_df = query_gfdid_by_transaction_id(transaction_id_search, session)
                add_to_search_history('Transaction', transaction_id_search)
                
                if not gfdid_df.empty:
                    # Loop through all found GFDIDs and display info
                    for gfd_id in gfdid_df['GFDID']:
                        gfdid_info_df = query_gfdid_information(gfd_id, session)
                        tpv_info_df = query_donortpv(gfd_id, session)
    
                        donor_name_formatted = format_donor_name(gfdid_info_df["DONOR_NM"].iloc[0])
                        display_gfdid_and_tpv_info(gfdid_info_df, tpv_info_df, donor_name_formatted, gfd_id)
                        
                        # Handle transactions as before
                        success_ids = parse_ids(gfdid_info_df["SUCCESS_IDS"].iloc[0])
                        failed_ids = parse_ids(gfdid_info_df["FAILED_IDS"].iloc[0])
                        transaction_ids = success_ids + failed_ids
                        all_transactions = query_transactions(transaction_ids, session)
                        
                        if not all_transactions.empty:
                            display_transactions(all_transactions, success_ids)
                        else:
                            st.warning("No transaction records found.")
                else:
                    st.write("No GFDID found for this API Transaction ID.")
            except Exception as e:
                st.error(f"An error occurred: {str(e)}")
                
    # Handle TN searching
    if phone_search_button:
        if not phone_number_search:
            st.error("Please enter a valid phone number.")
        else:
            sanitized_phone = sanitize_phone_number(phone_number_search)
            if not sanitized_phone.isdigit():
                st.error("Invalid phone number. Please enter numeric characters only.")
            else:
                try:
                    # Query for GFDID by phone number
                    gfdid_df = query_gfdid_by_phone(sanitized_phone, session)
                    add_to_search_history('Phone', sanitized_phone)
    
                    if not gfdid_df.empty:
                        # Loop through all found GFDIDs and display info
                        for gfd_id in gfdid_df['GFDID']:
                            gfdid_info_df = query_gfdid_information(gfd_id, session)
                            tpv_info_df = query_donortpv(gfd_id, session)
    
                            donor_name_formatted = format_donor_name(gfdid_info_df["DONOR_NM"].iloc[0])
                            display_gfdid_and_tpv_info(gfdid_info_df, tpv_info_df, donor_name_formatted, gfd_id)
    
                            # Handle transactions as before
                            success_ids = parse_ids(gfdid_info_df["SUCCESS_IDS"].iloc[0])
                            failed_ids = parse_ids(gfdid_info_df["FAILED_IDS"].iloc[0])
                            transaction_ids = success_ids + failed_ids
                            all_transactions = query_transactions(transaction_ids, session)
    
                            if not all_transactions.empty:
                                display_transactions(all_transactions, success_ids)
                            else:
                                st.warning("No transaction records found.")
                    else:
                        st.write(f"No donor found for the phone number: {phone_number_search}")
                except Exception as e:
                    st.error(f"An error occurred: {str(e)}")


    # Auto searching based on click from other tabs
    if selected_tab == "Donor Search":
        gfd_id = st.session_state.get('search_gfdid', '')
        
        if gfd_id:
            #st.write(f"Auto-searching for GFDID: {gfd_id}")
            st.markdown(f"""
                <div style='text-align: center; background-color: #f0f0f5; padding: 10px; border-radius: 5px; margin-bottom: 20px;'>
                    <div style='margin: 0; font-size: 16px;'><i>Auto-searching for GFD3V00000{gfd_id}</i></div>
                </div>
            """, unsafe_allow_html=True)
            gfdid_df = query_gfdid_information(gfd_id, session)
            tpv_info_df = query_donortpv(gfd_id, session)
            add_to_search_history('GFDID', gfd_id)
            
            # Within the main app logic, call the display_gfdid_and_tpv_info function with gfd_id
            if not gfdid_df.empty:
                donor_name = format_donor_name(gfdid_df["DONOR_NM"].iloc[0])
                display_gfdid_and_tpv_info(gfdid_df, tpv_info_df, donor_name, gfd_id)

                # Get transaction information as before
                success_ids = parse_ids(gfdid_df["SUCCESS_IDS"].iloc[0])
                failed_ids = parse_ids(gfdid_df["FAILED_IDS"].iloc[0])
                transaction_ids = success_ids + failed_ids
                all_transactions = query_transactions(transaction_ids, session)
                
                if not all_transactions.empty:
                    display_transactions(all_transactions, success_ids)
                else:
                    st.warning("No transaction records found.")
            else:
                st.write(f"No information found for GFDID {gfd_id}.")

elif selected_tab == "Search History":
    st.markdown("""
        <div style='text-align: center; background-color: #d1ecf1; padding: 10px; border-radius: 5px; margin-bottom: 20px;'>
            <div style='margin: 0; font-size: 32px;'>Search History</div>
        </div>
    """, unsafe_allow_html=True)

    # Refresh button
    if st.button("Refresh Data", use_container_width=True):
        st.rerun()

    # Retrieve search history data from Snowflake
    search_history_df = get_search_history()

    st.markdown("""
        <div style='text-align: center; background-color: #d9d9e2 ; color: #343a40; padding: 10px; border-radius: 5px; margin-bottom: 10px;'>
            <div style='display: grid; grid-template-columns: 1fr 1fr 1fr 2fr;'>
                <div><strong>User</strong></div>
                <div><strong>Type</strong></div>
                <div><strong>Value</strong></div>
                <div><strong>Timestamp</strong></div>
            </div>
        </div>
    """, unsafe_allow_html=True)

    # Display each search record in a styled grey container
    for _, row in search_history_df.iterrows():
        user = row['USER_NAME']
        type = row['SEARCH_TYPE']
        value = row['GFDID']
        timestamp = row['TIMESTAMP']

        # Display each record in a grey box with the GFDID and Timestamp
        st.markdown(f"""
            <div style='text-align: center; background-color: #f0f0f5; color: #343a40; padding: 10px; border-radius: 5px; margin-bottom: 10px;'>
                <div style='display: grid; grid-template-columns: 1fr 1fr 1fr 2fr;'>
                    <div>{user}</div>
                    <div>{type}</div>
                    <div>{value}</div>
                    <div>{timestamp}</div>
                </div>
            </div>
        """, unsafe_allow_html=True)

elif selected_tab == "Transactions":
    st.markdown("""
        <div style='text-align: center; background-color: #d1ecf1; padding: 10px; border-radius: 5px; margin-bottom: 20px;'>
            <div style='margin: 0; font-size: 32px;'>Donor Transactions</div>
                <div style='margin: 0; font-size: 16px;'><i>Showing Most Recent 200 Donors, Ordered By Latest Transaction Timestamp</i></div>
        </div>
    """, unsafe_allow_html=True)
    # Code for live data view goes here
    
    # Fetch and display recent transactions
    transactions_df = fetch_recent_transactions()

    # Refresh button
    if st.button("Refresh Data", use_container_width=True):
        st.rerun()
                
    # Display each transaction in the updated format
    for _, row in transactions_df.iterrows():
        donor_name = format_donor_name(row['DONOR_NM'])
        status = "Process Later" if row['Process_Later'] else ("Approved" if row['DONOR_STATUS'] == "Successful" else "Failed")
        gfdid = row['GFDID']
        charity_name = row['CHARITY_NM']
        gift_amt = row['GIFT_AMT']
        donor_acq_dtm = row['FINAL_ATTEMPT_DTM']
        

        if status == "Approved":
            # Full-width section for successful transactions
            st.markdown(f"""
                <div style='background-color: #d4edda; color: #155724; padding: 10px; border-radius: 5px; margin-bottom: 10px;'>
                    <strong>Donor Name:</strong> {donor_name}<br>
                    <strong>GFDID:</strong> GFD3V00000{gfdid}<br>
                    <strong>Charity Name:</strong> {charity_name}<br>
                    <strong>Gift Amount:</strong> ${gift_amt}<br>
                    <strong>Status:</strong> {status}<br>
                    <strong>Latest Transaction At:</strong> {donor_acq_dtm}<br>
                </div>
            """, unsafe_allow_html=True)
        else: # Display failed / Process Laters beside donor info
            # Fetch transaction attempts for the given GFDID
            attempt_df = fetch_gfdid_transactions(gfdid)
            attempt_df = attempt_df.sort_values(by='TRANSACTION_DTM', ascending=True)

            # Two-column layout for failed transactions
            col1, col2 = st.columns([1, 1])

            min_height = len(attempt_df)
            min_height_px = min_height * 78 + 1

            with col1:
                background_color = "#fff3cd" if status == "Process Later" else "#f8d7da"
                text_color = "#856404" if status == "Process Later" else "#721c24"

                st.markdown(f"""
                    <div style='min-height: {min_height_px}px; background-color: {background_color}; color: {text_color}; padding: 10px; margin-bottom: 10px; border-radius: 5px;'>
                        <strong>Donor Name:</strong> {donor_name}<br>
                        <strong>GFDID:</strong> GFD3V00000{gfdid}<br>
                        <strong>Charity Name:</strong> {charity_name}<br>
                        <strong>Gift Amount:</strong> ${gift_amt}<br>
                        <strong>Status:</strong> {status}<br>
                        <strong>Latest Transaction At:</strong> {donor_acq_dtm}<br>
                    </div>
                """, unsafe_allow_html=True)

            with col2: # Display failed transaction details beside failed donor info
                for i, attempt in enumerate(attempt_df.itertuples(), start=1):
                    attempt_status = "Process Later" if attempt.ERROR_TXT == "Process Later" else "Failed"
                    attempt_color = "#fff3cd" if attempt_status == "Process Later" else "#f8d7da"
                    attempt_text_color = "#856404" if attempt_status == "Process Later" else "#721c24"
                    
                    #full_error_text = attempt.ERROR_TXT.replace("\r\n", "<br>").replace("<", "&lt;").replace(">", "&gt;")
                    # Safely handle ERROR_TXT
                    if attempt.ERROR_TXT is not None:
                        full_error_text = attempt.ERROR_TXT.replace("\r\n", "<br>").replace("<", "&lt;").replace(">", "&gt;")
                    else:
                        full_error_text = "No error text available."
                    
                    if attempt.ERROR_TXT != "Process Later":
                        first_line = full_error_text.split("<br>")[0] if "<br>" in full_error_text else full_error_text[:25] + "..."
                        st.markdown(f"""
                            <div style='background-color: {attempt_color}; color: {attempt_text_color}; padding: 10px; border-radius: 5px; margin-bottom: 10px;'>
                                <strong>Attempt #{i}:</strong> {attempt.TRANSACTION_DTM}<br>
                                <details>
                                    <summary style="cursor: pointer; color: {attempt_text_color};"><strong>Error Text:</strong> {first_line}</summary>
                                    <p style="color: {attempt_text_color};">{full_error_text}</p>
                                </details>
                            </div>
                        """, unsafe_allow_html=True)
                    else:
                        st.markdown(f"""
                            <div style='background-color: {attempt_color}; color: {attempt_text_color}; padding: 10px; border-radius: 5px; margin-bottom: 10px;'>
                                <strong>Attempt #{i}:</strong> {attempt.TRANSACTION_DTM}<br>
                                <strong>Status:</strong> {attempt_status}
                            </div>
                        """, unsafe_allow_html=True)
                    
elif selected_tab == "Donors By Charity":
    st.markdown("""
        <div style='text-align: center; background-color: #d1ecf1; padding: 10px; border-radius: 5px; margin-bottom: 20px;'>
            <div style='margin: 0; font-size: 32px;'>Donors By Charity</div>
                <div style='margin: 0; font-size: 16px;'><i>Showing Yesterday & Today's Donors Grouped By Charity</i></div>
        </div>
    """, unsafe_allow_html=True)
    
    # Refresh button
    if st.button("Refresh Data", use_container_width=True):
        st.rerun()

    # Fetch recent transactions grouped by charity
    transactions_df = fetch_recent_charity_transactions()

    # Extract unique charities from the transactions
    charity_options = sorted(transactions_df["CHARITY_NM"].unique())
    charity_options.insert(0, "All Charities")

    # Charity filter dropdown
    selected_charity = st.selectbox("Filter by Charity:", charity_options, key='charity_filter_live_charity_view')
    
    # Filter data based on selected charity
    if selected_charity != "All Charities":
        transactions_df = transactions_df[transactions_df["CHARITY_NM"] == selected_charity]
    
    # Display the filtered transactions grouped by charity
    for charity_name, group in transactions_df.groupby("CHARITY_NM"):
        st.markdown(f"""
            <div style='text-align: center; padding: 10px; background-color: #d1ecf1; border-radius: 5px; margin-top: 20px; margin-bottom: 10px;'>
                <div style='font-size: 24px;'>{charity_name}</div>
            </div>
        """, unsafe_allow_html=True)
        
        for _, row in group.iterrows():
            gfdid = row['GFDID']
            donor_name = format_donor_name(row['DONOR_NM'])
            status = "Process Later" if row['DONOR_STATUS'] == "Process Later" else ("Approved" if row['DONOR_STATUS'] == "Successful" else "Failed")
            timestamp = row['DONOR_ACQ_DTM']
            
            # Define styles based on the transaction status
            if status == "Process Later":
                background_color = "#fff3cd"
                text_color = "#856404"
            elif status == "Approved":
                background_color = "#d4edda"
                text_color = "#155724"
            else:
                background_color = "#f8d7da"
                text_color = "#721c24"
                
            # Display each donor in a yellow container to indicate "Process Later"
            st.markdown(f"""
                <div style='background-color: {background_color}; color: {text_color}; padding: 10px; border-radius: 5px; margin-bottom: 10px;'>
                    <div style='display: grid; grid-template-columns: 1fr 1fr 1fr 1fr; gap: 10px; align-items: center;'>
                        <div style='text-align: center;'>{gfdid}</div>
                        <div style='text-align: center;'>{donor_name}</div>
                        <div style='text-align: center;'>{status}</div>
                        <div style='text-align: center;'>{timestamp}</div>
                    </div>
                </div>
            """, unsafe_allow_html=True)
            
elif selected_tab == "Process Later List":
    st.markdown("""
        <div style='text-align: center; background-color: #d1ecf1; padding: 10px; border-radius: 5px; margin-bottom: 10px;'>
            <div style='margin: 0; font-size: 32px;'>Process Later List</div>
            <div style='margin: 0; font-size: 16px;'><i>Donors with Transactions Marked as "Process Later", Last 21 Days</i></div>
        </div>
    """, unsafe_allow_html=True)
    
    st.markdown("""
        <div style='text-align: center; background-color: #f0f0f5; padding: 10px; border-radius: 5px; margin-bottom: 20px;'>
            <div style='margin: 0px; font-size: 12px;'><i><strong>Show Current Process Later List</strong> shows all donors who are currently on the process later list, including if they had a failed reattempt. <strong>Selecting a date</strong> from the dropdown shows donors that were marked process later on that date.  If green, the reattempt was successful.  If yellow, the reattempt failed and the donor remains on the list.</i></div>
        </div>
    """, unsafe_allow_html=True)

    # Initialize 'default_ind' and 'selected_date' in session state if they don't exist
    if 'default_ind' not in st.session_state:
        st.session_state['default_ind'] = 1  # Default to show the last 30 days view
    
    if 'selected_date' not in st.session_state:
        st.session_state['selected_date'] = None  # No date is selected initially

    # Refresh button
    if st.button("Refresh Data", use_container_width=True):
        st.session_state['default_ind'] = 1  # Reset to show last 30 days view
        st.rerun()



    # Dropdown for selecting a date
    col1, col2 = st.columns([1, 1])
    with col2:
        default_ind = 0 # Reset to pull selected date
        selected_date = st.selectbox("Select Date for 'Process Later' Transactions", seven_days, label_visibility="collapsed", key='process_later_date_selector')
    with col1:
        if st.button("Show Current Process Later List", use_container_width=True):
            default_ind = 1  # Reset the default view

    # Fetch 'Process Later' donors based on the selected date (or the current list if no date is selected)
    process_later_df = fetch_process_later_donors(session, selected_date, default_ind)

    if process_later_df.empty:
        st.warning(f"No Process Later donors found for {selected_date or 'the current view'}.")

    # Display the results grouped by DONOR_ACQ_DTM (date)
    for transaction_date, group in process_later_df.groupby(process_later_df['DONOR_ACQ_DTM'].dt.date, sort=False):
        st.markdown(f"""
            <div style='text-align: center; background-color: #d1ecf1; padding: 10px; border-radius: 5px; margin-top: 20px; margin-bottom: 10px;'>
                {transaction_date.strftime('%B %d, %Y')}
            </div>
        """, unsafe_allow_html=True)

        for _, row in group.iterrows():
            gfdid = row['GFDID']
            donor_name = format_donor_name(row['DONOR_NM'])
            charity_name = row['CHARITY_NM']
            timestamp = row['DONOR_ACQ_DTM']
            status = row['DONOR_STATUS']

            if status == 'Successful':
                bg_color = "#d4edda"  # Light green background for Successful
                text_color = "#155724"  # Dark green text
            else:
                bg_color = "#fff3cd"
                text_color = "#856404"

            # Display each donor in a yellow container to indicate 'Process Later'
            st.markdown(f"""
                <div style='background-color: {bg_color}; color: {text_color}; padding: 10px; border-radius: 5px; margin-bottom: 10px;'>
                    <div style='display: grid; grid-template-columns: 1fr 1fr 1fr 1fr; gap: 10px; align-items: center;'>
                        <div style='text-align: center;'>{gfdid}</div>
                        <div style='text-align: center;'>{donor_name}</div>
                        <div style='text-align: center;'>{charity_name}</div>
                        <div style='text-align: center;'>{timestamp}</div>
                    </div>
                </div>
            """, unsafe_allow_html=True)

# Implement the "Donors with Multiple Failed Attempts" tab
elif selected_tab == "Multiple Failed Attempts":
    st.markdown("""
        <div style='text-align: center; background-color: #d1ecf1; padding: 10px; border-radius: 5px; margin-bottom: 20px;'>
            <div style='margin: 0; font-size: 32px;'>Donors with Multiple Failed Attempts</div>
            <div style='margin: 0; font-size: 16px;'><i>Showing Donors From Past 10 Days</i></div>
        </div>
    """, unsafe_allow_html=True)

    # Refresh button
    if st.button("Refresh Data", use_container_width=True):
        st.rerun()

    # Fetch data and get the list of charities
    multiple_failed_df, charity_list = fetch_donors_with_multiple_failed_attempts(session)
    
    # Selectbox for filtering by charity
    selected_charity = st.selectbox("Filter by Charity", options=charity_list, index=0)
    
    # Apply filter only if a specific charity is selected
    if selected_charity != "All Charities":
        multiple_failed_df, _ = fetch_donors_with_multiple_failed_attempts(session, charity_filter=selected_charity)

    # Display the filtered list of donors
    for _, row in multiple_failed_df.iterrows():
        gfdid = row['GFDID']
        donor_name = format_donor_name(row['DONOR_NM'])
        charity_name = row['CHARITY_NM']
        donor_acq_dtm = row['DONOR_ACQ_DTM']
        failed_cnt = row['FAILED_CNT']
        
        # Define styles for failed attempts
        background_color = "#f8d7da"  # Red for Failed
        text_color = "#721c24"
        
        st.markdown(f"""
            <div style='background-color: {background_color}; color: {text_color}; padding: 10px; border-radius: 5px; margin-bottom: 10px;'>
                <strong>Donor Name:</strong> {donor_name}<br>
                <strong>GFDID:</strong> GFD3V00000{gfdid}<br>
                <strong>Charity Name:</strong> {charity_name}<br>
                <strong>Acquired At:</strong> {donor_acq_dtm}<br>
                <strong>Total Number of Attempts:</strong> {failed_cnt}<br>
            </div>
        """, unsafe_allow_html=True)
        
elif selected_tab == "Charity Overview":
   
    st.markdown(f"""
        <div style='text-align: center; background-color: #d1ecf1; padding: 10px; border-radius: 5px; margin-bottom: 20px;'>
            <div style='margin: 0; font-size: 32px;'>Charity Overview</div>
            <div style='margin: 0; font-size: 16px;'><i>Single Day's Donors by Charity (Insight Only)</i></div>
        </div>
    """, unsafe_allow_html=True)

    # Refresh button
    if st.button("Refresh Data", use_container_width=True):
        st.rerun()
    
    # Step 4: Date Selector
    selected_date = st.selectbox("", seven_days, index=seven_days.index(st.session_state['selected_date']), label_visibility='collapsed', key='date_selector')
    
    # Step 5: Update session state and rerun if the date has changed
    if selected_date != st.session_state['selected_date']:
        st.session_state['selected_date'] = selected_date
        st.rerun()

    # Step 6: Fetch Process Later GFDIDs
    process_later_gfdids = get_process_later_gfdids(session)

    # Step 7: Adjust Query for Charity Overview Based on Selected Date
    if process_later_gfdids:
        process_later_str = ", ".join(f"'{x}'" for x in process_later_gfdids)
    else:
        # Dummy non-matching GFDID to ensure safe SQL
        process_later_str = "'00000000'"

    charity_status_query = f"""
        SELECT 
            INITCAP(CHARITY_NM) AS CHARITY_NM,
            SUM(CASE WHEN SUCCESS_CNT = 1 THEN 1 END) AS SUCCESS_COUNT,
            SUM(CASE WHEN SUCCESS_CNT = 0 AND GFDID NOT IN ({process_later_str}) THEN 1 END) AS FAIL_COUNT,
            SUM(CASE WHEN GFDID IN ({process_later_str}) THEN 1 END) AS PROCESS_LATER_COUNT
        FROM FIVETRAN_DATABASE.ARMOR_SQL_2012_DBO.LOGGING_BY_GFDID
        WHERE DONOR_ACQ_DTM BETWEEN '{selected_date}' AND dateadd('day', 1, '{selected_date}')
        GROUP BY CHARITY_NM
        HAVING SUCCESS_COUNT > 0 OR FAIL_COUNT > 0 OR PROCESS_LATER_COUNT > 0;
    """

    
    # Step 8: Execute the query and handle null values
    charity_status_df = session.sql(charity_status_query).to_pandas()
    charity_status_df.fillna(0, inplace=True)  # Replace NaN with 0

    # Step 9: Format the DataFrame
    charity_status_df['FAIL_COUNT'] = charity_status_df['FAIL_COUNT'].astype(int)
    charity_status_df['SUCCESS_COUNT'] = charity_status_df['SUCCESS_COUNT'].astype(int)
    charity_status_df['PROCESS_LATER_COUNT'] = charity_status_df['PROCESS_LATER_COUNT'].astype(int)

    # Step 10: Display Charity Overview in a 3-column Layout
    # Display charity data in a 3-column format
    for i in range(0, len(charity_status_df), 3):
        cols = st.columns(1 if is_mobile() else 3)
        for j, col in enumerate(cols):
            if i + j < len(charity_status_df):
                charity = charity_status_df.iloc[i + j]
                col.markdown(f"""
                    <div style='
                        display: flex; 
                        flex-direction: column; 
                        justify-content: space-between; 
                        align-items: center; 
                        text-align: center; 
                        background-color: #f0f0f5; 
                        padding: 10px; 
                        border-radius: 5px; 
                        margin-bottom: 10px; 
                        min-height: 150px;
                        height: 100%;
                    '>
                        <div style='flex-grow: 1; width: 100%; padding-bottom: 10px;'>
                            <strong style='white-space: normal;'>{charity['CHARITY_NM']}</strong>
                        </div>
                        <div>
                            <span style='color: #155724;'>Successful Donors: {charity['SUCCESS_COUNT']}</span><br>
                            <span style='color: #721c24;'>Failed Donors: {charity['FAIL_COUNT']}</span><br>
                            <span style='color: #856404;'>Pending Donors: {charity['PROCESS_LATER_COUNT']}</span>
                        </div>
                    </div>
                """, unsafe_allow_html=True)

elif selected_tab == "Reconciliation":
    st.markdown("""
        <div style='text-align: center; background-color: #d1ecf1; padding: 10px; border-radius: 5px; margin-bottom: 20px;'>
            <div style='margin: 0; font-size: 32px;'>Reconciliation</div>
            <div style='margin: 0; font-size: 16px;'><i>Comparing Charity Recon Files to Insight DB (Showing Last 90 Days of Acquisitions)</i></div>
        </div>
    """, unsafe_allow_html=True)

    st.markdown("""
        <div style='text-align: center; background-color: #f0f0f5; padding: 10px; border-radius: 5px; margin-bottom: 20px;'>
            <div style='margin: 0px; font-size: 12px;'><strong>THIS IS NOT TO BE USED FOR RECONCILIATION</strong><br><i>This page compares the reconciliation file on a given week with the status of donors in Insight.  If a record is <strong>green</strong>, the file matches Insight.  If a record is <strong>red</strong>, the Charity shows approved and Insight does not.  <strong>Grey</strong> records are cancelled by the charity.</i></div>
        </div>
    """, unsafe_allow_html=True)

    # Refresh button
    if st.button("Refresh Data", use_container_width=True):
        st.rerun()

    # Query the last 7 LOAD_DATEs for dropdown selection
    load_date_query = """
        SELECT DISTINCT LOAD_DATE 
        FROM RECONCILE_DB.TRANSFORM.RAW_TRANSFORM_UNIFIED 
        WHERE LOAD_DATE IS NOT NULL
        ORDER BY LOAD_DATE DESC 
        LIMIT 7
    """
    load_dates = session.sql(load_date_query).to_pandas()['LOAD_DATE'].tolist()
    
    # Create two columns for the dropdowns
    col1, col2 = st.columns(1 if is_mobile() else 2)
    
    # LOAD_DATE dropdown in the first column
    with col1:
        selected_load_date = st.selectbox("Select Recon File Date:", load_dates, index=0)
    
    # Fetch available CHARITY_NAMEs for the selected LOAD_DATE and place it in the second column
    with col2:
        charity_query = f"""
            SELECT DISTINCT CHARITY_NAME 
            FROM RECONCILE_DB.TRANSFORM.RAW_TRANSFORM_UNIFIED 
            WHERE LOAD_DATE = '{selected_load_date}'
            ORDER BY CHARITY_NAME ASC
        """
        charities = session.sql(charity_query).to_pandas()['CHARITY_NAME'].tolist()
        selected_charity = st.selectbox("Select Charity:", charities, index=0)

    # Place the 3-column summary directly below the dropdowns
    # Query to count GFD_IDs by CHARITY_STATUS
    summary_query = f"""
        SELECT 
            CHARITY_STATUS,
            COUNT(GFD_ID) AS STATUS_COUNT
        FROM 
            FIVETRAN_DATABASE.ARMOR_SQL_2012_DBO.RECON_TPV_VIEW
        WHERE 
            CHARITY_NAME = '{selected_charity}' 
            AND LOAD_DATE = '{selected_load_date}'
            AND TO_DATE(PAYMENT_DT) >= DATEADD('day', -90, '{selected_load_date}')
        GROUP BY CHARITY_STATUS
    """
    summary_df = session.sql(summary_query).to_pandas()

    net_query = f"""
        SELECT 
            GFD_STATUS,
            COUNT(GFD_ID) AS STATUS_COUNT
        FROM 
            FIVETRAN_DATABASE.ARMOR_SQL_2012_DBO.RECON_TPV_VIEW
        WHERE 
            CHARITY_NAME = '{selected_charity}' 
            AND LOAD_DATE = '{selected_load_date}'
            AND TO_DATE(PAYMENT_DT) >= DATEADD('day', -90, '{selected_load_date}')
            AND CHARITY_STATUS = 'NET DONOR'
        GROUP BY GFD_STATUS
    """
    net_df = session.sql(net_query).to_pandas()

    can_query = f"""
        SELECT 
            GFD_STATUS,
            COUNT(GFD_ID) AS STATUS_COUNT
        FROM 
            FIVETRAN_DATABASE.ARMOR_SQL_2012_DBO.RECON_TPV_VIEW
        WHERE 
            CHARITY_NAME = '{selected_charity}' 
            AND LOAD_DATE = '{selected_load_date}'
            AND TO_DATE(PAYMENT_DT) >= DATEADD('day', -90, '{selected_load_date}')
            AND CHARITY_STATUS = 'CANCELED'
        GROUP BY GFD_STATUS
    """
    can_df = session.sql(can_query).to_pandas()

    otg_query = f"""
        SELECT 
            GFD_STATUS,
            COUNT(GFD_ID) AS STATUS_COUNT
        FROM 
            FIVETRAN_DATABASE.ARMOR_SQL_2012_DBO.RECON_TPV_VIEW
        WHERE 
            CHARITY_NAME = '{selected_charity}' 
            AND LOAD_DATE = '{selected_load_date}'
            AND TO_DATE(PAYMENT_DT) >= DATEADD('day', -90, '{selected_load_date}')
            AND CHARITY_STATUS = 'OTG'
        GROUP BY GFD_STATUS
    """
    otg_df = session.sql(otg_query).to_pandas()
    
    # Extract counts based on CHARITY_STATUS
    net_donor_count = summary_df.loc[summary_df['CHARITY_STATUS'] == 'NET DONOR', 'STATUS_COUNT'].sum()
    canceled_count = summary_df.loc[summary_df['CHARITY_STATUS'] == 'CANCELED', 'STATUS_COUNT'].sum()
    otg_count = summary_df.loc[summary_df['CHARITY_STATUS'] == 'OTG', 'STATUS_COUNT'].sum()
    
    # Define a function to render the summary with dynamic GFD status breakdown
    def render_summary(df, title, background_color, text_color):
        # Total count for the main status
        total_count = df['STATUS_COUNT'].sum()
        
        # Generate the breakdown as HTML formatted strings for each GFD status
        breakdown_html = ""
        for _, row in df.iterrows():
            breakdown_html += f"<strong>{row['GFD_STATUS']}:</strong> {row['STATUS_COUNT']}<br>"
    
        # Render the summary box
        st.markdown(f"""
            <div style='background-color: {background_color}; color: {text_color}; padding: 10px; border-radius: 5px;'>
                <div style='text-align: center; font-size: 18px;'><strong>{title}: {total_count}</strong></div>
                <div style='text-align: center; font-size: 16px; color: {text_color};'>{breakdown_html}</div>
            </div>
        """, unsafe_allow_html=True)
    
    # Define background and text colors for each category
    net_donor_bg = "#d4edda"  # Light green for NET DONOR
    net_donor_text_color = "#155724" 
    
    canceled_bg = "#f8d7da"  # Light red for CANCELED
    canceled_text_color = "#721c24"
    
    otg_bg = "#e2e3e5"  # Light grey for OTG
    otg_text_color = "#6c757d"
    
    # Display each status summary in a column
    col1, col2, col3 = st.columns(1 if is_mobile() else 3)
    with col1:
        render_summary(net_df, "NET DONOR", net_donor_bg, net_donor_text_color)
    with col2:
        render_summary(can_df, "CANCELED", canceled_bg, canceled_text_color)
    with col3:
        render_summary(otg_df, "OTG", otg_bg, otg_text_color)

    # Query for GFD_ID, FIRST_PAYMENT_DATE, DONOR_STATUS, and join with donortpv for GFD Status
    reconciliation_query = f"""
        SELECT 
            GFD_ID, 
            TO_DATE(PAYMENT_DT) AS PAYMENT_DT, 
            CHARITY_STATUS, 
            GFD_STATUS
        FROM 
            FIVETRAN_DATABASE.ARMOR_SQL_2012_DBO.RECON_TPV_VIEW
        WHERE 
            CHARITY_NAME = '{selected_charity}' 
            AND LOAD_DATE = '{selected_load_date}'
        HAVING PAYMENT_DT >= DATEADD('day', -90, '{selected_load_date}')
        ORDER BY PAYMENT_DT DESC, GFD_ID DESC
    """
    recon_df = session.sql(reconciliation_query).to_pandas()

    # Iterate over each row and apply color formatting based on DONOR_STATUS
    st.markdown(f"""
        <div style='background-color: #d1ecf1;padding: 10px; border-radius: 5px; margin-bottom: 10px; margin-top: 10px;'>
            <div style='display: grid; grid-template-columns: 1fr 1fr 1fr 1fr; gap: 10px; align-items: center;'>
                <div style='text-align: center;'>GFD ID</div>
                <div style='text-align: center;'>Acquisition Date</div>
                <div style='text-align: center;'>Charity Status</div>
                <div style='text-align: center;'>GFD Status</div>
            </div>
        </div>
    """, unsafe_allow_html=True)
    
    for _, row in recon_df.iterrows():
        gfd_id = row['GFD_ID']
        first_payment_date = row['PAYMENT_DT']
        donor_status = row['CHARITY_STATUS']
        gfd_status = row['GFD_STATUS']

        # Define colors based on donor_status
        if donor_status == 'NET DONOR' and (gfd_status == 'Approved Net' or gfd_status == 'Approved Gross'):
            background_color = "#d4edda"
            text_color = "#155724"
        elif donor_status == 'NET DONOR':
            background_color = "#f8d7da"
            text_color = "#721c24"
        else:  # OTG or other statuses
            background_color = "#e2e3e5"
            text_color = "#6c757d"

        # Display each record with the chosen style
        st.markdown(f"""
            <div style='background-color: {background_color}; color: {text_color}; padding: 10px; border-radius: 5px; margin-bottom: 10px;'>
                <div style='display: grid; grid-template-columns: 1fr 1fr 1fr 1fr; gap: 10px; align-items: center;'>
                    <div style='text-align: center;'>{gfd_id}</div>
                    <div style='text-align: center;'>{first_payment_date}</div>
                    <div style='text-align: center;'>{donor_status}</div>
                    <div style='text-align: center;'>{gfd_status}</div>
                </div>
            </div>
        """, unsafe_allow_html=True)

elif selected_tab == "Donors By Office":
    st.markdown("""
        <div style='text-align: center; background-color: #d1ecf1; padding: 10px; border-radius: 5px; margin-bottom: 20px;'>
            <div style='margin: 0; font-size: 32px;'>Donors By Office</div>
            <div style='margin: 0; font-size: 16px;'><i>By Fundraiser</i></div>
        </div>
    """, unsafe_allow_html=True)

    # Refresh button
    if st.button("Refresh Data", use_container_width=True):
        st.rerun()

    # Create two columns for the dropdowns
    col1, col2 = st.columns(1 if is_mobile() else 2)
    
    # LOAD_DATE dropdown in the first column
    with col1:
        selected_date = st.selectbox("Select Date:", seven_days, index=0, label_visibility='collapsed')
    
    # Fetch available Offices
    with col2:
        # Step 2: Query to fetch available offices on the selected date
        office_query = f"""
            SELECT DISTINCT B.OFFICECODE
            FROM FIVETRAN_DATABASE.ARMOR_SQL_2012_DBO.LOGGING_BY_GFDID A
            LEFT JOIN PHOENIX_DW.DIMENSION.FUNDRAISER B ON A.REPCODE = B.REPCODE
            WHERE DONOR_ACQ_DTM BETWEEN '{selected_date}' AND DATEADD(day, 1, '{selected_date}')
            ORDER BY B.OFFICECODE
        """
        office_options = session.sql(office_query).to_pandas()['OFFICECODE'].tolist()
        office_options.insert(0, "All Offices")  # Add "All Offices" option to reset the filter

        # Step 3: Office filter dropdown
        selected_office = st.selectbox("Filter by Office:", office_options, key='office_filter_by_office', label_visibility='collapsed')

    # Query data with join to FUNDRAISER table for Office Code and Fundraiser Name
    donors_query = f"""
        SELECT A.*, B.FIRSTNAME, B.LASTNAME, B.OFFICECODE, C.CITY, C.PROVINCE, C.COUNTRY, C.MANAGER
        FROM FIVETRAN_DATABASE.ARMOR_SQL_2012_DBO.LOGGING_BY_GFDID A
        LEFT JOIN PHOENIX_DW.DIMENSION.FUNDRAISER B ON A.REPCODE = B.REPCODE
        LEFT JOIN FIVETRAN_DATABASE.ARMOR_SQL_2012_DBO.DISTINCT_OFFICES C ON B.OFFICECODE = C.CODE
        WHERE DONOR_ACQ_DTM BETWEEN '{selected_date}' AND DATEADD(day, 1, '{selected_date}')
            AND '{selected_date}' BETWEEN C.EFFECTIVEDATE AND C.EXPIRATIONDATE
    """
    
    if selected_office != "All Offices":
        donors_query += f" AND B.OFFICECODE = '{selected_office}'"
    
    donors_by_office_df = session.sql(donors_query).to_pandas()

    if donors_by_office_df.empty:
        st.warning(f"No Donors found for {selected_date or 'the current view'}.")
    
    # Sort by OFFICECODE, REPCODE
    donors_by_office_df.sort_values(by=['OFFICECODE', 'REPCODE', 'DONOR_ACQ_DTM'], ascending=[True, True, False], inplace=True)

    # Group by OFFICECODE and REPCODE, and display in nested format
    for office_code, office_group in donors_by_office_df.groupby('OFFICECODE'):
        if not office_group.empty:  # Only display if there are donors for the office
            city = office_group['CITY'].iloc[0]
            province = office_group['PROVINCE'].iloc[0]
            country = office_group['COUNTRY'].iloc[0]
            manager = office_group['MANAGER'].iloc[0]
            
            # Blue header for Office Code
            st.markdown(f"""
                <div style='text-align: center; background-color: #d1ecf1; padding: 10px; border-radius: 5px; margin-top: 20px; margin-bottom: 10px;'>
                    <strong>{office_code}</strong> | {manager}<br>
                    <i>{city}, {province}, {country}</i>
                </div>
            """, unsafe_allow_html=True)
            
            for repcode, rep_group in office_group.groupby('REPCODE'):
                if not rep_group.empty:  # Only display if there are donors for the fundraiser
                    rep_name = rep_group['FIRSTNAME'].iloc[0] + " " + rep_group['LASTNAME'].iloc[0]
                    # Lighter blue background for Fundraiser
                    st.markdown(f"""
                        <div style='text-align: center; background-color: #e3f2fd; padding: 10px; border-radius: 5px; margin-bottom: 10px;'>
                            <strong>{repcode}</strong> | {rep_name}
                        </div>
                    """, unsafe_allow_html=True)
                    
                # Iterate over rows within each Fundraiser group to display donor transaction details
                for _, row in rep_group.iterrows():
                    donor_name = format_donor_name(row['DONOR_NM'])
                    status = row['DONOR_STATUS']
                    gift_amt = row['GIFT_AMT']
                    timestamp = row['DONOR_ACQ_DTM']
                    gfdid = row['GFDID']
                    failed_count = row['FAILED_CNT']
                    charity_name = row['CHARITY_NM']
                    
                    # Color styling based on transaction status
                    bg_color = "#d4edda" if status == "Successful" else ("#fff3cd" if status == "Process Later" else "#f8d7da")
                    text_color = "#155724" if status == "Successful" else ("#856404" if status == "Process Later" else "#721c24")
                
                    # Full-width container for donor transaction details
                    st.markdown(f"""
                        <div style='background-color: {bg_color}; color: {text_color}; padding: 10px; border-radius: 5px; margin-bottom: 10px;'>
                            <div style='display: flex; justify-content: space-between;'>
                                <div style='flex: 1;'>
                                    <strong>Charity:</strong> {charity_name}<br>
                                    <strong>Donor Name:</strong> {donor_name}<br>
                                    <strong>GFDID:</strong> GFD3V00000{gfdid}<br>
                                    <strong>Acquisition Date:</strong> {timestamp}<br>
                                </div>
                                <div style='flex: 1;'>
                                    <strong>Amount:</strong> ${gift_amt}<br>
                                    <strong>Status:</strong> {status}<br>
                                    <strong>Failed Transactions:</strong> {failed_count}
                                </div>
                            </div>
                        </div>
                    """, unsafe_allow_html=True)
                
                    # Add full-width container for failed transaction details
                    if status == "Payment Failed":
                        failed_transactions_df = fetch_gfdid_transactions(gfdid)
                        
                        # Loop through each failed transaction attempt to display its details
                        for _, failed_txn_row in failed_transactions_df.iterrows():
                            error_text = failed_txn_row['ERROR_TXT']
                            transaction_time = failed_txn_row['TRANSACTION_DTM']
                            full_error_text = error_text.replace("\n", "<br>")  # Replace newlines with <br> for HTML display
                            first_line = error_text[:25] + '...'   # Get the first line of the error message
                
                            # Full-width container with two columns: transaction time (left) and error text (right)
                            st.markdown(f"""
                                <div style='background-color: #f8d7da; color: #721c24; padding: 10px; border-radius: 5px; margin-bottom: 10px;'>
                                    <div style='display: flex;'>
                                        <div style='flex: 1; padding-right: 0px;'>
                                            <strong>Transaction Time:</strong> {transaction_time}
                                        </div>
                                        <div style='flex: 1; padding-left: 0px;'>
                                            <details>
                                                <summary style="cursor: pointer; color: #721c24;"><strong>Error Text:</strong> {first_line}</summary>
                                                <p style="color: #721c24;">{full_error_text}</p>
                                            </details>
                                        </div>
                                    </div>
                                </div>
                            """, unsafe_allow_html=True)

elif selected_tab == "Live Dashboard":
    
    # Current time and date in local time (PST assumed)
    now = datetime.datetime.now()
    
    # Add 3 hours to get Eastern Time (quick fix)
    now_eastern = now - datetime.timedelta(hours=4)
    today = now_eastern.date()
    
    # Build timestamps for queries
    interval_time = now_eastern
    interval_time_str = interval_time.strftime('%H:%M:%S')
    
    # Date offsets
    yesterday = today - datetime.timedelta(days=1)
    last_week = today - datetime.timedelta(days=7)
    
    # SQL-compatible timestamp strings
    yesterday_str = f"{yesterday} {interval_time_str}"
    lastweek_str = f"{last_week} {interval_time_str}"
    
    # Display labels (e.g., 11:09 PM)
    cutoff_display_time = now_eastern.strftime("%-I:%M %p")  # use %#I on Windows
    yesterday_label = f"Yesterday @ {cutoff_display_time} ET"
    lastweek_label = f"Last Week @ {cutoff_display_time} ET"
    
    # 4. Query cumulative donor counts up to this time block
    historical_query = f"""
        WITH base_data AS (
            SELECT DONOR_ACQ_DTM AS dt
            FROM FIVETRAN_DATABASE.ARMOR_SQL_2012_DBO.LOGGING_BY_GFDID
            WHERE DONOR_STATUS = 'Successful'
              AND GIFT_TYPE = 'Recurring'
              AND REPCODE <> 'TESTFRNACA'
    
            UNION ALL
    
            SELECT DATEADD(HOUR, -4, PAYMENT_DATE) AS dt
            FROM MADEMEDIA.RAW.MADEMEDIA_PROD
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
    
    # 5. Extract counts
    count_yesterday = historical_df["COUNT_YESTERDAY"].iloc[0]
    count_last_week = historical_df["COUNT_LAST_WEEK"].iloc[0]
    count_yesterday_block = historical_df["COUNT_YESTERDAY_BLOCK"].iloc[0]
    count_last_week_block = historical_df["COUNT_LAST_WEEK_BLOCK"].iloc[0]

    goal = count_last_week_block

    # Get approved donors by charity
    query = """
        SELECT 
            INITCAP(CHARITY_NM) AS CHARITY_NM,
            COUNT(*) AS APPROVED_DONOR_COUNT
        FROM FIVETRAN_DATABASE.ARMOR_SQL_2012_DBO.LOGGING_BY_GFDID
        WHERE DONOR_STATUS = 'Successful'
          AND DONOR_ACQ_DTM >= CURRENT_DATE
          AND GIFT_TYPE = 'Recurring'
          AND REPCODE <> 'TESTFRNACA'
        GROUP BY CHARITY_NM
        
        UNION ALL
        
        SELECT
            CASE
                WHEN CHARITY_NAME = 'USO' THEN 'United Service Organizations'
                WHEN CHARITY_NAME = 'NKH' THEN 'No Kid Hungry'
                ELSE INITCAP(CHARITY_NAME) END AS CHARITY_NM,
            COUNT(*) AS APPROVED_DONOR_COUNT
        FROM MADEMEDIA.RAW.MADEMEDIA_PROD
        WHERE DONOR_STATUS = 'approved'
            AND DATEADD(HOUR, -4, PAYMENT_DATE) >= CURRENT_DATE
        GROUP BY CHARITY_NM
        ORDER BY APPROVED_DONOR_COUNT DESC
    """
    df = session.sql(query).to_pandas()
    total = df["APPROVED_DONOR_COUNT"].sum()

    # Total count display
    st.markdown(f"""
        <div style='text-align: center; background-color: #d1ecf1; padding: 10px; border-radius: 5px; margin-bottom: 20px;'>
            <div style='margin: 0; font-size: 32px;'><b>{total:,}</b> Donors Acquired</div>
                <div style='margin: 0; font-size: 16px;'><i>Showing Total Count of Approved Recurring Donors Today, Refreshing Every 5 Minutes<br>MadeMedia Charities Included, Goal set to same day last week.</i></div>
        </div>
    """, unsafe_allow_html=True)

    st.markdown(f"""
        <div style='display: flex; justify-content: space-around; text-align: center; margin-bottom: 15px;'>
            <div>
                <div style='font-size: 14px;'>Yesterday Total</div>
                <div style='font-size: 20px; font-weight: bold;'>{count_yesterday:,}</div>
            </div>
            <div>
                <div style='font-size: 14px;'>{yesterday_label}</div>
                <div style='font-size: 20px; font-weight: bold;'>{count_yesterday_block:,}</div>
            </div>
            <div>
                <div style='font-size: 14px;'>Same Day Last Week</div>
                <div style='font-size: 20px; font-weight: bold;'>{count_last_week:,}</div>
            </div>
            <div>
                <div style='font-size: 14px;'>{lastweek_label}</div>
                <div style='font-size: 20px; font-weight: bold;'>{count_last_week_block:,}</div>
            </div>
        </div>
    """, unsafe_allow_html=True)
    

    # Referencing Goal from top of app
    progress_pct = (total / goal) * 100 if goal > 0 else 0
    bar_color = "#d4edda" if progress_pct >= 100 else "#d1ecf1" 
    
    # Progress bar at the top
    st.markdown(f"""
        <div style='
            background-color: #e9ecef;
            border-radius: 5px;
            height: 30px;
            width: 100%;
            margin-bottom: 20px;
            overflow: hidden;
            position: relative;
        '>
            <div style='
                height: 100%;
                width: {progress_pct:.1f}%;
                background-color: {bar_color};
                transition: width 0.5s ease;
            '></div>
            <div style='
                position: absolute;
                top: 0;
                left: 0;
                right: 0;
                bottom: 0;
                display: flex;
                align-items: center;
                justify-content: center;
                font-weight: bold;
                color: #003366;
                font-size: 16px;
            '>
                {total:,} of {goal} ({progress_pct:.1f}%)
            </div>
        </div>
    """, unsafe_allow_html=True)
    
    if len(df) == 0:
        st.markdown("""
            <div style='text-align: center; padding: 20px; font-size: 20px; color: #888;'>
                No donors acquired yet today.
            </div>
        """, unsafe_allow_html=True)
    else:
        # --- PAGINATION SETTINGS ---
        per_page = 9
        total_pages = (len(df) + per_page - 1) // per_page
    
        if 'live_dashboard_page' not in st.session_state:
            st.session_state.live_dashboard_page = 0
            st.rerun()
    
        current_page = st.session_state.live_dashboard_page
        start = current_page * per_page
        end = start + per_page
        display_df = df.iloc[start:end]
    
        # Display current "slide"
        rows = [display_df[i:i+3] for i in range(0, len(display_df), 3)]
        for row_df in rows:
            cols = st.columns(3)
            for i, (_, row) in enumerate(row_df.iterrows()):
                with cols[i]:
                    bg_color = "#fcf5fb" if row['CHARITY_NM'] in made_media_charities else "#f8f9fa"
                    st.markdown(f"""
                        <div style='
                            background-color: {bg_color};
                            border: 3px solid #ffffff;
                            border-radius: 10px;
                            height: 200px;
                            padding: 20px;
                            display: flex;
                            flex-direction: column;
                            justify-content: center;
                            align-items: center;
                            text-align: center;
                            font-weight: 600;
                            color: #003366;
                            margin-bottom: 20px;
                        '>
                            <div style='
                                font-size: 20px;
                                line-height: 1.2;
                                max-height: 3.6em;
                                overflow: hidden;
                                text-overflow: ellipsis;
                                display: -webkit-box;
                                -webkit-line-clamp: 3;
                                -webkit-box-orient: vertical;
                                margin-bottom: 10px;
                            '>
                                {row['CHARITY_NM']}
                            </div>
                            <div style='
                                font-size: 36px;
                                font-weight: bold;
                            '>
                                {row['APPROVED_DONOR_COUNT']:,}
                            </div>
                        </div>
                    """, unsafe_allow_html=True)
    
        # Advance to next page after delay
        time.sleep(15)
        st.session_state.live_dashboard_page = (current_page + 1) % total_pages
        st.rerun()


# New tab: Payment Processor Status
elif selected_tab == "Payment Processor Status":
    st.markdown("""
        <div style='text-align: center; background-color: #d1ecf1; padding: 10px; border-radius: 5px; margin-bottom: 20px;'>
            <div style='margin: 0; font-size: 32px;'>Payment Processor Status</div>
            <div style='margin: 0; font-size: 16px;'><i>Status of the last 5 transactions per charity, grouped by processor, in last 24 hours</i></div>
        </div>
    """, unsafe_allow_html=True)

    # Refresh button
    if st.button("Refresh Data", use_container_width=True):
        st.rerun()

    # Query: Get mapping of charities to payment processors
    processor_query = session.sql("""
        SELECT INITCAP(CHARITYNAME) AS CHARITY_NM, CHARITYCODE, PAYMENTPROCESSOR
        FROM PHOENIX_DW.REPORT.CHARITY_PAYMENT_PROCESSOR
    """)
    processor_df = processor_query.to_pandas()

    # Get the last 5 transactions per charity from the logging table
    transaction_query = session.sql("""
        SELECT * FROM (
            SELECT *,
                   ROW_NUMBER() OVER (PARTITION BY INITCAP(CHARITY_NM) ORDER BY FINAL_ATTEMPT_DTM DESC) AS rn
            FROM FIVETRAN_DATABASE.ARMOR_SQL_2012_DBO.LOGGING_BY_GFDID
            WHERE FINAL_ATTEMPT_DTM > CURRENT_DATE - 2
        )
        WHERE rn <= 5
    """)
    transaction_df = transaction_query.to_pandas()
    transaction_df["CHARITY_NM"] = transaction_df["CHARITY_NM"].str.title()

    # Join the processor data with the last 5 transactions
    merged_df = transaction_df.merge(processor_df, on="CHARITY_NM", how="left")

    # Group by processor
    grouped = merged_df.groupby("PAYMENTPROCESSOR")

    for processor, group in grouped:
        st.markdown(f"""
            <div style='text-align: center; background-color: #f0f0f5; padding: 10px; border-radius: 5px; margin-top: 20px; margin-bottom: 10px;'>
                <div style='font-size: 24px;'><strong>{processor}</strong></div>
            </div>
        """, unsafe_allow_html=True)

        charities = list(group.groupby("CHARITY_NM"))

        for i in range(0, len(charities), 3):
            cols = st.columns(1 if is_mobile() else 3)
            for j in range(3):
                if i + j < len(charities):
                    charity, cgroup = charities[i + j]
                    total = len(cgroup)
                    success_count = (cgroup["DONOR_STATUS"] == "Successful").sum()

                    # Determine color based on number of successes
                    if success_count == total and total > 0:
                        bg_color = "#d4edda"  # green
                    elif success_count >= 3:
                        bg_color = "#fff3cd"  # yellow
                    else:
                        bg_color = "#f8d7da"  # red

                    gfdids = cgroup.loc[cgroup["DONOR_STATUS"] != "Successful", "GFDID"].astype(str).tolist()
                    gfdid_text = "Unsuccessful GFDIDs: " + ", ".join(gfdids) if gfdids else ""

                    with cols[j]:
                        st.markdown(f"""
                            <div style='
                                background-color: {bg_color};
                                border-radius: 10px;
                                padding: 20px;
                                text-align: center;
                                font-weight: bold;
                                font-size: 18px;
                                color: #003366;
                                margin-bottom: 20px;
                                height: 160px;
                                display: flex;
                                flex-direction: column;
                                justify-content: center;
                            '>
                                {charity}<br>
                                <span style='font-size: 14px;'>Last {total} Txns: {success_count}/{total} successful</span><br>
                                <span style='font-size: 12px; color: #721c24;'>{gfdid_text}</span>
                            </div>
                        """, unsafe_allow_html=True)

# Tab for calling out prepaids/gift cards by charity
elif selected_tab == "Prepaid & Gift Monitoring":
    st.markdown("""
        <div style='text-align: center; background-color: #d1ecf1; padding: 10px; border-radius: 5px; margin-bottom: 20px;'>
            <div style='margin: 0; font-size: 32px;'>Prepaid & Gift Monitoring</div>
            <div style='margin: 0; font-size: 16px;'><i>Prepaid & Gift Card Donors by Charity in last 24 hours</i></div>
        </div>
    """, unsafe_allow_html=True)

    if st.button("Refresh Data", use_container_width=True):
        st.rerun()

    bin_df = fetch_prepaid_or_giftcard_details(session)

    if bin_df.empty:
        st.warning("No prepaid or gift card donors found.")
    else:
        for charity_name, group in bin_df.groupby("CHARITY"):
            st.markdown(f"""
                <div style='text-align: center; padding: 10px; background-color: #f0f0f5; border-radius: 5px; margin-top: 20px; margin-bottom: 10px;'>
                    <div style='font-size: 24px;'>{charity_name}</div>
                </div>
            """, unsafe_allow_html=True)

            for i in range(0, len(group), 3):
                cols = st.columns(3)
                for j, col in enumerate(cols):
                    if i + j < len(group):
                        row = group.iloc[i + j]
                        col.markdown(f"""
                            <div style='
                                background-color: #f5f5f5;
                                padding: 15px;
                                border-radius: 8px;
                                margin-bottom: 15px;
                                font-size: 14px;
                            '>
                                <strong>GFDID:</strong> {row['GFDID']}<br>
                                <strong>Fundraiser:</strong> {row['FUNDRAISER']}<br>
                                <strong>Timestamp:</strong> {row['TRANSACTION_DTM']}<br>
                                <strong>Card Level:</strong> {row['CARDLEVEL']}
                            </div>
                        """, unsafe_allow_html=True)

elif selected_tab == "Weekly Donor Trends":
    import altair as alt

    st.markdown("""
        <div style='text-align: center; background-color: #d1ecf1; padding: 10px; border-radius: 5px; margin-bottom: 20px;'>
            <div style='margin: 0; font-size: 32px;'>Weekly Donor Trends</div>
            <div style='margin: 0; font-size: 16px;'><i>Daily totals of successful and declined recurring donors over the last 7 days</i></div>
        </div>
    """, unsafe_allow_html=True)

    if st.button("Refresh Data", use_container_width=True):
        st.rerun()

    query = """
        SELECT 
            TO_DATE(DONOR_ACQ_DTM) AS DATE,
            COUNT_IF(DONOR_STATUS = 'Successful') AS SUCCESSFUL,
            COUNT_IF(DONOR_STATUS = 'Payment Failed') AS DECLINED
        FROM FIVETRAN_DATABASE.ARMOR_SQL_2012_DBO.LOGGING_BY_GFDID
        WHERE DONOR_ACQ_DTM >= DATEADD(DAY, -14, CURRENT_DATE)
          AND GIFT_TYPE = 'Recurring'
        GROUP BY DATE
        ORDER BY DATE
    """
    df = session.sql(query).to_pandas()
    
    st.markdown("""
        <div style='text-align: center; background-color: #f0f0f5; padding: 10px; border-radius: 5px; margin-top: 20px; margin-bottom: 20px;'>
            <div style='margin: 0; font-size: 24px;'>Gross Donor Results by Day</div>
            <div style='margin: 0; font-size: 14px;'><i>Line chart showing daily counts of successful and declined recurring donors</i></div>
        </div>
    """, unsafe_allow_html=True)

    if df.empty:
        st.warning("No donor records found in the past 30 days.")
    else:
        # Ensure DATE column is datetime
        df["DATE"] = pd.to_datetime(df["DATE"])
        df["LABEL"] = df["DATE"].dt.strftime("%b %-d (%a)")  # e.g., Jun 7 (Fri)
        df_melted = df.melt(id_vars=["DATE", "LABEL"], value_vars=["SUCCESSFUL", "DECLINED"], 
                            var_name="STATUS", value_name="COUNT")

        chart = alt.Chart(df_melted).mark_line(point=True).encode(
            x=alt.X("LABEL:N", title="Date (Day of Week)", sort=df["LABEL"].tolist()),
            y=alt.Y("COUNT:Q", title="Donor Count"),
            color=alt.Color("STATUS:N", scale=alt.Scale(
                domain=["SUCCESSFUL", "DECLINED"],
                range=["#28a745", "#dc3545"]
            ))
        ).properties(
            width=800,
            height=400
        ).configure_axis(
            labelAngle=-45
        )

        st.altair_chart(chart, use_container_width=True)

    # === Chart 2: Counts by Payment Processor ===
    st.markdown("""
        <div style='text-align: center; background-color: #f0f0f5; padding: 10px; border-radius: 5px; margin-top: 40px; margin-bottom: 20px;'>
            <div style='margin: 0; font-size: 24px;'>Donor Results by Payment Processor</div>
            <div style='margin: 0; font-size: 14px;'><i>Breakdown of successful vs declined recurring donors by processor</i></div>
        </div>
    """, unsafe_allow_html=True)

    processor_query = """
        SELECT 
            INITCAP(p.PAYMENTPROCESSOR) AS PROCESSOR,
            COUNT_IF(g.DONOR_STATUS = 'Successful') AS SUCCESSFUL,
            COUNT_IF(g.DONOR_STATUS = 'Payment Failed') AS DECLINED
        FROM FIVETRAN_DATABASE.ARMOR_SQL_2012_DBO.LOGGING_BY_GFDID g
        LEFT JOIN PHOENIX_DW.REPORT.CHARITY_PAYMENT_PROCESSOR p 
            ON INITCAP(g.CHARITY_NM) = INITCAP(p.CHARITYNAME)
        WHERE g.DONOR_ACQ_DTM >= DATEADD(DAY, -7, CURRENT_DATE)
          AND g.GIFT_TYPE = 'Recurring'
        GROUP BY PROCESSOR
        ORDER BY PROCESSOR
    """

    processor_df = session.sql(processor_query).to_pandas()

    if not processor_df.empty:
        processor_melted = processor_df.melt(
            id_vars="PROCESSOR", 
            value_vars=["SUCCESSFUL", "DECLINED"], 
            var_name="STATUS", 
            value_name="COUNT"
        )

        bar_chart = alt.Chart(processor_melted).mark_bar().encode(
            x=alt.X("PROCESSOR:N", title="Payment Processor"),
            y=alt.Y("COUNT:Q", title="Donor Count"),
            color=alt.Color("STATUS:N", scale=alt.Scale(
                domain=["SUCCESSFUL", "DECLINED"],
                range=["#28a745", "#dc3545"]
            )),
            tooltip=["PROCESSOR", "STATUS", "COUNT"]
        ).properties(
            width=800,
            height=400
        )

        st.altair_chart(bar_chart, use_container_width=True)
    else:
        st.info("No payment processor data found for the last 30 days.")


    # === Chart 3: Counts by Charity ===
    st.markdown("""
        <div style='text-align: center; background-color: #f0f0f5; padding: 10px; border-radius: 5px; margin-top: 40px; margin-bottom: 20px;'>
            <div style='margin: 0; font-size: 24px;'>Donor Results by Charity</div>
            <div style='margin: 0; font-size: 14px;'><i>Breakdown of successful vs declined recurring donors by charity</i></div>
        </div>
    """, unsafe_allow_html=True)

    charity_query = """
        SELECT 
            INITCAP(CHARITY_NM) AS CHARITY,
            COUNT_IF(DONOR_STATUS = 'Successful') AS SUCCESSFUL,
            COUNT_IF(DONOR_STATUS = 'Payment Failed') AS DECLINED
        FROM FIVETRAN_DATABASE.ARMOR_SQL_2012_DBO.LOGGING_BY_GFDID
        WHERE DONOR_ACQ_DTM >= DATEADD(DAY, -7, CURRENT_DATE)
          AND GIFT_TYPE = 'Recurring'
        GROUP BY CHARITY
        ORDER BY CHARITY
    """

    charity_df = session.sql(charity_query).to_pandas()

    if not charity_df.empty:
        melted_charity_df = charity_df.melt(
            id_vars="CHARITY", 
            value_vars=["SUCCESSFUL", "DECLINED"], 
            var_name="STATUS", 
            value_name="COUNT"
        )

        charity_chart = alt.Chart(melted_charity_df).mark_bar().encode(
            x=alt.X("CHARITY:N", title="Charity", sort="-y"),
            y=alt.Y("COUNT:Q", title="Donor Count"),
            color=alt.Color("STATUS:N", scale=alt.Scale(
                domain=["SUCCESSFUL", "DECLINED"],
                range=["#28a745", "#dc3545"]
            )),
            tooltip=["CHARITY", "STATUS", "COUNT"]
        ).properties(
            width=800,
            height=600
        ).configure_axisX(
            labelAngle=-45
        )

        st.altair_chart(charity_chart, use_container_width=True)
    else:
        st.info("No charity-level donor records found in the last 30 days.")

    # === Chart 4: Counts by Office ===
    st.markdown("""
        <div style='text-align: center; background-color: #f0f0f5; padding: 10px; border-radius: 5px; margin-top: 40px; margin-bottom: 20px;'>
            <div style='margin: 0; font-size: 24px;'>Donor Results by Office</div>
            <div style='margin: 0; font-size: 14px;'><i>Breakdown of successful vs declined recurring donors by office</i></div>
        </div>
    """, unsafe_allow_html=True)

    office_query = """
        SELECT 
            INITCAP(fr.OFFICECODE) AS OFFICE,
            COUNT_IF(g.DONOR_STATUS = 'Successful') AS SUCCESSFUL,
            COUNT_IF(g.DONOR_STATUS = 'Payment Failed') AS DECLINED
        FROM FIVETRAN_DATABASE.ARMOR_SQL_2012_DBO.LOGGING_BY_GFDID g
        LEFT JOIN PHOENIX_DW.DIMENSION.FUNDRAISER fr
            ON g.REPCODE = fr.REPCODE
        WHERE g.DONOR_ACQ_DTM >= DATEADD(DAY, -7, CURRENT_DATE)
          AND g.GIFT_TYPE = 'Recurring'
        GROUP BY OFFICE
        ORDER BY OFFICE
    """

    office_df = session.sql(office_query).to_pandas()

    if not office_df.empty:
        melted_office_df = office_df.melt(
            id_vars="OFFICE", 
            value_vars=["SUCCESSFUL", "DECLINED"], 
            var_name="STATUS", 
            value_name="COUNT"
        )

        office_chart = alt.Chart(melted_office_df).mark_bar().encode(
            x=alt.X("OFFICE:N", title="Office Code"),
            y=alt.Y("COUNT:Q", title="Donor Count"),
            color=alt.Color("STATUS:N", scale=alt.Scale(
                domain=["SUCCESSFUL", "DECLINED"],
                range=["#28a745", "#dc3545"]
            )),
            tooltip=["OFFICE", "STATUS", "COUNT"]
        ).properties(
            width=800,
            height=400
        ).configure_axisX(
            labelAngle=-45
        )

        st.altair_chart(office_chart, use_container_width=True)
    else:
        st.info("No office-level donor records found in the last 7 days.")

    # === Chart 5: Counts by Card Type ===
    st.markdown("""
        <div style='text-align: center; background-color: #f0f0f5; padding: 10px; border-radius: 5px; margin-top: 40px; margin-bottom: 20px;'>
            <div style='margin: 0; font-size: 24px;'>Donor Results by Card Type</div>
            <div style='margin: 0; font-size: 14px;'><i>Based on BIN metadata for successful and declined recurring donations (last 7 days)</i></div>
        </div>
    """, unsafe_allow_html=True)

    card_type_query = """
        SELECT 
            INITCAP(b.CREDITCARDTYPE) AS CARD_TYPE,
            COUNT_IF(g.DONOR_STATUS = 'Successful') AS SUCCESSFUL,
            COUNT_IF(g.DONOR_STATUS = 'Payment Failed') AS DECLINED
        FROM FIVETRAN_DATABASE.ARMOR_SQL_2012_DBO.LOGGING_BY_GFDID g
        LEFT JOIN FIVETRAN_DATABASE.ARMOR_SQL_2012_DBO.LOGGING_BY_TRANSACTION t 
            ON g.GFDID = t.GFDID
        LEFT JOIN FIVETRAN_DATABASE.ARMOR_SQL_2012_DBO.BIN b 
            ON REPLACE(SUBSTRING(t.BIN, 1, 7), ' ', '') = TO_VARCHAR(b.BIN)
        WHERE g.DONOR_ACQ_DTM >= DATEADD(DAY, -7, CURRENT_DATE)
          AND g.GIFT_TYPE = 'Recurring'
          AND b.CREDITCARDTYPE IS NOT NULL
          AND b._FIVETRAN_DELETED = FALSE
        GROUP BY CARD_TYPE
        ORDER BY CARD_TYPE
    """

    card_df = session.sql(card_type_query).to_pandas()

    if not card_df.empty:
        melted_card_df = card_df.melt(
            id_vars="CARD_TYPE", 
            value_vars=["SUCCESSFUL", "DECLINED"], 
            var_name="STATUS", 
            value_name="COUNT"
        )

        card_chart = alt.Chart(melted_card_df).mark_bar().encode(
            x=alt.X("CARD_TYPE:N", title="Card Type"),
            y=alt.Y("COUNT:Q", title="Donor Count"),
            color=alt.Color("STATUS:N", scale=alt.Scale(
                domain=["SUCCESSFUL", "DECLINED"],
                range=["#28a745", "#dc3545"]
            )),
            tooltip=["CARD_TYPE", "STATUS", "COUNT"]
        ).properties(
            width=800,
            height=400
        ).configure_axisX(
            labelAngle=-45
        )

        st.altair_chart(card_chart, use_container_width=True)
    else:
        st.info("No card type data available for the last 7 days.")

    # === Chart 9: Top Declining BINs ===
    st.markdown("""
        <div style='text-align: center; background-color: #f0f0f5; padding: 10px; border-radius: 5px; margin-top: 40px; margin-bottom: 20px;'>
            <div style='margin: 0; font-size: 24px;'>Top Declining BINs</div>
            <div style='margin: 0; font-size: 14px;'><i>BINs with the highest decline rates over the past 7 days</i></div>
        </div>
    """, unsafe_allow_html=True)

    bin_decline_query = """
        SELECT 
            TO_VARCHAR(b.BIN) AS BIN_NUM,
            INITCAP(b.BANKNAME) AS BANK,
            INITCAP(b.CREDITCARDTYPE) AS CARD_TYPE,
            INITCAP(b.FUNDINGTYPE) AS FUNDING,
            INITCAP(b.CARDLEVEL) AS CARD_LEVEL,
            COUNT_IF(g.DONOR_STATUS = 'Payment Failed') AS DECLINES,
            COUNT(*) AS TOTAL,
            ROUND(100.0 * COUNT_IF(g.DONOR_STATUS = 'Payment Failed') / NULLIF(COUNT(*), 0), 1) AS DECLINE_PCT
        FROM FIVETRAN_DATABASE.ARMOR_SQL_2012_DBO.LOGGING_BY_GFDID g
        JOIN FIVETRAN_DATABASE.ARMOR_SQL_2012_DBO.LOGGING_BY_TRANSACTION t 
            ON g.GFDID = t.GFDID
        JOIN FIVETRAN_DATABASE.ARMOR_SQL_2012_DBO.BIN b 
            ON REPLACE(SUBSTRING(t.BIN, 1, 7), ' ', '') = TO_VARCHAR(b.BIN)
        WHERE g.DONOR_ACQ_DTM >= DATEADD(DAY, -7, CURRENT_DATE)
          AND b._FIVETRAN_DELETED = FALSE
          AND b.BIN IS NOT NULL
        GROUP BY BIN_NUM, BANK, CARD_TYPE, FUNDING, CARD_LEVEL
        HAVING TOTAL >= 3
        ORDER BY DECLINE_PCT DESC, DECLINES DESC
        LIMIT 20
    """

    bin_df = session.sql(bin_decline_query).to_pandas()

    if not bin_df.empty:
        bin_df["LABEL"] = bin_df["BIN_NUM"] + " - " + bin_df["BANK"].fillna("Unknown")
        bin_chart = alt.Chart(bin_df).mark_bar().encode(
            x=alt.X("LABEL:N", title="BIN (Bank)", sort="-y"),
            y=alt.Y("DECLINE_PCT:Q", title="Decline Rate (%)"),
            color=alt.value("#dc3545"),
            tooltip=["BIN_NUM", "BANK", "CARD_TYPE", "FUNDING", "CARD_LEVEL", "DECLINES", "TOTAL", "DECLINE_PCT"]
        ).properties(
            width=800,
            height=500,
            title="Top 20 BINs by Decline Rate"
        ).configure_axisX(
            labelAngle=-45
        )

        st.altair_chart(bin_chart, use_container_width=True)
    else:
        st.info("No BIN-level decline data available for the last 7 days.")

    # === Chart 10: Approval Rate by Charity ===
    st.markdown("""
        <div style='text-align: center; background-color: #f0f0f5; padding: 10px; border-radius: 5px; margin-top: 40px; margin-bottom: 20px;'>
            <div style='margin: 0; font-size: 24px;'>Approval Rate by Charity</div>
            <div style='margin: 0; font-size: 14px;'><i>Percentage of successful recurring donations by charity (last 7 days)</i></div>
        </div>
    """, unsafe_allow_html=True)

    approval_rate_query = """
        SELECT 
            INITCAP(CHARITY_NM) AS CHARITY,
            COUNT_IF(DONOR_STATUS = 'Successful') AS SUCCESSFUL,
            COUNT_IF(DONOR_STATUS = 'Payment Failed') AS DECLINED,
            ROUND(100.0 * COUNT_IF(DONOR_STATUS = 'Successful') / 
                NULLIF(COUNT_IF(DONOR_STATUS IN ('Successful', 'Payment Failed')), 0), 1) AS APPROVAL_RATE
        FROM FIVETRAN_DATABASE.ARMOR_SQL_2012_DBO.LOGGING_BY_GFDID
        WHERE DONOR_ACQ_DTM >= DATEADD(DAY, -7, CURRENT_DATE)
          AND GIFT_TYPE = 'Recurring'
        GROUP BY CHARITY
        HAVING SUCCESSFUL + DECLINED > 0
        ORDER BY APPROVAL_RATE ASC
    """

    approval_df = session.sql(approval_rate_query).to_pandas()

    if not approval_df.empty:
        approval_chart = alt.Chart(approval_df).mark_bar().encode(
            x=alt.X("CHARITY:N", title="Charity", sort="-y"),
            y=alt.Y("APPROVAL_RATE:Q", title="Approval Rate (%)"),
            color=alt.condition(
                alt.datum.APPROVAL_RATE < 70,
                alt.value("#dc3545"),  # red
                alt.value("#28a745")   # green
            ),
            tooltip=["CHARITY", "SUCCESSFUL", "DECLINED", "APPROVAL_RATE"]
        ).properties(
            width=800,
            height=500,
            title="Charity Approval Rate (Last 7 Days)"
        ).configure_axisX(
            labelAngle=-45
        )

        st.altair_chart(approval_chart, use_container_width=True)
    else:
        st.info("No approval rate data available for the last 7 days.")

    # === Chart 11: Approval Rate by Fundraiser ===
    st.markdown("""
        <div style='text-align: center; background-color: #f0f0f5; padding: 10px; border-radius: 5px; margin-top: 40px; margin-bottom: 20px;'>
            <div style='margin: 0; font-size: 24px;'>Approval Rate by Fundraiser</div>
            <div style='margin: 0; font-size: 14px;'><i>Percentage of successful recurring donations by REPCODE (last 7 days)</i></div>
        </div>
    """, unsafe_allow_html=True)

    fundraiser_query = """
        SELECT 
            g.REPCODE,
            INITCAP(f.FIRSTNAME || ' ' || f.LASTNAME) AS FUNDRAISER,
            COUNT_IF(g.DONOR_STATUS = 'Successful') AS SUCCESSFUL,
            COUNT_IF(g.DONOR_STATUS = 'Payment Failed') AS DECLINED,
            ROUND(100.0 * COUNT_IF(g.DONOR_STATUS = 'Successful') / 
                NULLIF(COUNT_IF(g.DONOR_STATUS IN ('Successful', 'Payment Failed')), 0), 1) AS APPROVAL_RATE
        FROM FIVETRAN_DATABASE.ARMOR_SQL_2012_DBO.LOGGING_BY_GFDID g
        LEFT JOIN PHOENIX_DW.DIMENSION.FUNDRAISER f 
            ON g.REPCODE = f.REPCODE
        WHERE g.DONOR_ACQ_DTM >= DATEADD(DAY, -7, CURRENT_DATE)
          AND g.GIFT_TYPE = 'Recurring'
        GROUP BY g.REPCODE, FUNDRAISER
        HAVING SUCCESSFUL + DECLINED > 0
        ORDER BY APPROVAL_RATE ASC
        LIMIT 25
    """

    fundraiser_df = session.sql(fundraiser_query).to_pandas()

    if not fundraiser_df.empty:
        fundraiser_df["LABEL"] = fundraiser_df["REPCODE"] + " - " + fundraiser_df["FUNDRAISER"].fillna("Unknown")

        fundraiser_chart = alt.Chart(fundraiser_df).mark_bar().encode(
            x=alt.X("LABEL:N", title="Fundraiser", sort="-y"),
            y=alt.Y("APPROVAL_RATE:Q", title="Approval Rate (%)"),
            color=alt.condition(
                alt.datum.APPROVAL_RATE < 70,
                alt.value("#dc3545"),  # red
                alt.value("#28a745")   # green
            ),
            tooltip=["REPCODE", "FUNDRAISER", "SUCCESSFUL", "DECLINED", "APPROVAL_RATE"]
        ).properties(
            width=800,
            height=500,
            title="Fundraiser Approval Rate (Last 7 Days)"
        ).configure_axisX(
            labelAngle=-45
        )

        st.altair_chart(fundraiser_chart, use_container_width=True)
    else:
        st.info("No fundraiser-level approval data available for the last 7 days.")

    # === Chart 12: Hourly Donor Acquisition Trend ===
    st.markdown("""
        <div style='text-align: center; background-color: #f0f0f5; padding: 10px; border-radius: 5px; margin-top: 40px; margin-bottom: 20px;'>
            <div style='margin: 0; font-size: 24px;'>Hourly Donor Acquisition Trend</div>
            <div style='margin: 0; font-size: 14px;'><i>Successful and declined recurring donations by hour (past 7 days)</i></div>
        </div>
    """, unsafe_allow_html=True)

    hourly_query = """
        SELECT 
            DATE_PART('HOUR', TO_TIMESTAMP(DONOR_ACQ_DTM)) AS HOUR,
            COUNT_IF(DONOR_STATUS = 'Successful') AS SUCCESSFUL,
            COUNT_IF(DONOR_STATUS = 'Payment Failed') AS DECLINED
        FROM FIVETRAN_DATABASE.ARMOR_SQL_2012_DBO.LOGGING_BY_GFDID
        WHERE DONOR_ACQ_DTM >= DATEADD(DAY, -7, CURRENT_DATE)
          AND GIFT_TYPE = 'Recurring'
        GROUP BY HOUR
        ORDER BY HOUR
    """

    hourly_df = session.sql(hourly_query).to_pandas()

    if not hourly_df.empty:
        hourly_df["HOUR"] = hourly_df["HOUR"].astype(int)
        melted_hourly_df = hourly_df.melt(
            id_vars="HOUR", 
            value_vars=["SUCCESSFUL", "DECLINED"], 
            var_name="STATUS", 
            value_name="COUNT"
        )

        hourly_chart = alt.Chart(melted_hourly_df).mark_bar().encode(
            x=alt.X("HOUR:O", title="Hour of Day (0–23)", sort=list(range(24))),
            y=alt.Y("COUNT:Q", title="Donor Count"),
            color=alt.Color("STATUS:N", scale=alt.Scale(
                domain=["SUCCESSFUL", "DECLINED"],
                range=["#28a745", "#dc3545"]
            )),
            tooltip=["HOUR", "STATUS", "COUNT"]
        ).properties(
            width=800,
            height=400,
            title="Hourly Donor Volume (Last 7 Days)"
        )

        st.altair_chart(hourly_chart, use_container_width=True)
    else:
        st.info("No hourly donor activity found in the past 7 days.")

    # === Chart 13: Retry Success After Decline ===
    st.markdown("""
        <div style='text-align: center; background-color: #f0f0f5; padding: 10px; border-radius: 5px; margin-top: 40px; margin-bottom: 20px;'>
            <div style='margin: 0; font-size: 24px;'>Retry Success After Initial Decline</div>
            <div style='margin: 0; font-size: 14px;'><i>Share of recurring donors who initially failed but later succeeded (past 7 days)</i></div>
        </div>
    """, unsafe_allow_html=True)

    retry_query = """
        WITH base AS (
            SELECT 
                GFDID,
                INITCAP(CHARITY_NM) AS CHARITY,
                MIN(CASE WHEN DONOR_STATUS = 'Payment Failed' THEN DONOR_ACQ_DTM END) AS FIRST_FAIL,
                MIN(CASE WHEN DONOR_STATUS = 'Successful' THEN DONOR_ACQ_DTM END) AS FIRST_SUCCESS
            FROM FIVETRAN_DATABASE.ARMOR_SQL_2012_DBO.LOGGING_BY_GFDID
            WHERE DONOR_ACQ_DTM >= DATEADD(DAY, -7, CURRENT_DATE)
              AND GIFT_TYPE = 'Recurring'
            GROUP BY GFDID, CHARITY_NM
        )
        SELECT 
            CHARITY,
            COUNT(*) AS TOTAL_FAILED,
            COUNT_IF(FIRST_SUCCESS > FIRST_FAIL) AS RETRY_SUCCEEDED,
            ROUND(100.0 * COUNT_IF(FIRST_SUCCESS > FIRST_FAIL) / NULLIF(COUNT(*), 0), 1) AS RETRY_SUCCESS_PCT
        FROM base
        WHERE FIRST_FAIL IS NOT NULL
        GROUP BY CHARITY
        ORDER BY RETRY_SUCCESS_PCT DESC
    """

    retry_df = session.sql(retry_query).to_pandas()

    if not retry_df.empty:
        retry_chart = alt.Chart(retry_df).mark_bar().encode(
            x=alt.X("CHARITY:N", title="Charity", sort="-y"),
            y=alt.Y("RETRY_SUCCESS_PCT:Q", title="Retry Success Rate (%)"),
            color=alt.Color("RETRY_SUCCESS_PCT:Q", scale=alt.Scale(scheme="greenblue")),
            tooltip=["CHARITY", "TOTAL_FAILED", "RETRY_SUCCEEDED", "RETRY_SUCCESS_PCT"]
        ).properties(
            width=800,
            height=500,
            title="Retry Success Rate by Charity"
        ).configure_axisX(
            labelAngle=-45
        )

        st.altair_chart(retry_chart, use_container_width=True)
    else:
        st.info("No retry success data available for the last 7 days.")

    # === Chart 14: Donor Failures During Outages ===
    st.markdown("""
        <div style='text-align: center; background-color: #f0f0f5; padding: 10px; border-radius: 5px; margin-top: 40px; margin-bottom: 20px;'>
            <div style='margin: 0; font-size: 24px;'>Donor Failures During System Outages</div>
            <div style='margin: 0; font-size: 14px;'><i>Number of failed recurring donations that occurred during known outages (last 7 days)</i></div>
        </div>
    """, unsafe_allow_html=True)

    outage_query = """
        WITH failures AS (
            SELECT 
                GFDID,
                INITCAP(CHARITY_NM) AS CHARITY,
                DONOR_ACQ_DTM AS FAILURE_TIME
            FROM FIVETRAN_DATABASE.ARMOR_SQL_2012_DBO.LOGGING_BY_GFDID
            WHERE DONOR_STATUS = 'Payment Failed'
              AND GIFT_TYPE = 'Recurring'
              AND DONOR_ACQ_DTM >= DATEADD(DAY, -7, CURRENT_DATE)
        ),
        outages AS (
            SELECT 
                DATEADD('HOUR', -4, CREATED_DATETIME) AS START_TIME,
                DATEADD('HOUR', -4, UPDATED_DATETIME) AS END_TIME
            FROM FIVETRAN_DATABASE.MAKE.OUTAGE_LOG
            WHERE GFD_STATUS <> 'No Impact'
              AND STATUS = 'OUTAGE'
              AND CREATED_DATETIME >= DATEADD(DAY, -7, CURRENT_TIMESTAMP())
        )
        SELECT 
            f.CHARITY,
            COUNT(*) AS TOTAL_FAILURES,
            COUNT_IF(EXISTS (
                SELECT 1 FROM outages o
                WHERE f.FAILURE_TIME BETWEEN o.START_TIME AND o.END_TIME
            )) AS DURING_OUTAGE,
            ROUND(100.0 * COUNT_IF(EXISTS (
                SELECT 1 FROM outages o
                WHERE f.FAILURE_TIME BETWEEN o.START_TIME AND o.END_TIME
            )) / NULLIF(COUNT(*), 0), 1) AS OUTAGE_FAILURE_PCT
        FROM failures f
        GROUP BY f.CHARITY
        HAVING TOTAL_FAILURES > 0
        ORDER BY OUTAGE_FAILURE_PCT ASC
    """

    outage_df = session.sql(outage_query).to_pandas()

    if not outage_df.empty:
        outage_chart = alt.Chart(outage_df).mark_bar().encode(
            x=alt.X("CHARITY:N", title="Charity", sort="-y"),
            y=alt.Y("OUTAGE_FAILURE_PCT:Q", title="Failure Rate During Outages (%)"),
            color=alt.Color("OUTAGE_FAILURE_PCT:Q", scale=alt.Scale(scheme="redpurple")),
            tooltip=["CHARITY", "TOTAL_FAILURES", "DURING_OUTAGE", "OUTAGE_FAILURE_PCT"]
        ).properties(
            width=800,
            height=500,
            title="Failure Rate During Outages by Charity"
        ).configure_axisX(
            labelAngle=-45
        )

        st.altair_chart(outage_chart, use_container_width=True)
    else:
        st.info("No failures matched any outage periods in the last 7 days.")

elif selected_tab == "Drillable Donor Explorer":
    st.markdown("""
        <div style='text-align: center; background-color: #e9f7fc; padding: 12px; border-radius: 8px; margin-bottom: 20px;'>
            <div style='font-size: 28px;'>Drillable Donor Explorer</div>
            <div style='font-size: 14px;'><i>Explore donor behavior across BIN, charity, repcode, retry outcome, and more</i></div>
        </div>
    """, unsafe_allow_html=True)

    # === Inline Filters ===
    col1, col2 = st.columns(2)
    with col1:
        date_range = st.slider("Acquisition Date Range (days)", 1, 30, 14)
    with col2:
        retry_only = st.checkbox("Only Show Retry Successes", value=False)

    # Fetch filter options based on date range
    filter_values_query = f"""
        SELECT DISTINCT 
            INITCAP(g.CHARITY_NM) AS CHARITY,
            g.REPCODE,
            REPLACE(SUBSTRING(t.BIN, 1, 7), ' ', '') AS BIN_PREFIX,
            INITCAP(p.PAYMENTPROCESSOR) AS PROCESSOR
        FROM FIVETRAN_DATABASE.ARMOR_SQL_2012_DBO.LOGGING_BY_GFDID g
        JOIN FIVETRAN_DATABASE.ARMOR_SQL_2012_DBO.LOGGING_BY_TRANSACTION t ON g.GFDID = t.GFDID
        LEFT JOIN PHOENIX_DW.REPORT.CHARITY_PAYMENT_PROCESSOR p 
          ON INITCAP(g.CHARITY_NM) = INITCAP(p.CHARITYNAME)
        WHERE g.DONOR_ACQ_DTM >= DATEADD(DAY, -{date_range}, CURRENT_DATE)
          AND g.GIFT_TYPE = 'Recurring'
    """
    filter_df = session.sql(filter_values_query).to_pandas()
    charity_options = sorted(filter_df["CHARITY"].dropna().unique())
    repcode_options = sorted(filter_df["REPCODE"].dropna().unique())
    bin_options = sorted(filter_df["BIN_PREFIX"].dropna().unique())
    processor_options = sorted(filter_df["PROCESSOR"].dropna().unique())

    col3, col4 = st.columns(2)
    with col3:
        selected_charity = st.multiselect("Charity", options=charity_options)
        selected_rep = st.multiselect("Fundraiser (REPCODE)", options=repcode_options)
    with col4:
        selected_bin = st.multiselect("BIN Prefix", options=bin_options)
        selected_processor = st.multiselect("Payment Processor", options=processor_options)

    # === Core Query ===
    donor_query = f"""
        WITH events AS (
            SELECT 
                g.GFDID,
                INITCAP(g.CHARITY_NM) AS CHARITY,
                g.REPCODE,
                INITCAP(p.PAYMENTPROCESSOR) AS PROCESSOR,
                g.DONOR_STATUS,
                g.DONOR_ACQ_DTM,
                REPLACE(SUBSTRING(t.BIN, 1, 7), ' ', '') AS BIN,
                INITCAP(b.CREDITCARDTYPE) AS CARD_TYPE,
                INITCAP(b.FUNDINGTYPE) AS FUNDING_TYPE,
                INITCAP(b.CARDLEVEL) AS CARD_LEVEL,
                INITCAP(b.BANKNAME) AS BANK
            FROM FIVETRAN_DATABASE.ARMOR_SQL_2012_DBO.LOGGING_BY_GFDID g
            JOIN FIVETRAN_DATABASE.ARMOR_SQL_2012_DBO.LOGGING_BY_TRANSACTION t ON g.GFDID = t.GFDID
            LEFT JOIN PHOENIX_DW.REPORT.CHARITY_PAYMENT_PROCESSOR p 
              ON INITCAP(g.CHARITY_NM) = INITCAP(p.CHARITYNAME)
            LEFT JOIN FIVETRAN_DATABASE.ARMOR_SQL_2012_DBO.BIN b 
              ON REPLACE(SUBSTRING(t.BIN, 1, 7), ' ', '') = TO_VARCHAR(b.BIN)
            WHERE g.DONOR_ACQ_DTM >= DATEADD(DAY, -{date_range}, CURRENT_DATE)
              AND g.GIFT_TYPE = 'Recurring'
              AND b._FIVETRAN_DELETED = FALSE
        ),
        summary AS (
            SELECT
                GFDID,
                CHARITY,
                REPCODE,
                PROCESSOR,
                MIN(CASE WHEN DONOR_STATUS = 'Payment Failed' THEN DONOR_ACQ_DTM END) AS FIRST_FAIL,
                MIN(CASE WHEN DONOR_STATUS = 'Successful' THEN DONOR_ACQ_DTM END) AS FIRST_SUCCESS,
                MAX(DONOR_ACQ_DTM) AS LAST_EVENT,
                ANY_VALUE(BIN) AS BIN,
                ANY_VALUE(CARD_TYPE) AS CARD_TYPE,
                ANY_VALUE(FUNDING_TYPE) AS FUNDING_TYPE,
                ANY_VALUE(CARD_LEVEL) AS CARD_LEVEL,
                ANY_VALUE(BANK) AS BANK
            FROM events
            GROUP BY GFDID, CHARITY, REPCODE, PROCESSOR
        )
        SELECT *,
            CASE 
                WHEN FIRST_FAIL IS NOT NULL AND FIRST_SUCCESS > FIRST_FAIL THEN 'Yes'
                ELSE 'No'
            END AS RETRY_SUCCESS
        FROM summary
    """

    df = session.sql(donor_query).to_pandas()

    # === Apply Filters ===
    if selected_charity:
        df = df[df["CHARITY"].isin(selected_charity)]
    if selected_rep:
        df = df[df["REPCODE"].isin(selected_rep)]
    if selected_bin:
        df = df[df["BIN"].isin(selected_bin)]
    if selected_processor:
        df = df[df["PROCESSOR"].isin(selected_processor)]
    if retry_only:
        df = df[df["RETRY_SUCCESS"] == "Yes"]

    # === Display ===
    if df.empty:
        st.warning("No donors found for the selected filters.")
    else:
        df["FIRST_FAIL"] = pd.to_datetime(df["FIRST_FAIL"])
        df["FIRST_SUCCESS"] = pd.to_datetime(df["FIRST_SUCCESS"])
        df["LAST_EVENT"] = pd.to_datetime(df["LAST_EVENT"])
        df["TIME_TO_SUCCESS"] = (
            (df["FIRST_SUCCESS"] - df["FIRST_FAIL"]).dt.total_seconds() / 60
        ).round(1)
        df["TIME_TO_SUCCESS"] = df["TIME_TO_SUCCESS"].fillna("N/A")

        display_df = df[[
            "GFDID", "CHARITY", "REPCODE", "PROCESSOR", "BIN", "CARD_TYPE", "FUNDING_TYPE", 
            "CARD_LEVEL", "BANK", "FIRST_FAIL", "FIRST_SUCCESS", 
            "RETRY_SUCCESS", "TIME_TO_SUCCESS"
        ]].rename(columns={
            "FIRST_FAIL": "First Decline",
            "FIRST_SUCCESS": "First Success",
            "TIME_TO_SUCCESS": "Time to Success (min)"
        })

        st.markdown("### Donor Explorer Results")
        st.dataframe(display_df, use_container_width=True)

elif selected_tab == "BIN Risk Analyzer":
    st.markdown("""
        <div style='text-align: center; background-color: #e9f7fc; padding: 12px; border-radius: 8px; margin-bottom: 20px;'>
            <div style='font-size: 28px;'>BIN Risk Analyzer</div>
            <div style='font-size: 14px;'><i>View decline rates and card metadata by BIN to identify risky payment sources</i></div>
        </div>
    """, unsafe_allow_html=True)

    # === Inline Filters ===
    col1, col2 = st.columns(2)
    with col1:
        days_back = st.slider("Days of History", min_value=1, max_value=60, value=14)
    with col2:
        min_volume = st.slider("Minimum BIN Volume", min_value=1, max_value=100, value=25)

    query = f"""
        SELECT 
            REPLACE(SUBSTRING(t.BIN, 1, 7), ' ', '') AS BIN_PREFIX,
            COUNT_IF(g.DONOR_STATUS = 'Successful') AS SUCCESSFUL,
            COUNT_IF(g.DONOR_STATUS = 'Payment Failed') AS DECLINED,
            INITCAP(b.CREDITCARDTYPE) AS CARD_TYPE,
            INITCAP(b.FUNDINGTYPE) AS FUNDING_TYPE,
            INITCAP(b.CARDLEVEL) AS CARD_LEVEL,
            INITCAP(b.BANKNAME) AS BANK,
            INITCAP(b.REGION) AS REGION,
            INITCAP(b.SUBREGION) AS SUBREGION
        FROM FIVETRAN_DATABASE.ARMOR_SQL_2012_DBO.LOGGING_BY_GFDID g
        JOIN FIVETRAN_DATABASE.ARMOR_SQL_2012_DBO.LOGGING_BY_TRANSACTION t ON g.GFDID = t.GFDID
        LEFT JOIN FIVETRAN_DATABASE.ARMOR_SQL_2012_DBO.BIN b 
            ON REPLACE(SUBSTRING(t.BIN, 1, 7), ' ', '') = TO_VARCHAR(b.BIN)
        WHERE g.DONOR_ACQ_DTM >= DATEADD(DAY, -{days_back}, CURRENT_DATE)
          AND g.GIFT_TYPE = 'Recurring'
          AND b._FIVETRAN_DELETED = FALSE
        GROUP BY BIN_PREFIX, CARD_TYPE, FUNDING_TYPE, CARD_LEVEL, BANK, REGION, SUBREGION
        HAVING SUCCESSFUL + DECLINED > 0
        ORDER BY DECLINED DESC
    """

    df = session.sql(query).to_pandas()

    if df.empty:
        st.warning("No BIN data found in the selected range.")
    else:
        df["TOTAL"] = df["SUCCESSFUL"] + df["DECLINED"]
        df["DECLINE_RATE"] = (df["DECLINED"] / df["TOTAL"] * 100).round(1)
        df.rename(columns={"BIN_PREFIX": "BIN"}, inplace=True)

        # ✅ Filter for high-volume BINs before sorting or charting
        df = df[df["TOTAL"] >= min_volume].sort_values(by="DECLINED", ascending=False)

        st.markdown("### BIN Decline Rates and Metadata")
        st.dataframe(df[[
            "BIN", "SUCCESSFUL", "DECLINED", "TOTAL", "DECLINE_RATE",
            "CARD_TYPE", "FUNDING_TYPE", "CARD_LEVEL", "BANK", "REGION", "SUBREGION"
        ]], use_container_width=True)

        # === Chart of Top Declining BINs
        st.markdown("### 📊 Top Declining BINs")
        if not df.empty and df["DECLINED"].max() > 0:
            top_bins = df.nlargest(10, "DECLINED")
            import altair as alt
            chart = alt.Chart(top_bins).mark_bar().encode(
                x=alt.X("BIN:N", sort="-y", title="BIN"),
                y=alt.Y("DECLINED:Q", title="Declined Count"),
                color=alt.value("#dc3545"),
                tooltip=["BIN", "SUCCESSFUL", "DECLINED", "DECLINE_RATE", "BANK", "CARD_TYPE"]
            ).properties(height=400)

            st.altair_chart(chart, use_container_width=True)
        else:
            st.info("No BINs with declines found in the selected date and volume range.")
