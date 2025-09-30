import streamlit as st
import pandas as pd
import json
import io
import toml

from snowflake.snowpark.context import get_active_session
from datetime import datetime, date, timedelta

# It's good practice to get the session once.
session = get_active_session()

# ------------------- Helper Functions -------------------

@st.cache_data(ttl=3600)
def get_user_list():
    """Fetches a list of non-deleted users from Snowflake."""
    return session.sql("SELECT NAME FROM SNOWFLAKE.ACCOUNT_USAGE.USERS WHERE DELETED_ON IS NULL ORDER BY NAME").to_pandas()['NAME'].tolist()


def get_initial_config():
    """
    Retrieves initial configuration information from the Config table.
    """
    if 'initial_config' not in st.session_state:
        try:
            config_all = toml.load("config.toml")
            
            st.session_state.initial_config = config_all["app_settings"]
            st.session_state.data_retention_date = datetime.today().date() + timedelta(days=st.session_state.initial_config["max_retention_days"])
        except Exception as e:
            st.error(f"Error fetching config: {e}")
            st.session_state.initial_config = []
    return st.session_state.initial_config


def get_data_share_name(environment, db_name):
    """Generates the data share name based on the environment and database."""
    data_share_name = st.session_state.initial_config["environments"][selected_environment]['share_name_template']
    
    data_share_name = data_share_name.replace("<DB>", db_name)
    data_share_name = data_share_name.replace("<ENV>", environment)
    return data_share_name

def get_db_list():
    """
    Retrieves a list of all available databases in the Snowflake account.
    Caches the list in session state to avoid repeated calls.
    """
    if 'db_list' not in st.session_state:
        try:
            lst_db = session.sql("SHOW DATABASES").collect()
            db_names_all = [row["name"] for row in lst_db]
            st.session_state.db_list = db_names_all
        except Exception as e:
            st.error(f"Error fetching databases: {e}")
            st.session_state.db_list = []
    return st.session_state.db_list


def fetch_and_cache_table_columns(db_name, full_table_name):
    """
    Fetches all columns and date columns for a given table and caches them.
    """
    if db_name not in st.session_state.cached_table_metadata:
        st.session_state.cached_table_metadata[db_name] = {}
    
    if full_table_name not in st.session_state.cached_table_metadata[db_name]:
        st.session_state.cached_table_metadata[db_name][full_table_name] = {
            'all_columns': [],
            'date_columns': []
        }
        try:
            schema, table = full_table_name.split('.')
            
            sql_all_cols = f"SELECT COLUMN_NAME FROM {db_name}.INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '{schema}' AND TABLE_NAME = '{table}' ORDER BY ORDINAL_POSITION"
            results_all_cols = session.sql(sql_all_cols).collect()
            st.session_state.cached_table_metadata[db_name][full_table_name]['all_columns'] = [row["COLUMN_NAME"] for row in results_all_cols]

            sql_date_cols = f"SELECT COLUMN_NAME FROM {db_name}.INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '{schema}' AND TABLE_NAME = '{table}' AND DATA_TYPE IN ('DATE', 'TIMESTAMP_LTZ', 'TIMESTAMP_NTZ', 'TIMESTAMP_TZ')"
            results_date_cols = session.sql(sql_date_cols).collect()
            st.session_state.cached_table_metadata[db_name][full_table_name]['date_columns'] = [row["COLUMN_NAME"] for row in results_date_cols]
        except Exception as e:
            st.error(f"Error fetching column metadata for {full_table_name}: {e}")


def get_table_list(db_name):
    """
    Retrieves a list of all base tables within a specified database,
    excluding tables matching specific patterns provided in the 'x' list.
    """
    filter_pattern = st.session_state.initial_config["exclude_table_pattern"]

    # Start with the base SQL query
    sql = f"""
        SELECT CONCAT(TABLE_SCHEMA, '.', TABLE_NAME) AS TABLENAME
        FROM {db_name}.INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA <> 'INFORMATION_SCHEMA'
          AND TABLE_TYPE = 'BASE TABLE'
    """

    # Add NOT LIKE clauses for each pattern in 'x'
    for pattern in filter_pattern:
        sql += f" AND TABLE_NAME NOT LIKE '{pattern}'"

    # Add the ORDER BY clause
    sql += " ORDER BY TABLENAME"

    try:
        lst_tables = session.sql(sql).collect()
        return [row["TABLENAME"] for row in lst_tables]
    except Exception as e:
        # Assuming 'st' is a Streamlit object or similar for error reporting
        # If not, you might want to replace st.error with a print statement or logging
        print(f"Error fetching tables for {db_name}: {e}")
        return []

def get_columns_cached(db_name, full_table_name, column_type='all'):
    """
    Retrieves columns for a given table from cache. Fetches if not present.
    """
    if db_name not in st.session_state.cached_table_metadata or full_table_name not in st.session_state.cached_table_metadata[db_name]:
        fetch_and_cache_table_columns(db_name, full_table_name)
    
    key = 'all_columns' if column_type == 'all' else 'date_columns'
    return st.session_state.cached_table_metadata.get(db_name, {}).get(full_table_name, {}).get(key, [])


def create_data_share(share_name, environment, final_filters):
    """
    Creates a Snowflake data share with the specified name and grants necessary permissions.
    """
    try:
        share_sql = f'CREATE OR REPLACE SHARE {share_name} COMMENT = \'{final_filters}\''
        session.sql(share_sql).collect()

        db_name = st.session_state.source_db_name
        session.sql(f"GRANT USAGE ON DATABASE {db_name} TO SHARE {share_name}").collect()
        
        unique_schemas = {table.split('.')[0] for table in st.session_state.selected_tables}
        for schema in unique_schemas:
            session.sql(f"GRANT USAGE ON SCHEMA {db_name}.{schema} TO SHARE {share_name}").collect()

        for table in st.session_state.selected_tables:
            session.sql(f"GRANT SELECT ON TABLE {db_name}.{table} TO SHARE {share_name}").collect()

        reference_usage_database = st.session_state.initial_config['environments'][selected_environment]['reference_database'].strip()
        if reference_usage_database != "":
            session.sql(f"GRANT REFERENCE_USAGE ON DATABASE {reference_usage_database} TO SHARE {share_name}").collect()

        organisation = session.sql("SELECT CURRENT_ORGANIZATION_NAME()").collect()[0][0]
        target_account = st.session_state.initial_config['environments'][selected_environment]['account_name']
        session.sql(f"ALTER SHARE {share_name} ADD ACCOUNTS = {organisation}.{target_account}").collect()

        st.success(f"Share '{share_name}' created successfully for account '{target_account}'.")
    except Exception as e:
        st.error(f"Failed to create share: {e}")

# --- JSON Upload and Parsing Logic ---
def process_uploaded_config(uploaded_file):
    """
    Parses an uploaded JSON file and populates the session state.
    """
    if uploaded_file is not None:
        try:
            stringio = io.StringIO(uploaded_file.getvalue().decode("utf-8"))
            config_data = json.load(stringio)

            if "database_name" not in config_data or "tables" not in config_data:
                st.error("Invalid JSON format. It must contain 'database_name' and 'tables' keys.")
                return

            db_name = config_data["database_name"]
            tables_data = config_data["tables"]
            global_filter_date_str = config_data.get("filter_date")
            data_retention_date_str = config_data.get("data_retention_date")

            # --- Populate Session State ---
            reset_app(clear_all=True, rerun=False)
            
            # FIX: Re-initialize cached_table_metadata after it was deleted by reset_app
            st.session_state.cached_table_metadata = {}

            st.session_state.database_selected = True
            st.session_state.source_db_name = db_name
            st.session_state.selected_tables = list(tables_data.keys())
            st.session_state.table_transforms = tables_data
            st.session_state.share_requested_by = config_data.get("share_requested_by")
            st.session_state.share_requested_reason = config_data.get("share_requested_reason")

            st.session_state.data_retention_date = datetime.strptime(data_retention_date_str, '%Y-%m-%d').date()
            if global_filter_date_str:
                st.session_state.global_filter_date = datetime.strptime(global_filter_date_str, '%Y-%m-%d').date()
            else:
                st.session_state.global_filter_date = datetime.today().date()
            
            if st.session_state.selected_tables:
                st.session_state.current_transform_table = st.session_state.selected_tables[0]

            with st.spinner(f"Loading configuration for database '{db_name}'..."):
                st.session_state.available_tables = get_table_list(db_name)
                # for table in st.session_state.selected_tables:
                #      fetch_and_cache_table_columns(db_name, table)

            st.success(f"Successfully loaded configuration for database '{db_name}'.")
            
            st.rerun()

        except json.JSONDecodeError:
            st.error("Invalid JSON file. Please upload a file with a valid JSON structure.")
        except Exception as e:
            st.error(f"An error occurred while processing the file: {e}")

def sync_retention_date_widget():
    st.session_state.data_retention_date = st.session_state.data_retention_date_widget


# --- Session State Initialization ---

def reset_app(clear_all=False, rerun=True):
    """ Resets the session state. """
    keys_to_delete = list(st.session_state.keys())
    keys_to_preserve = ['initial_config', 'db_list']
    keys_to_delete = [k for k in keys_to_delete if k not in keys_to_preserve]

    for key in keys_to_delete:
        del st.session_state[key]
    
    if rerun:
        st.rerun()

# Initialize session state variables
st.session_state.setdefault('database_selected', False)
st.session_state.setdefault('source_db_name', None)
st.session_state.setdefault('available_tables', [])
st.session_state.setdefault('selected_tables', [])
st.session_state.setdefault('table_transforms', {})
st.session_state.setdefault('current_transform_table', None)
st.session_state.setdefault('global_filter_date', datetime.today().date())
st.session_state.setdefault('data_retention_date', datetime.today().date())
st.session_state.setdefault('cached_table_metadata', {})
st.session_state.setdefault('share_requested_by', None)
st.session_state.setdefault('share_requested_reason', "")

# ------------------- UI Layout -------------------

# Using st.experimental_user as requested.
current_user = st.user.email

get_initial_config()


st.image(st.session_state.initial_config["logo_url"], width=100)

st.title("Data Share Configuration")
st.subheader(f"Welcome, {current_user}")

# --- Configuration Upload Section ---
with st.expander("Upload an Existing Configuration", expanded=not st.session_state.database_selected):
    uploaded_file = st.file_uploader(
        "Choose a JSON configuration file",
        type="json",
        key="config_uploader"
    )
    if uploaded_file:
        if st.button("Load Configuration"):
            process_uploaded_config(uploaded_file)


# Show manual configuration steps only if a database isn't already selected
if not st.session_state.database_selected:
    st.markdown("---")
    st.markdown("### Select Source Database")
    db_list = get_db_list()
    if not db_list:
        st.warning("Could not retrieve database list. Please check permissions and connection.")
        st.stop()
        
    d_name_selected = st.selectbox(
        "Available databases",
        options=db_list,
        key="source_db_name_select",
        index=None,
        placeholder= "Select a database..."
    )

    if st.button("Confirm Database Selection", disabled=(not d_name_selected)):
        st.session_state.database_selected = True
        st.session_state.source_db_name = d_name_selected
        with st.spinner(f"Fetching table list from {d_name_selected}..."):
            st.session_state.available_tables = get_table_list(d_name_selected)
        st.rerun()
else:
    st.markdown(f"### Selected Database: `{st.session_state.source_db_name}`")
    d_name_selected = st.session_state.source_db_name
    if st.button("Reset and Start Again", type="secondary"):
        reset_app(clear_all=True)

 
    
# The rest of the UI renders if a database is selected
if st.session_state.database_selected:

    users = get_user_list()
    st.session_state.share_requested_by = st.selectbox("Select Requestor", users, index=None, placeholder="Select a user...")

    st.session_state.share_requested_reason = st.text_input("Reason for data copy", max_chars=250, value=st.session_state.share_requested_reason)

    if st.session_state.initial_config["max_retention_days"] > 0:
        st.date_input(
            "Select **Retention Date** for the dataset on target environment. (Data will be truncated after this date)",
            value=st.session_state.data_retention_date,
            min_value=datetime.today().date(),
            max_value=datetime.today().date() + timedelta(days=st.session_state.initial_config["max_retention_days"]),
            key="data_retention_date_widget", # Use a unique key for the widget itself
            on_change=sync_retention_date_widget
        )
    else:
        st.session_state.data_retention_date = date(9999, 12, 31)
    
    st.markdown("### Select Tables")
    col1, col2 = st.columns(2)

    with col1:
        st.markdown("**Available Tables**")
        available_options = [t for t in st.session_state.available_tables if t not in st.session_state.selected_tables]
        to_select = st.multiselect(
            "Choose tables to add:",
            options=available_options,
            key="current_selection"
        )
        if st.button("Add Selected Tables >>", disabled=not to_select):
            newly_added = []
            for item in to_select:
                if item not in st.session_state.selected_tables:
                    st.session_state.selected_tables.append(item)
                    newly_added.append(item)
                    st.session_state.table_transforms.setdefault(item, {})
            
            if not st.session_state.current_transform_table and newly_added:
                st.session_state.current_transform_table = newly_added[0]
            st.rerun()

    with col2:
        st.markdown("**Selected Tables**")
        to_remove = st.multiselect(
            "Choose tables to remove:",
            options=st.session_state.selected_tables,
            key="selected_to_remove"
        )
        if st.button("<< Remove Selected Tables", disabled=not to_remove):
            for item in to_remove:
                st.session_state.selected_tables.remove(item)
                st.session_state.table_transforms.pop(item, None)
            
            if st.session_state.current_transform_table not in st.session_state.selected_tables:
                st.session_state.current_transform_table = st.session_state.selected_tables[0] if st.session_state.selected_tables else None
            st.rerun()


# Step 3: Configure Table Transformations
if st.session_state.selected_tables:
    st.markdown("---")
    st.markdown("### Configure Table Transformations")

    st.markdown("#### Global Date Filter")
    
    # FIX: Use a callback to reliably sync the widget's state with the app's session state.
    def sync_date_widget():
        st.session_state.global_filter_date = st.session_state.global_filter_date_widget

    st.date_input(
        "Select a date to apply to all date-filtered tables:",
        value=st.session_state.global_filter_date,
        key="global_filter_date_widget", # Use a unique key for the widget itself
        on_change=sync_date_widget
    )

    st.markdown("#### Table-Specific Configuration")
    
    if st.session_state.current_transform_table not in st.session_state.selected_tables:
        st.session_state.current_transform_table = st.session_state.selected_tables[0] if st.session_state.selected_tables else None

    if st.session_state.current_transform_table:
        selected_table_for_transform = st.selectbox(
            "Select a table to configure:",
            options=st.session_state.selected_tables,
            index=st.session_state.selected_tables.index(st.session_state.current_transform_table),
            key="transform_table_selector"
        )
        st.session_state.current_transform_table = selected_table_for_transform
        table = st.session_state.current_transform_table

        st.session_state.table_transforms.setdefault(table, {})
        
        st.markdown("**Date Filtering**")
        date_cols = get_columns_cached(d_name_selected, table, 'date')
        current_filter = st.session_state.table_transforms.get(table, {}).get("filter", {})
        date_filter_enabled = st.checkbox(
            f"Apply global date filter to this table",
            value="filter" in st.session_state.table_transforms.get(table, {}),
            key=f"{table}_enable_date_filter",
            disabled=not date_cols,
            help="If no date columns are found, this option is disabled."
        )

        if date_filter_enabled:
            selected_date_col = current_filter.get("date_column")
            date_col_index = date_cols.index(selected_date_col) if selected_date_col in date_cols else 0
            date_col_selected = st.selectbox(
                f"Select date column for filter on `{table}`",
                options=date_cols,
                index=date_col_index,
                key=f"{table}_date_col_select"
            )
            st.session_state.table_transforms[table]["filter"] = {"date_column": date_col_selected}
        else:
            st.session_state.table_transforms[table].pop("filter", None)

        st.markdown("**Column Masking**")
        all_cols = get_columns_cached(d_name_selected, table, 'all')
        current_masking_list = st.session_state.table_transforms.get(table, {}).get("mask", {}).get("columns", [])
        existing_mask_tags = {mc["mask_column"]: mc.get("masked_tag", "") for mc in current_masking_list}
        pre_selected_mask_cols = list(existing_mask_tags.keys())

        selected_mask_cols = st.multiselect(
            f"Select columns to mask in `{table}`",
            options=all_cols,
            default=pre_selected_mask_cols,
            key=f"{table}_mask_cols_select"
        )

        if selected_mask_cols:
            new_mask_columns_list = []
            for col in selected_mask_cols:
                masked_tag = existing_mask_tags.get(col, "")
                new_mask_columns_list.append({"mask_column": col, "masked_tag": masked_tag})
            st.session_state.table_transforms[table]["mask"] = {"columns": new_mask_columns_list}
        else:
            st.session_state.table_transforms[table].pop("mask", None)

        if st.session_state.table_transforms.get(table):
            st.markdown("**Advanced: Direct JSON Editing**")
            st.info("You can directly edit the `masked_tag` values in the JSON below. Ensure valid JSON format.")
            
            current_table_transform_json = json.dumps(st.session_state.table_transforms[table], indent=4)
            edited_json_str = st.text_area(
                f"JSON for `{table}` transformations",
                value=current_table_transform_json,
                height=200,
                key=f"{table}_transform_json_editor"
            )
            try:
                parsed_json = json.loads(edited_json_str)
                st.session_state.table_transforms[table] = parsed_json
            except json.JSONDecodeError:
                st.error("Invalid JSON format. Please correct it to apply changes.")
        else:
            st.info("No transformations configured for this table yet.")


# Step 4: Show Final JSON Output
if st.session_state.database_selected:
    st.markdown("---")
    st.markdown("### Review and Download Final Configuration")

    final_json_output = {
        "database_name": st.session_state.source_db_name,
        "data_retention_date": str(st.session_state.data_retention_date),
        "share_created_by": current_user.upper(),
        "share_requested_by": st.session_state.share_requested_by,
        "share_requested_reason": st.session_state.share_requested_reason,
        "filter_date": str(st.session_state.global_filter_date),
        "tables": st.session_state.table_transforms
    }

    final_json_string = json.dumps(final_json_output, indent=4)
    st.code(final_json_string, language='json')
    
    st.download_button(
        label="Download Configuration as JSON",
        data=final_json_string,
        file_name=f"datashare_config_{st.session_state.source_db_name}.json",
        mime="application/json",
    )


# Step 5: Create Data Share

if st.session_state.source_db_name and st.session_state.selected_tables:
    st.markdown("---")
    st.markdown("### Create Data Share")

    data_share_certify = st.checkbox(f"I certify that all [PI information]({st.session_state.initial_config['pi_attributes_url']}) is masked before creating data share.")

    if st.session_state.share_requested_by is None:
        st.warning("Please add the requestor name.")
    elif not st.session_state.share_requested_reason.strip():
        st.warning("Please add the data share request reason.")
    elif not data_share_certify:
        st.warning("Please certify PI verification before creating data share.")
    else:
        environment_ids = list(st.session_state.initial_config["environments"].keys())
        
        selected_environment = st.selectbox(
            'Select target environment',
            options=environment_ids
        )
    
        
        if selected_environment:
            data_share_name = get_data_share_name(selected_environment, st.session_state.source_db_name)
            
            st.text_input(
                "Share Name:", 
                value=data_share_name or "Error generating name", 
                disabled=True
            )
            
            if st.button("Create Share", key="create_share_button", type="primary", disabled=(not data_share_name)):
    
                with st.spinner(f"Creating share '{data_share_name}'..."):
                    create_data_share(data_share_name, selected_environment, final_json_string)
import streamlit as st
import pandas as pd
import json
import io
import toml

from snowflake.snowpark.context import get_active_session
from datetime import datetime, date, timedelta

# It's good practice to get the session once.
session = get_active_session()

# ------------------- Helper Functions -------------------

@st.cache_data(ttl=3600)
def get_user_list():
    """Fetches a list of non-deleted users from Snowflake."""
    return session.sql("SELECT NAME FROM SNOWFLAKE.ACCOUNT_USAGE.USERS WHERE DELETED_ON IS NULL ORDER BY NAME").to_pandas()['NAME'].tolist()


def get_initial_config():
    """
    Retrieves initial configuration information from the Config table.
    """
    if 'initial_config' not in st.session_state:
        try:
            config_all = toml.load("config.toml")
            
            st.session_state.initial_config = config_all["app_settings"]
            st.session_state.data_retention_date = datetime.today().date() + timedelta(days=st.session_state.initial_config["max_retention_days"])
        except Exception as e:
            st.error(f"Error fetching config: {e}")
            st.session_state.initial_config = []
    return st.session_state.initial_config


def get_data_share_name(environment, db_name):
    """Generates the data share name based on the environment and database."""
    data_share_name = st.session_state.initial_config["environments"][selected_environment]['share_name_template']
    
    data_share_name = data_share_name.replace("<DB>", db_name)
    data_share_name = data_share_name.replace("<ENV>", environment)
    return data_share_name

def get_db_list():
    """
    Retrieves a list of all available databases in the Snowflake account.
    Caches the list in session state to avoid repeated calls.
    """
    if 'db_list' not in st.session_state:
        try:
            lst_db = session.sql("SHOW DATABASES").collect()
            db_names_all = [row["name"] for row in lst_db]
            st.session_state.db_list = db_names_all
        except Exception as e:
            st.error(f"Error fetching databases: {e}")
            st.session_state.db_list = []
    return st.session_state.db_list


def fetch_and_cache_table_columns(db_name, full_table_name):
    """
    Fetches all columns and date columns for a given table and caches them.
    """
    if db_name not in st.session_state.cached_table_metadata:
        st.session_state.cached_table_metadata[db_name] = {}
    
    if full_table_name not in st.session_state.cached_table_metadata[db_name]:
        st.session_state.cached_table_metadata[db_name][full_table_name] = {
            'all_columns': [],
            'date_columns': []
        }
        try:
            schema, table = full_table_name.split('.')
            
            sql_all_cols = f"SELECT COLUMN_NAME FROM {db_name}.INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '{schema}' AND TABLE_NAME = '{table}' ORDER BY ORDINAL_POSITION"
            results_all_cols = session.sql(sql_all_cols).collect()
            st.session_state.cached_table_metadata[db_name][full_table_name]['all_columns'] = [row["COLUMN_NAME"] for row in results_all_cols]

            sql_date_cols = f"SELECT COLUMN_NAME FROM {db_name}.INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '{schema}' AND TABLE_NAME = '{table}' AND DATA_TYPE IN ('DATE', 'TIMESTAMP_LTZ', 'TIMESTAMP_NTZ', 'TIMESTAMP_TZ')"
            results_date_cols = session.sql(sql_date_cols).collect()
            st.session_state.cached_table_metadata[db_name][full_table_name]['date_columns'] = [row["COLUMN_NAME"] for row in results_date_cols]
        except Exception as e:
            st.error(f"Error fetching column metadata for {full_table_name}: {e}")


def get_table_list(db_name):
    """
    Retrieves a list of all base tables within a specified database,
    excluding tables matching specific patterns provided in the 'x' list.
    """
    filter_pattern = st.session_state.initial_config["exclude_table_pattern"]

    # Start with the base SQL query
    sql = f"""
        SELECT CONCAT(TABLE_SCHEMA, '.', TABLE_NAME) AS TABLENAME
        FROM {db_name}.INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA <> 'INFORMATION_SCHEMA'
          AND TABLE_TYPE = 'BASE TABLE'
    """

    # Add NOT LIKE clauses for each pattern in 'x'
    for pattern in filter_pattern:
        sql += f" AND TABLE_NAME NOT LIKE '{pattern}'"

    # Add the ORDER BY clause
    sql += " ORDER BY TABLENAME"

    try:
        lst_tables = session.sql(sql).collect()
        return [row["TABLENAME"] for row in lst_tables]
    except Exception as e:
        # Assuming 'st' is a Streamlit object or similar for error reporting
        # If not, you might want to replace st.error with a print statement or logging
        print(f"Error fetching tables for {db_name}: {e}")
        return []

def get_columns_cached(db_name, full_table_name, column_type='all'):
    """
    Retrieves columns for a given table from cache. Fetches if not present.
    """
    if db_name not in st.session_state.cached_table_metadata or full_table_name not in st.session_state.cached_table_metadata[db_name]:
        fetch_and_cache_table_columns(db_name, full_table_name)
    
    key = 'all_columns' if column_type == 'all' else 'date_columns'
    return st.session_state.cached_table_metadata.get(db_name, {}).get(full_table_name, {}).get(key, [])


def create_data_share(share_name, environment, final_filters):
    """
    Creates a Snowflake data share with the specified name and grants necessary permissions.
    """
    try:
        share_sql = f'CREATE OR REPLACE SHARE {share_name} COMMENT = \'{final_filters}\''
        session.sql(share_sql).collect()

        db_name = st.session_state.source_db_name
        session.sql(f"GRANT USAGE ON DATABASE {db_name} TO SHARE {share_name}").collect()
        
        unique_schemas = {table.split('.')[0] for table in st.session_state.selected_tables}
        for schema in unique_schemas:
            session.sql(f"GRANT USAGE ON SCHEMA {db_name}.{schema} TO SHARE {share_name}").collect()

        for table in st.session_state.selected_tables:
            session.sql(f"GRANT SELECT ON TABLE {db_name}.{table} TO SHARE {share_name}").collect()

        reference_usage_database = st.session_state.initial_config['environments'][selected_environment]['reference_database'].strip()
        if reference_usage_database != "":
            session.sql(f"GRANT REFERENCE_USAGE ON DATABASE {reference_usage_database} TO SHARE {share_name}").collect()

        organisation = session.sql("SELECT CURRENT_ORGANIZATION_NAME()").collect()[0][0]
        target_account = st.session_state.initial_config['environments'][selected_environment]['account_name']
        session.sql(f"ALTER SHARE {share_name} ADD ACCOUNTS = {organisation}.{target_account}").collect()

        st.success(f"Share '{share_name}' created successfully for account '{target_account}'.")
    except Exception as e:
        st.error(f"Failed to create share: {e}")

# --- JSON Upload and Parsing Logic ---
def process_uploaded_config(uploaded_file):
    """
    Parses an uploaded JSON file and populates the session state.
    """
    if uploaded_file is not None:
        try:
            stringio = io.StringIO(uploaded_file.getvalue().decode("utf-8"))
            config_data = json.load(stringio)

            if "database_name" not in config_data or "tables" not in config_data:
                st.error("Invalid JSON format. It must contain 'database_name' and 'tables' keys.")
                return

            db_name = config_data["database_name"]
            tables_data = config_data["tables"]
            global_filter_date_str = config_data.get("filter_date")
            data_retention_date_str = config_data.get("data_retention_date")

            # --- Populate Session State ---
            reset_app(clear_all=True, rerun=False)
            
            # FIX: Re-initialize cached_table_metadata after it was deleted by reset_app
            st.session_state.cached_table_metadata = {}

            st.session_state.database_selected = True
            st.session_state.source_db_name = db_name
            st.session_state.selected_tables = list(tables_data.keys())
            st.session_state.table_transforms = tables_data
            st.session_state.share_requested_by = config_data.get("share_requested_by")
            st.session_state.share_requested_reason = config_data.get("share_requested_reason")

            st.session_state.data_retention_date = datetime.strptime(data_retention_date_str, '%Y-%m-%d').date()
            if global_filter_date_str:
                st.session_state.global_filter_date = datetime.strptime(global_filter_date_str, '%Y-%m-%d').date()
            else:
                st.session_state.global_filter_date = datetime.today().date()
            
            if st.session_state.selected_tables:
                st.session_state.current_transform_table = st.session_state.selected_tables[0]

            with st.spinner(f"Loading configuration for database '{db_name}'..."):
                st.session_state.available_tables = get_table_list(db_name)
                # for table in st.session_state.selected_tables:
                #      fetch_and_cache_table_columns(db_name, table)

            st.success(f"Successfully loaded configuration for database '{db_name}'.")
            
            st.rerun()

        except json.JSONDecodeError:
            st.error("Invalid JSON file. Please upload a file with a valid JSON structure.")
        except Exception as e:
            st.error(f"An error occurred while processing the file: {e}")

def sync_retention_date_widget():
    st.session_state.data_retention_date = st.session_state.data_retention_date_widget


# --- Session State Initialization ---

def reset_app(clear_all=False, rerun=True):
    """ Resets the session state. """
    keys_to_delete = list(st.session_state.keys())
    keys_to_preserve = ['initial_config', 'db_list']
    keys_to_delete = [k for k in keys_to_delete if k not in keys_to_preserve]

    for key in keys_to_delete:
        del st.session_state[key]
    
    if rerun:
        st.rerun()

# Initialize session state variables
st.session_state.setdefault('database_selected', False)
st.session_state.setdefault('source_db_name', None)
st.session_state.setdefault('available_tables', [])
st.session_state.setdefault('selected_tables', [])
st.session_state.setdefault('table_transforms', {})
st.session_state.setdefault('current_transform_table', None)
st.session_state.setdefault('global_filter_date', datetime.today().date())
st.session_state.setdefault('data_retention_date', datetime.today().date())
st.session_state.setdefault('cached_table_metadata', {})
st.session_state.setdefault('share_requested_by', None)
st.session_state.setdefault('share_requested_reason', "")

# ------------------- UI Layout -------------------

# Using st.experimental_user as requested.
current_user = st.user.email

get_initial_config()


st.image(st.session_state.initial_config["logo_url"], width=100)

st.title("Data Share Configuration")
st.subheader(f"Welcome, {current_user}")

# --- Configuration Upload Section ---
with st.expander("Upload an Existing Configuration", expanded=not st.session_state.database_selected):
    uploaded_file = st.file_uploader(
        "Choose a JSON configuration file",
        type="json",
        key="config_uploader"
    )
    if uploaded_file:
        if st.button("Load Configuration"):
            process_uploaded_config(uploaded_file)


# Show manual configuration steps only if a database isn't already selected
if not st.session_state.database_selected:
    st.markdown("---")
    st.markdown("### Select Source Database")
    db_list = get_db_list()
    if not db_list:
        st.warning("Could not retrieve database list. Please check permissions and connection.")
        st.stop()
        
    d_name_selected = st.selectbox(
        "Available databases",
        options=db_list,
        key="source_db_name_select",
        index=None,
        placeholder= "Select a database..."
    )

    if st.button("Confirm Database Selection", disabled=(not d_name_selected)):
        st.session_state.database_selected = True
        st.session_state.source_db_name = d_name_selected
        with st.spinner(f"Fetching table list from {d_name_selected}..."):
            st.session_state.available_tables = get_table_list(d_name_selected)
        st.rerun()
else:
    st.markdown(f"### Selected Database: `{st.session_state.source_db_name}`")
    d_name_selected = st.session_state.source_db_name
    if st.button("Reset and Start Again", type="secondary"):
        reset_app(clear_all=True)

 
    
# The rest of the UI renders if a database is selected
if st.session_state.database_selected:

    users = get_user_list()
    st.session_state.share_requested_by = st.selectbox("Select Requestor", users, index=None, placeholder="Select a user...")

    st.session_state.share_requested_reason = st.text_input("Reason for data copy", max_chars=250, value=st.session_state.share_requested_reason)

    if st.session_state.initial_config["max_retention_days"] > 0:
        st.date_input(
            "Select **Retention Date** for the dataset on target environment. (Data will be truncated after this date)",
            value=st.session_state.data_retention_date,
            min_value=datetime.today().date(),
            max_value=datetime.today().date() + timedelta(days=st.session_state.initial_config["max_retention_days"]),
            key="data_retention_date_widget", # Use a unique key for the widget itself
            on_change=sync_retention_date_widget
        )
    else:
        st.session_state.data_retention_date = date(9999, 12, 31)
    
    st.markdown("### Select Tables")
    col1, col2 = st.columns(2)

    with col1:
        st.markdown("**Available Tables**")
        available_options = [t for t in st.session_state.available_tables if t not in st.session_state.selected_tables]
        to_select = st.multiselect(
            "Choose tables to add:",
            options=available_options,
            key="current_selection"
        )
        if st.button("Add Selected Tables >>", disabled=not to_select):
            newly_added = []
            for item in to_select:
                if item not in st.session_state.selected_tables:
                    st.session_state.selected_tables.append(item)
                    newly_added.append(item)
                    st.session_state.table_transforms.setdefault(item, {})
            
            if not st.session_state.current_transform_table and newly_added:
                st.session_state.current_transform_table = newly_added[0]
            st.rerun()

    with col2:
        st.markdown("**Selected Tables**")
        to_remove = st.multiselect(
            "Choose tables to remove:",
            options=st.session_state.selected_tables,
            key="selected_to_remove"
        )
        if st.button("<< Remove Selected Tables", disabled=not to_remove):
            for item in to_remove:
                st.session_state.selected_tables.remove(item)
                st.session_state.table_transforms.pop(item, None)
            
            if st.session_state.current_transform_table not in st.session_state.selected_tables:
                st.session_state.current_transform_table = st.session_state.selected_tables[0] if st.session_state.selected_tables else None
            st.rerun()


# Step 3: Configure Table Transformations
if st.session_state.selected_tables:
    st.markdown("---")
    st.markdown("### Configure Table Transformations")

    st.markdown("#### Global Date Filter")
    
    # FIX: Use a callback to reliably sync the widget's state with the app's session state.
    def sync_date_widget():
        st.session_state.global_filter_date = st.session_state.global_filter_date_widget

    st.date_input(
        "Select a date to apply to all date-filtered tables:",
        value=st.session_state.global_filter_date,
        key="global_filter_date_widget", # Use a unique key for the widget itself
        on_change=sync_date_widget
    )

    st.markdown("#### Table-Specific Configuration")
    
    if st.session_state.current_transform_table not in st.session_state.selected_tables:
        st.session_state.current_transform_table = st.session_state.selected_tables[0] if st.session_state.selected_tables else None

    if st.session_state.current_transform_table:
        selected_table_for_transform = st.selectbox(
            "Select a table to configure:",
            options=st.session_state.selected_tables,
            index=st.session_state.selected_tables.index(st.session_state.current_transform_table),
            key="transform_table_selector"
        )
        st.session_state.current_transform_table = selected_table_for_transform
        table = st.session_state.current_transform_table

        st.session_state.table_transforms.setdefault(table, {})
        
        st.markdown("**Date Filtering**")
        date_cols = get_columns_cached(d_name_selected, table, 'date')
        current_filter = st.session_state.table_transforms.get(table, {}).get("filter", {})
        date_filter_enabled = st.checkbox(
            f"Apply global date filter to this table",
            value="filter" in st.session_state.table_transforms.get(table, {}),
            key=f"{table}_enable_date_filter",
            disabled=not date_cols,
            help="If no date columns are found, this option is disabled."
        )

        if date_filter_enabled:
            selected_date_col = current_filter.get("date_column")
            date_col_index = date_cols.index(selected_date_col) if selected_date_col in date_cols else 0
            date_col_selected = st.selectbox(
                f"Select date column for filter on `{table}`",
                options=date_cols,
                index=date_col_index,
                key=f"{table}_date_col_select"
            )
            st.session_state.table_transforms[table]["filter"] = {"date_column": date_col_selected}
        else:
            st.session_state.table_transforms[table].pop("filter", None)

        st.markdown("**Column Masking**")
        all_cols = get_columns_cached(d_name_selected, table, 'all')
        current_masking_list = st.session_state.table_transforms.get(table, {}).get("mask", {}).get("columns", [])
        existing_mask_tags = {mc["mask_column"]: mc.get("masked_tag", "") for mc in current_masking_list}
        pre_selected_mask_cols = list(existing_mask_tags.keys())

        selected_mask_cols = st.multiselect(
            f"Select columns to mask in `{table}`",
            options=all_cols,
            default=pre_selected_mask_cols,
            key=f"{table}_mask_cols_select"
        )

        if selected_mask_cols:
            new_mask_columns_list = []
            for col in selected_mask_cols:
                masked_tag = existing_mask_tags.get(col, "")
                new_mask_columns_list.append({"mask_column": col, "masked_tag": masked_tag})
            st.session_state.table_transforms[table]["mask"] = {"columns": new_mask_columns_list}
        else:
            st.session_state.table_transforms[table].pop("mask", None)

        if st.session_state.table_transforms.get(table):
            st.markdown("**Advanced: Direct JSON Editing**")
            st.info("You can directly edit the `masked_tag` values in the JSON below. Ensure valid JSON format.")
            
            current_table_transform_json = json.dumps(st.session_state.table_transforms[table], indent=4)
            edited_json_str = st.text_area(
                f"JSON for `{table}` transformations",
                value=current_table_transform_json,
                height=200,
                key=f"{table}_transform_json_editor"
            )
            try:
                parsed_json = json.loads(edited_json_str)
                st.session_state.table_transforms[table] = parsed_json
            except json.JSONDecodeError:
                st.error("Invalid JSON format. Please correct it to apply changes.")
        else:
            st.info("No transformations configured for this table yet.")


# Step 4: Show Final JSON Output
if st.session_state.database_selected:
    st.markdown("---")
    st.markdown("### Review and Download Final Configuration")

    final_json_output = {
        "database_name": st.session_state.source_db_name,
        "data_retention_date": str(st.session_state.data_retention_date),
        "share_created_by": current_user.upper(),
        "share_requested_by": st.session_state.share_requested_by,
        "share_requested_reason": st.session_state.share_requested_reason,
        "filter_date": str(st.session_state.global_filter_date),
        "tables": st.session_state.table_transforms
    }

    final_json_string = json.dumps(final_json_output, indent=4)
    st.code(final_json_string, language='json')
    
    st.download_button(
        label="Download Configuration as JSON",
        data=final_json_string,
        file_name=f"datashare_config_{st.session_state.source_db_name}.json",
        mime="application/json",
    )


# Step 5: Create Data Share

if st.session_state.source_db_name and st.session_state.selected_tables:
    st.markdown("---")
    st.markdown("### Create Data Share")

    data_share_certify = st.checkbox(f"I certify that all [PI information]({st.session_state.initial_config['pi_attributes_url']}) is masked before creating data share.")

    if st.session_state.share_requested_by is None:
        st.warning("Please add the requestor name.")
    elif not st.session_state.share_requested_reason.strip():
        st.warning("Please add the data share request reason.")
    elif not data_share_certify:
        st.warning("Please certify PI verification before creating data share.")
    else:
        environment_ids = list(st.session_state.initial_config["environments"].keys())
        
        selected_environment = st.selectbox(
            'Select target environment',
            options=environment_ids
        )
    
        
        if selected_environment:
            data_share_name = get_data_share_name(selected_environment, st.session_state.source_db_name)
            
            st.text_input(
                "Share Name:", 
                value=data_share_name or "Error generating name", 
                disabled=True
            )
            
            if st.button("Create Share", key="create_share_button", type="primary", disabled=(not data_share_name)):
    
                with st.spinner(f"Creating share '{data_share_name}'..."):
                    create_data_share(data_share_name, selected_environment, final_json_string)
