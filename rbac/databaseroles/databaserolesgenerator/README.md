# Snowflake Database Role Generator

This repository contains a **Snowflake Notebook** designed to automate the creation and management of database roles within your Snowflake environment. It leverages **Python and Snowpark** to dynamically generate SQL DDL (Data Definition Language) statements based on a configurable `config.yaml` file, simplifying the process of implementing a robust **role-based access control (RBAC)** model.

---

## Features

* **Automated Role Creation:** Generates Snowflake database roles for various access levels (**Read-Only, Write, Full, Owner, Share**) at both the database and schema levels.
* **Configurable Environment:** Utilizes a `config.yaml` file to define **environment-specific settings**, database names, and schema exclusion/inclusion.
* **Flexible Schema Management:** Supports different script modes:
    * **`all_schema`**: Applies roles to all schemas within a database, with an option to exclude specific schemas.
    * **`single_schema`**: Focuses role creation on a single, specified schema.
    * **`no_schema`**: Creates database-level roles without schema-specific roles.
* **Managed Access Control:** Optionally enables **managed access** for schemas to enforce stricter privilege inheritance.
* **Snowpark Integration:** Seamlessly interacts with Snowflake using the **Snowpark library** for efficient data operations and DDL execution.

---

## Getting Started

### Prerequisites

* A **Snowflake account** with appropriate privileges to create roles and databases (e.g., `ACCOUNTADMIN` or a role with similar permissions initially).
* Access to **Snowflake Notebooks** or a similar environment capable of running Python with Snowpark.
* The following Python packages: `streamlit`, `pandas`, `pyyaml`. These are typically pre-installed or easily installable within a Snowflake Notebook environment.

### Setup

1.  **Clone this repository** to your local machine or upload the `database_role_generator.ipynb` notebook and `config.yaml` file directly into your Snowflake environment.

2.  **Configure `config.yaml`**:
    Open the `config.yaml` file and update the settings according to your Snowflake environment and desired role generation strategy.

    ```yaml
    app_settings:
      environment: "DEV" # e.g., DEV, QA, PROD
      database_name: "YOUR_DATABASE_NAME" # The Snowflake database to manage roles for

    script_mode:
      type: "all_schema" # Choose from "all_schema", "single_schema", "no_schema"

      # --- Settings for "all_schema" mode ---
      all_schema:
        schemas_to_exclude: # List schemas to exclude from role generation
          - "INFORMATION_SCHEMA"
          - "SNOWFLAKE"
        create_managed_schema: False # Set to True to enable managed access on schemas

      # --- Settings for "single_schema" mode ---
      single_schema:
        schema_name: "YOUR_TARGET_SCHEMA" # Specify the single schema name
    ```

    * **`environment`**: A prefix for your generated roles (e.g., `DEV_AR_ALLDB_RO`).
    * **`database_name`**: The name of the Snowflake database where roles will be created.
    * **`script_mode.type`**: Select one of the three modes:
        * `all_schema`: Roles are generated for all schemas in `database_name` except those listed in `schemas_to_exclude`. Set `create_managed_schema` to `True` if you want to enable managed access on these schemas.
        * `single_schema`: Roles are generated only for the `schema_name` specified.
        * `no_schema`: Only database-level roles are generated.

3.  **Open the Snowflake Notebook**:
    Upload or open the `database_role_generator.ipynb` file in your Snowflake Notebooks interface.

---

## Usage

1.  **Review the Notebook Cells**:
    The notebook is structured into logical cells. You can execute them sequentially or individually after reviewing their purpose.
    * **`Imports`**: Imports necessary Python libraries.
    * **`ScriptTypes`**: Defines constants for script modes.
    * **`ReadConfig`**: Reads and parses your `config.yaml` file.
    * **`GlobalVar_DBRoleTypes`** and **`GlobalVar_SnowflakeObjects`**: Define constants for role types and Snowflake object types for privilege granting.
    * **`ACCOUNTADMIN`**: Sets the current role to `ACCOUNTADMIN`. This is generally required for initial setup and granting database ownership.
    * **`Gen_DataProductOwner`**, **`Gen_DatabaseAdmin`**, **`Gen_AccessRoles_DB`**: Create foundational organizational and database administration roles.
    * **`Gen_DBOwnership`**: Grants ownership of the database to the `DATABASE_ADMIN` role.
    * **`Use_Role_DBA`** and **`UseDB`**: Switch to the `DATABASE_ADMIN` role and set the current database for subsequent operations.
    * **`Get_Schema_List`**: Retrieves a list of schemas based on your `script_mode` configuration.
    * **`GenDatabaseRoles`**: Creates database-level roles (`DB_<DATABASE_NAME>_RO`, `DB_<DATABASE_NAME>_RW`, `DB_<DATABASE_NAME>_ALL`).
    * **`Gen_Ownership`**: Creates schema-level owner roles (`SC_<SCHEMA_NAME>_OWNER`) and grants ownership of schemas.
    * **`Gen_ReadOnly`**: Generates and grants read-only privileges on schemas/database to the respective roles.
    * **`Gen_Share`**: Generates and grants select privileges for data sharing, including secure views.
    * **`Gen_Write`**: Generates and grants write privileges on schemas/database.
    * **`Gen_All`**: Generates and grants full privileges on schemas/database.
    * **`Gen_ManagedSchema`**: (Conditional) Enables managed access on schemas if `create_managed_schema` is set to `True` in the config.

2.  **Execute the Notebook**:
    Run all cells in the notebook. The script will print the SQL statements it's generating and executing.

    **Important Considerations:**
    * The `EXECUTE_SCRIPT` variable in the `ReadConfig` cell controls whether the generated SQL DDL statements are actually executed against your Snowflake account. It's highly recommended to set `EXECUTE_SCRIPT = False` initially to review the generated SQL before applying it.
    * Ensure the role you are running the notebook with has the necessary permissions to create roles and grant privileges. For initial setup, `ACCOUNTADMIN` is often required. The notebook attempts to switch to `USERADMIN` and then to the generated `_DATABASE_ADMIN` role for subsequent operations.

---

## Role Naming Conventions

The notebook follows a clear naming convention for the generated roles:

* **Organizational/Access Roles:**
    * `<ENV>_ORG_DPOWNER` (Data Product Owner)
    * `<ENV>_DATABASE_ADMIN`
    * `<ENV>_AR_ALLDB_RO` (Access Role All Database Read-Only)
    * `<ENV>_AR_ALLDB_RW` (Access Role All Database Read-Write)
    * `<ENV>_AR_ALLDB_ALL` (Access Role All Database Full Access)
    * `<ENV>_AR_DBT_ETL` (Access Role for DBT ETL processes)

* **Database Roles:**
    * `DB_<DATABASE_NAME>_RO` (Database Read-Only)
    * `DB_<DATABASE_NAME>_RW` (Database Read-Write)
    * `DB_<DATABASE_NAME>_ALL` (Database Full Access)

* **Schema Roles:**
    * `SC_<SCHEMA_NAME>_OWNER`
    * `SC_<SCHEMA_NAME>_RO` (Schema Read-Only)
    * `SC_<SCHEMA_NAME>_RW` (Schema Read-Write)
    * `SC_<SCHEMA_NAME>_ALL` (Schema Full Access)
    * `SC_<SCHEMA_NAME>_SHARE` (Schema Share Access)

---

## Contributing

Feel free to open issues or submit pull requests to improve this notebook.

---

## License

This project is open-source and available under the [MIT License](LICENSE).

---
