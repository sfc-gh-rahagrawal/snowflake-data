# Data Share App

A simple, secure, and innovative way to provide **production-like data** in development environments — without manual copies, governance issues or stale datasets.

## Overview

Developers often need realistic data in Dev for meaningful testing:

- **Performance testing** requires real data volumes.
- **Edge-case validation** needs data that behaves like Production.
- **Automation** ensures Dev data stays fresh without manual refreshes.

Traditional methods — manual copies, sampling, ad-hoc refreshes, or mocked data — all fall short. They’re slow, inconsistent and often violate governance policies.

This project introduces an elegant solution using a **Streamlit app in Production** and a **Notebook in Development** connected through **Secure Data Share**.

## How It Works

### In Production (Streamlit App)
- Users select tables to share from Production.  
- Apply filters (e.g. date, region, product category).  
- Define masking, hashing, or tokenization for sensitive columns.  
- The configuration is stored inside the **Snowflake share’s comment section** — no external config files needed.

### In Development (Notebook)
- The Dev account reads the shared database.  
- Metadata from the share comments is parsed automatically.  
- The Notebook recreates filtered and masked views for testing — matching Production behavior securely.

## Key Benefits

**No manual data movement** – Data stays in Snowflake.  
**Always up-to-date** – Shares reflect latest Production data.  
**Secure by design** – Masking and tokenization applied before sharing.  
**Lightweight metadata exchange** – Uses Snowflake’s native comment field.  
**Simple architecture** – Streamlit on Prod, Notebook on Dev.  

## Tech Stack

- **Secure Data Share** – Secure data sharing and metadata handling  
- **Streamlit** – Interactive UI for selecting and configuring shares  
- **Python / Snowpark** – Query execution, filtering and metadata management  
