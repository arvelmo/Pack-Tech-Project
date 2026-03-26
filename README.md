HOW TO: Run pipeline.py
===================================================


# Pack-Tech-Project
PACK Data Challenge

Problem
=======================================================================================
Analyze gold mentors drive better retention and assess booking system reliability

Architecture
=======================================================================================
Local DWH => DuckDB
Data ingestion => Python
Transformation => SQL

Data flow: (Medallion architecture) Raw -> Staging -> Dimensional Model -> Analytics 

Challenges Solved
=======================================================================================
Mixed ID types -> To be normalized to string
Duplicate users -> Deduplicated by latest signup
Event stream -> Reconstructed sessions
Missing session_end -> Default 30 minutes
Idempotence -> No matter #executions, it will not create duplicated data

Data Model
=======================================================================================
dim_users
dim_mentors
fct_sessions

Schema validations
=======================================================================================
Column presence checks
Data type validation (timestamps, dates)
Non-empty file validation
JSON structure validation

Data Quality
=======================================================================================
Checks implemented:
  Invalid durations
  Orphan users
  Invalid mentors

Business Metrics
=======================================================================================
Rebooking Rate = % users booking again in last 30 days

Here is the result of data ingestion and validation. For analytical part, please execute analysis.sql

<img width="974" height="375" alt="image" src="https://github.com/user-attachments/assets/016ba4ec-1005-4e1e-a7fa-6ba2bdaae39e" />



