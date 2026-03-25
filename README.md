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

Data flow: Raw -> Staging -> Dimensional Model -> Analytics (Medallion architecture)

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

Data Quality
=======================================================================================
Checks implemented:
  Invalid durations
  Orphan users
  Invalid mentors

Business Metrics
=======================================================================================
Rebooking Rate = % users booking again in last 30 days

