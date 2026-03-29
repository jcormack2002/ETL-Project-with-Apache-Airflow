# Airflow ETL Pipeline & Web Traffic Analysis

## Project Overview

This project implements an **ETL pipeline using Apache Airflow** to process IIS web server log data.  
The pipeline extracts raw log files, transforms them into a structured fact table, generates supporting dimension data, and loads the cleaned results into a **SQLite database**. The database was then loaded into **Power BI** to uncover traffic patterns and detect potential automated or suspicious activity.

The goal of this project was to demonstrate a complete data workflow, including:

- Data Engineering (ETL pipeline)
- Data modelling (star schema design)
- Data analysis and visualisation (Power BI)
- Insight generation from real-world style log data


## Technologies Used

- Python
- Apache Airflow
- SQLite
- Power BI
- DAX (custom measures and ranking logic)
- Bash
- WSL / Ubuntu
- Git / Github


## ETL Pipeline Architecture

The Airflow DAG **Process_Logs_Data** orchestrates the ETL pipeline

### Pipeline Steps

1. **Copy Log Files to Staging Area**
   - The raw IIS web logs are copied into a staging directory for processing.

2. **Build Fact Table**
   - The log files containing two different formats (14-column and 18-column) are transformed into a unified fact table schema.

3. **Extract Raw IP Addresses**
   - The IP addresses are extracted from the fact table for later processing.

4. **Extract Raw Dates**
   - The date values are extracted from the fact table for later processing.

5. **Generate Unique IP Values**
   - Duplicate IP addresses are removed using a Bash command.

6. **Generate Unique Date Values**
   - Duplicate dates are removed using a Bash command.

7. **Create Location Dimension File**
   - A location dimension dataset is generated from the unique IP addresses and an external API.

8. **Create Date Dimension File**
   - A date dimension dataset is generated from the unique date values.

9. **Prepare Star Schema Data**
   - The fact and dimension tables are organised for modelling.

10. **Load Data into SQLite**
   - The final datasets are loaded into a SQLite database. 


## Data Quality Handling

To improve the robustness and reliability, the pipeline included:

- Removal of blank rows
- Validation of malformed log entries
- Handling inconsistent log formats (14-column and 18-columns)
- Prevention of ETL failures due to bad data
- Updated deprecated Airflow syntax

## Example Airflow DAG Execution

Successful DAG execution in Apache Airflow:

![DAG Execution](dag_run_successful.PNG)

## Database Verification

After the pipeline finished, the data was loaded into an SQLite database.
- The fact table contains **155,566 rows**.
- The date dimension table contains **93 rows**.
- The location dimension table contains **4013 rows**.

![SQLite Verification](sqlite_row_count.PNG)


## Power BI Dashboard 

The SQLite database was connected to **Power BI**, where an interactive dashboard was made.

### Dashboard Features

- Request trends over time
- Requests by day of the week
- Requests by hour of the day
- Traffic distribution by country (map visualisation)
- Top requested resources (URI analysis)
- HTTP status code breakdown
- Top 10 IP address deep dive analysis


### Key Insights

#### General Traffic Patterns

- Total requests: ~156K
- Nearly **80% of requests returned successful (2xx) responses**.
- Traffic peaks at **early in the week**, with **Monday** showing the highest volume.
- Activity is globally distributed, with the highest volume of traffic from:
  - North America
  - United Kingdom
 
#### Suspicious/ Automated Behaviour Detection

The analysis of the **top 10 IP addresses** revealed several patterns which indicated **non-human activity**:

   1. High Number of Requests in a Single Day (Canada)
      - Over **3000 requests in a single day**
      - No other activity outside this period
      - Heavy access to cached and image-based resources
   This strongly indicates **automated** or **scripted behaviour**

   2. High Error and Redirect Rates (United Kingdom)
      - Large proportion of:
        - Redirect responses (3xx)
        - Client errors (4xx)
      - Multiple spikes across different days
   This suggests **repeated failed requests**, which is consistent with:
      - Crawlers
      - Misconfigured clients
      - Automated scripts
     
   3. Continuous 24-Hour Activity (Russia)
      - Requests distributed across all hours of the day
      - No natural drop-off in the activity
      - Includes large request spikes
   This indicates **persistent automated access**, which is not typical user behaviour

## Summary

The key indicators of suspicious activity include:
- Sudden spikes in requests
- Continuous activity ascross all hours
- High proportions of error/redirect status codes
- Repeated access to specific resource types

Majority of the IPs showed:
- Evenly distributed activity
- Predominantly successful responses

These align with the normal user usage patterns.

## Project Outcome

This project demonstrated a **complete data pipeline lifecycle**, from the raw ingestion to the insight generation:
- Build an automated ETL pipeline using Airflow
- Designed and implemented a star schema
- Integrated with Power BI for analysis
- Identified meaningful behavioural patterns in the web traffic

## Future Improvements
There several areas for improvement to enhance the projects analysis and usability:
- Incorporate **user-agent analysis** to explicitly identify the bots
- Containerise the pipeline using Docker
- Deploy to a cloud-based data platform
- Enable automated dashboard refresh in Power BI Service


## Author

Jonathan Cormack










