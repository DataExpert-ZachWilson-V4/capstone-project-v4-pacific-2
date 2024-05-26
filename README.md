[![Review Assignment Due Date](https://classroom.github.com/assets/deadline-readme-button-24ddc0f5d75046c5622901739e7c5dd533143b0c8e959d652212380cedb1ea36.svg)](https://classroom.github.com/a/1lXY_Wlg)


## **Project Overview**

The goal of this project is to develop an automated, reliable, and scalable data pipeline that integrates data from 2 distinct sources—Chatfuel, and Mailchimp—into a centralized data warehouse. Then the data will be transformed, cleaned, and subjected to quality checks before being used to generate a **daily updated** dashboard on Looker. This pipeline will provide actionable insights for a small media non-profit focused on promoting renewable energy sources in the US.

## Scope of the project

### **1. Data Sources**

- **Chatfuel:** No-code platform that is used for building Facebook Messenger bots to send weekly content and interact with subscribers.
- **Mailchimp:** Email marketing platform to send weekly content and interact with subscribers.

### **2. Use Cases**

- Populate the dashboard for the content department to help them make data-driven decisions aimed at raising subscriber engagement. The content department will have access to key metrics such as user interactions with broadcasts, including clicks on various buttons, reads, and CTA clicks. This data will enable the content team to understand which types of content and broadcast formats are most effective in engaging subscribers.
- Utilize the same data pipeline to generate tables for further analysis and create an additional dashboard for stakeholders with high-level insights and KPIs.

### **3. Tech Stack**

- **Google Cloud Function with Scheduler:** to pull the data from the sources and push raw tables into Google Cloud Storage. These tasks are simple and we need a low-maintenance serverless solution.  (not so sure about it, will probably use Airflow to automate this step too)
- **BigQuery:** Serves as the data warehouse for its ability to handle large datasets and support complex queries efficiently.
- **DBT:** Facilitates data transformation and cleaning within BigQuery.
- **Airflow:** Manages workflow automation to ensure data pulls, transformations, and loads are executed on a daily schedule.
- **Looker:** Provides advanced data visualization capabilities, seamlessly integrating with BigQuery to create dashboards.

### **4. Key Features**

- **Automated Data Ingestion:** Daily data extraction from Chatfuel and weekly from Mailchimp.
- **Data Transformation and Cleaning:** Utilizing DBT to standardize and clean the data.
- **Data Quality Checks:** Implementing data quality checks to ensure data accuracy and reliability.
- **Data Visualization:** Creating interactive dashboards in Looker that update daily, providing insights into subscriber engagement and broadcast performance.

## **Implementation Plan**

1. **Data ingestion (again, this could possibly change when I start the work)**
    - Develop and test Google Cloud Functions with the Scheduler for pulling the data from Chatfuel, and Mailchimp and ingesting the data into Google Cloud Storage.
2. **Data transformation, cleaning, and data quality checks**
    - Create DBT models for data transformation.
    - Implement data cleaning routines.
    - Design and implement data quality checks for each data source.
    - Integrate checks into DBT models.
    - Test and validate DBT models.
3. Automation of the DBT
    - Set up Airflow to automate data load from GCS to BigQuery and run DBT transformations.
4. **Data Visualization**
    - Design a Looker dashboard to answer the content team questions.
    - Write SQL queries based on transformed data to analyze it and connect it to the dashboard.
    - Test and refine the dashboard with end-users.
    

## **Conceptual data model**

![conceptual data modelling (1)](https://github.com/DataExpert-ZachWilson-V4/capstone-project-v4-pacific-2/assets/77248576/95130b38-67ea-4ca7-a206-0bff28b2e1b3)
