# E-commerce Data Processing and Visualization Pipeline


This project automates an end-to-end data pipeline for e-commerce sales data, using Docker, PostgreSQL, Google Cloud Storage, BigQuery, Looker for visualization, and Apache Airflow for workflow orchestration. It handles data from initial loading and cleaning to analysis and visualization, providing a robust framework for data-driven decision-making.

---

#### Problem Statement:

An e-commerce company has access to consolidated sales data from multiple regions PostgreSQL. However, it struggles with analyzing the information in the database effectively due to manual and inconsistent data processing methods. The challenges include:

1. **Delayed Insights**: Manual analysis processes lead to significant delays in generating actionable business reports.
2. **Data Inconsistency**: Despite having consolidated data, discrepancies arise from manual handling and transformations.
3. **Limited Accessibility**: Stakeholders lack real-time access to analytics, hindering rapid decision-making.

To address these issues, the company needs an automated solution that can:

- **Automate Data Flows**: Use Apache Airflow to streamline data transformations and migrations, reducing manual intervention and error rates.
- **Enhance Data Integrity**: Implement robust cleaning and transformation processes to ensure high-quality, reliable data.
- **Provide Real-Time Analytics**: Utilize BigQuery and Looker to deliver dynamic, real-time visual analytics, enabling immediate insights into sales trends and customer behavior.


#### Main Steps:

1. **Data Acquisition**: Import and load the e-commerce transactions dataset from Kaggle (https://www.kaggle.com/datasets/iamsouravbanerjee/customer-shopping-trends-dataset).

2. **Data Cleaning and Transformation**:
   - Sanitize column names to adhere to SQL standards.
   - Apply data cleaning techniques such as duplicate removal and missing value handling.

3. **Database Management**:
   - Utilize PostgreSQL for structured data storage and manage schema dynamically.
   - Automate data uploads to PostgreSQL with detailed logging and error handling.

4. **Data Migration and Storage**:
   - Transfer cleaned data from PostgreSQL to Google Cloud Storage.
   - Load data into BigQuery for advanced querying and analysis preparation.

5. **Visualization with Looker**:
   - Integrate BigQuery with Looker for dynamic data visualization.
   - Create insightful dashboards in Looker to facilitate strategic decision-making.

   ![image](https://github.com/MargaritaTav/E-commerce-DE-project/assets/113361968/877e0221-7f6b-46cf-8716-9bc720f0fd09)


6. **Workflow Automation with Airflow**:
   - Define and schedule data processing tasks using Apache Airflow.
   - Monitor the pipeline's health and automate the execution flow from data ingestion to visualization.

  

#### Docker and Airflow Configuration

The project utilizes Docker to containerize Apache Airflow, ensuring an isolated and consistent environment for managing the data pipeline. Key elements include:

- **Docker Base Image**: Utilizes `apache/airflow:2.7.1` for consistent deployment.
- **Service Configuration**: Sets up services such as webserver, scheduler, and worker with essential services like Redis and PostgreSQL defined in `docker-compose.yml`.

##### Docker Compose Highlights:

- **Simple Configuration**: Environment variables and volumes are configured to support local development and integration with cloud services.
- **Service Health Checks**: Ensures all services are properly started and operational.

#### Airflow DAG Configuration:

- **DAG Definition**: Define the DAG for the ETL process, including parameters for retries, start dates, and notifications.
- **Task Definition and Dependencies**:
  - **Upload CSV to PostgreSQL**: Initial task to upload the raw dataset.
  - **PostgreSQL to GCS**: Transfers data for further cleaning.
  - **Clean and Process Data**: Prepares data for analysis.
  - **GCS to BigQuery**: Loads cleaned data for visualization.
  - **Task Sequence**: Ensures tasks are executed in an orderly manner.

![image](https://github.com/MargaritaTav/E-commerce-DE-project/assets/113361968/f28c542c-93d4-4a76-8e2b-c521e7ad0df1)


---

This repository provides a comprehensive guide for setting up an automated data pipeline from raw data ingestion to actionable insights visualization, integrating multiple technologies to enhance the data management and analytics capabilities of e-commerce businesses.
