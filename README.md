# Brazilian E-Commerce Data Pipeline

## Project Overview

This project is a data pipeline for processing the Brazilian Olist E-Commerce dataset using a suite of data engineering tools. The pipeline extracts data from a MySQL database, stores it in MinIO (object storage), loads it into PostgreSQL, and transforms it using dbt.

## Architecture

The project consists of multiple services managed using Docker Compose:

- **MySQL**: Stores raw e-commerce data.
- **MinIO**: Acts as an object storage to save extracted data.
- **PostgreSQL**: Stores transformed data.
- **Dagster**: Manages ETL workflows.
- **Spark Notebook**: Provides an interactive environment for analysis.
- **ETL Pipeline**: Facilitates data extraction, transformation, and loading.

## Services Configuration

### 1. MySQL
- **Purpose**: Stores the raw Olist dataset.
- **Configuration**:
  - Data is mounted to `./mysql`.
  - Exposed on port `3306`.

### 2. MinIO
- **Purpose**: Used as an object storage system.
- **Access**:
  - Console: [http://localhost:9001](http://localhost:9001)
  - API: [http://localhost:9000](http://localhost:9000)
- **Note**: Buckets and policies are configured automatically using the `mc` container.

### 3. PostgreSQL
- **Purpose**: Stores the cleaned and processed data.
- **Configuration**:
  - Data is mounted to `./postgresql`.
  - Exposed on port `5432`.

### 4. ETL Pipeline
- Extracts data from MySQL.
- Stores extracted data in MinIO.
- Loads data from MinIO into PostgreSQL.
- Uses Dagster for pipeline orchestration.

### 5. Dagster
- **Purpose**: Manages the ETL workflow.
- **Components**:
  - **Dagster Daemon**: Executes scheduled workflows.
  - **Dagit UI**: Accessible at [http://localhost:3001](http://localhost:3001).

### 6. Spark Notebook
- **Purpose**: Provides an interactive Jupyter environment.
- **Configuration**: Exposed on port `8888`.

## Data Pipeline Flow

1. **Extract**: Data is pulled from MySQL.
2. **Store**: Extracted data is saved to MinIO.
3. **Load**: Data is moved from MinIO to PostgreSQL.
4. **Transform**: dbt performs data transformations and generates reports.

## Running the Project

### Accessing the Services

- **MySQL:** `localhost:3306`
- **MinIO Console:** `http://localhost:9001`
- **PostgreSQL:** `localhost:5432`
- **Dagster UI (Dagit):** `http://localhost:3001`
- **Jupyter Notebook (Spark):** `http://localhost:8888`

### Environment Variables

The `.env` file should contain the required credentials:

```plaintext
MYSQL_ROOT_PASSWORD=root
MYSQL_DATABASE=olist
MYSQL_USER=admin
MYSQL_PASSWORD=admin123
POSTGRES_USER=admin
POSTGRES_PASSWORD=admin123
POSTGRES_DB=testing
AWS_ACCESS_KEY_ID=minio
AWS_SECRET_ACCESS_KEY=minio123
```plaintext




## Notes

- Ensure Docker and Docker Compose are installed before running the project.
- The MinIO bucket is created automatically by the `mc` service.
- Adjust paths and environment variables as needed.

## Future Enhancements

- Add monitoring with Prometheus + Grafana.
- Automate dbt transformations in Dagster.
- Implement Kafka for real-time processing.

This setup provides a scalable and efficient data pipeline for e-commerce analytics using modern data engineering tools. 🚀
