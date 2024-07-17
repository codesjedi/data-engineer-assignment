# Squirrel Census ETL Project

This project performs ETL (Extract, Transform, Load) operations on squirrel census data using Apache Spark and PostgreSQL.

## Prerequisites

- Docker
- Docker Compose

## Project Structure

```
project_root/
│
├── data/
│   ├── park-data.csv
│   └── squirrel-data.csv
│
├── etl_script.py
├── Dockerfile
├── docker-compose.yml
└── README.md
```

## Setup and Running

**Clone the repository**

   ```bash
   git clone <repository_url>
   cd <project_directory>
   ```
**Prepare the data**

Place your park-data.csv and squirrel-data.csv files in the data/ directory.
**Build and run the Docker containers**

```bash
docker-compose up --build
```

This command will:
- Build the custom Spark image
- Start the PostgreSQL and Spark services
- Run the ETL script
 
**View the results**

The ETL process will load the data into the PostgreSQL database. You can connect to the database to view the results:

```bash
docker exec -it <postgres_container_name> psql -U enrique -d verusen_squirrels
```
Replace `<postgres_container_name>` with the actual name of your PostgreSQL container.

**Stopping the services**

To stop the services, use:

```commandline
docker-compose down
```