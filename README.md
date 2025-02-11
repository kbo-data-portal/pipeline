# KBO Data Pipeline

This repository automates the collection and deployment of KBO data using Apache Airflow. It manages the flow of data from collection to visualization in the Data Portal.

## Deployment

This pipeline runs on **Apache Airflow** and is deployed using **Docker Compose**. To set up and run the pipeline, ensure Docker is installed and configured properly.

### Running Locally

To run the pipeline locally using Docker Compose:

1. Clone this repository and initialize submodules:
   ```bash
   git clone --recurse-submodules https://github.com/leewr9/kbo-data-pipeline.git
   cd kbo-data-pipeline
   ```
2. Ensure that your GCP service account key is placed in the `config` folder and renamed to `key.json`:
   ```bash
   mv your-service-account-key.json config/key.json
   ```
3. Start the Airflow services using Docker Compose:
   ```bash
   docker-compose up -d
   ```
4. Access the Airflow web UI at [http://localhost:8080](http://localhost:8080)  
   - Login with **Username:** `admin`, **Password:** `admin`
   
## DAGs Overview

The following DAGs are currently implemented:

- **fetch_kbo_games_daily** - Runs daily at **00:00**, parsing the latest KBO game results.
- **fetch_kbo_players_weekly** - Runs every **Sunday at 00:00**, parsing player records up to the current week.
- **fetch_kbo_schedules_weekly** - Runs every **Sunday at 00:00**, parsing the schedule for the upcoming week.
- **fetch_kbo_historical_data** - Runs every **year on January 1st at 00:00**, parsing the schedule for the upcoming year.

## Data Storage Structure
The collected data is stored in **Google Cloud Storage (GCS)** under the `kbo-data` bucket with the following structure:

- **schedules/**
  - `weekly/` (Upcoming game schedules, weekly basis)
  - `historical/` (Past game schedules by year)
- **games/**
  - `daily/` (Game details collected daily)
  - `historical/` (Historical game details by year)
- **players/**
  - `daily/` (Player statistics per game)
  - `weekly/` (Aggregated player statistics per week)
  - `historical/` (Past player statistics by year)

## Data Collection

The parsing modules are managed through the [kbo-data-collector](https://github.com/leewr9/kbo-data-collector) repository, which is included as a **Git submodule** in this project.

## License

This project is licensed under the **MIT License**. See the [LICENSE](LICENSE) file for details.

