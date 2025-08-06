# KBO Data Portal - Pipeline

This repository automates the collection and deployment of KBO data using Apache Airflow.
It manages the entire data flow from collection to visualization in the KBO Data Portal.

## Feature

- Automates data collection and processing using Apache Airflow
- Docker Compose-based deployment for local development
- Integrates with Collector and API Server via Git submodules
- Manages ETL workflows and data flow to visualization layer

## Installation

1. **Clone the repository with submodules**

   ```bash
   git clone --recurse-submodules https://github.com/kbo-data-portal/pipeline.git
   cd pipeline
   ```

   Ensure that your GCP service account key is placed in the `config` folder and renamed to `key.json`.

2. **Initialize submodules (if not cloned with --recurse-submodules)**

   ```bash
   git submodule update --init --remote
   ```

## Usage

1. **Start Airflow services using Docker Compose**

   ```bash
   docker-compose up -d
   ```

2. **Access the Airflow Web UI**

- Open your browser and navigate to http://localhost:8080/
- Login credentials:
  - Username: `admin`
  - Password: `admin`

3. **Update submodules (if needed)**

   ```bash
   git submodule update --remote
   ```

## Submodules

This project uses Git submodules to manage external components:

- [`collector`](https://github.com/kbo-data-portal/collector): Handles data collection logic.
- [`api-server`](https://github.com/kbo-data-portal/api-server): Flask-based web application for data visualization.

## License

This project is licensed under the **MIT License**. See the [LICENSE](LICENSE) file for details.
