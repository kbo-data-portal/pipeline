# KBO Data Portal - Pipeline

This repository automates the collection and deployment of KBO data using Apache Airflow. It manages the flow of data from collection to visualization in the Data Portal.

## Deployment
This pipeline runs on **Apache Airflow** and is deployed using **Docker Compose**. To set up and run the pipeline, ensure Docker is installed and configured properly.
     
### Usage
To run the pipeline locally using Docker Compose

1. **Clone this repository and initialize submodules**
    ```bash
    git clone --recurse-submodules https://github.com/kbo-data-portal/pipeline.git
    cd pipeline
    ```
    Ensure that your GCP service account key is placed in the `config` folder and renamed to `key.json`
   
2. **Start the Airflow services using Docker Compose**
    ```bash
    docker-compose up -d
    ```
3. **Access the Airflow web UI**
    - [http://localhost:8080/](http://localhost:8080/)
    - Login with **Username:** `admin`, **Password:** `admin`

## Submodules
This project uses Git submodules to manage external components

- [`collector`](https://github.com/kbo-data-portal/collector): Handles data collection logic.
- [`api-server`](https://github.com/kbo-data-portal/api-server): Flask-based web application for data visualization.

- To initialize submodules after cloning this repository
    ```bash
    git submodule update --init --remote
    ```

## License
This project is licensed under the **MIT License**. See the [LICENSE](LICENSE) file for details.

