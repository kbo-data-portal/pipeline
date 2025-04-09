# KBO Data Pipeline

This repository automates the collection and deployment of KBO data using Apache Airflow. It manages the flow of data from collection to visualization in the Data Portal.

## Deployment
This pipeline runs on **Apache Airflow** and is deployed using **Docker Compose**. To set up and run the pipeline, ensure Docker is installed and configured properly.
     
### Usage
To run the pipeline locally using Docker Compose

1. **Clone this repository and initialize submodules**
    ```bash
    git clone --recurse-submodules https://github.com/leewr9/kbo-data-pipeline.git
    cd kbo-data-pipeline
    ```
    
2. **Ensure that your GCP service account key is placed in the `config` folder and renamed to `key.json`**:
    ```bash
    mv your-service-account-key.json config/key.json
    ```
    
3. **Start the Airflow services using Docker Compose**
    ```bash
    docker-compose up -d
    ```
    
4. **Access the Airflow web UI at [http://localhost:8080](http://localhost:8080)**
    - Login with **Username:** `admin`, **Password:** `admin`

## Data Collection
The parsing modules are managed through the [kbo-data-collector](https://github.com/leewr9/kbo-data-collector) repository, which is included as a **Git submodule** in this project.

## License
This project is licensed under the **MIT License**. See the [LICENSE](LICENSE) file for details.

