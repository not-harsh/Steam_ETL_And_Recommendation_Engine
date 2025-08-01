# Steam_ETL_And_Recommendation_Engine
ETL pipeline for Steam game data using Apache Airflow, Google Cloud (BigQuery, GCS), and SteamSpy API, with a Streamlit-based recommendation engine and Looker Studio dashboard for game analytics and personalized recommendations.


<img width="2879" height="1799" alt="image" src="https://github.com/user-attachments/assets/b820e54a-cab8-4191-87f6-ab8a9835ecb3" />





## Project Overview

This project implements an **ETL (Extract, Transform, Load) pipeline** to collect, process, and store Steam game data, followed by a **recommendation engine** to provide game recommendations based on genres and tags. The pipeline leverages **Apache Airflow** for orchestration, **Google Cloud Platform (GCP)** services (Google Cloud Storage and BigQuery) for data storage, and **Streamlit** for an interactive user interface. The goal is to extract game data from the SteamSpy API, maintain a historical record using Slowly Changing Dimension (SCD) Type 2, and enable users to find similar games via a recommendation system.

### Objectives
- **Data Collection**: Extract comprehensive game data from the SteamSpy API, including game metadata, genres, tags, ownership, and pricing.
- **Data Storage**: Store raw and cleaned data in Google Cloud Storage (GCS) and maintain a historical dataset in BigQuery with SCD Type 2 for tracking changes over time.
- **Recommendation Engine**: Provide personalized game recommendations based on text-based similarity of genres and tags using TF-IDF and cosine similarity.
- **Visualization**: Embed an interactive Looker Studio dashboard for data analysis and insights.
- **Automation**: Automate the ETL process using Airflow for daily updates.

### Why This Approach?
- **SteamSpy API**: Chosen for its rich dataset on Steam games, including user reviews, ownership estimates, and tags, which are ideal for both analytics and recommendations.
- **SCD Type 2**: Used to maintain a historical record of game metadata changes (e.g., price, reviews) in BigQuery, enabling temporal analysis.
- **Airflow**: Selected for its robust scheduling and task dependency management, ensuring reliable and scalable ETL workflows.
- **GCP**: Leverages GCS for cost-effective storage and BigQuery for efficient querying and scalability.
- **Streamlit**: Provides a simple, interactive UI for the recommendation engine, making it accessible to non-technical users.
- **TF-IDF and Cosine Similarity**: Chosen for the recommendation engine due to their effectiveness in text-based similarity tasks, balancing simplicity and performance.

## Project Structure

The project consists of three main Python scripts:

1. **`steam_dag.py`**: Defines the Airflow DAG for orchestrating the ETL pipeline.
2. **`steam_etl.py`**: Contains the logic for extracting, transforming, and uploading Steam game data to GCS.
3. **`recommendation_engine.py`**: Implements the Streamlit-based recommendation engine and Looker Studio dashboard integration.

### File Descriptions

#### 1. `steam_dag.py`
- **Purpose**: Defines an Airflow DAG (`steam_scd_pipeline`) that orchestrates the ETL process, including creating BigQuery tables, extracting data, loading it into BigQuery, and applying SCD Type 2 logic.
- **Key Components**:
  - **DAG Configuration**: Runs daily, starting from July 19, 2025, with retries and no catchup.
  - **Tasks**:
    - `create_staging_table`: Creates a BigQuery staging table (`cleaned_steam_games_staging`) for temporary data storage.
    - `create_cleaned_table`: Creates the main BigQuery table (`cleaned_steam_games`) with SCD Type 2 fields (`valid_from`, `valid_to`, `is_active`).
    - `update_cleaned_table_clustering`: Applies clustering on `appid` for query optimization.
    - `extract_data_task`: Calls the `extract_and_upload` function from `steam_etl.py` to fetch and upload data to GCS.
    - `load_cleaned_task`: Loads cleaned data from GCS to the BigQuery staging table.
    - `apply_scd_merge`: Merges staging data into the main table, implementing SCD Type 2 logic to track changes.
- **Why SCD Type 2?**:
  - Maintains historical data for analysis (e.g., tracking price changes or review updates).
  - Allows querying the state of a game at any point in time.
  - Uses `valid_from`, `valid_to`, and `is_active` to manage record versions.
- **Why Clustering?**:
  - Clustering on `appid` optimizes queries filtering by game ID, common in analytical workloads.
- **Dependencies**: Tasks are chained to ensure sequential execution: table creation → clustering → data extraction → loading → SCD merge.

#### 2. `steam_etl.py`
- **Purpose**: Handles the extraction of game data from the SteamSpy API, transformation into a structured format, and uploading to GCS.
- **Key Functions**:
  - `get_steamspy_api_session`: Configures a `requests` session with retries to handle API rate limits and errors.
  - `fetch_app_list`: Retrieves the list of Steam app IDs from the Steam API.
  - `fetch_app_details`: Fetches detailed game data for a specific `appid` from SteamSpy.
  - `validate_app_ids`: Validates a sample of app IDs to estimate the proportion of valid IDs, reducing unnecessary API calls.
  - `extract_and_upload`: Main ETL function that orchestrates data extraction, transformation, and upload to GCS.
- **Transformation Logic**:
  - Converts genres, tags, and languages into lists for BigQuery’s `REPEATED` fields.
  - Normalizes prices (`price`, `initialprice`) from cents to dollars and handles "Free to Play" cases.
  - Parses ownership ranges (`min_owners`, `max_owners`) from strings.
  - Adds `load_date` for temporal tracking.
- **Why This Design?**:
  - **API Resilience**: Retries and rate-limiting (2-second delay) prevent API failures.
  - **Validation**: Sampling app IDs reduces API calls for invalid IDs, improving efficiency.
  - **ThreadPoolExecutor**: Parallelizes API requests in batches for faster processing.
  - **GCS Storage**: Stores raw and cleaned data separately for debugging and flexibility.
  - **Incremental Updates**: For non-initial loads, fetches only new or sampled existing app IDs to minimize redundant processing.

#### 3. `recommendation_engine.py`
- **Purpose**: Provides a Streamlit-based UI for game recommendations based on genre and tag similarity, with an embedded Looker Studio dashboard.
- **Key Components**:
  - **Data Loading**: Fetches data from BigQuery (`cleaned_steam_games`) or a cached pickle file to reduce query costs.
  - **Preprocessing**: Combines `genre` and `tags` into `combined_features` and applies text cleaning (lowercase, remove punctuation).
  - **Recommendation Logic**:
    - Uses `TfidfVectorizer` to convert text features into a TF-IDF matrix.
    - Computes `cosine_similarity` to measure similarity between games.
    - `get_recommendations`: Returns the top N similar games for a given game name.
  - **Streamlit UI**:
    - Dropdown to select a game.
    - Button to display recommendations in a formatted table.
    - Button to embed a Looker Studio dashboard for analytics.
- **Why This Approach?**:
  - **TF-IDF**: Effective for text-based similarity, capturing the importance of genres and tags.
  - **Cosine Similarity**: Robust for high-dimensional sparse data, common in text analysis.
  - **Caching**: Reduces BigQuery costs and improves UI responsiveness.
  - **Streamlit**: Simplifies deployment of an interactive UI without complex web development.
  - **Looker Studio**: Provides a pre-built, customizable dashboard for data visualization.

## Setup Instructions

### Prerequisites
- **Google Cloud Platform**:
  - A GCP project (`stellar-river-464405-k3`) with BigQuery and GCS enabled.
  - A GCS bucket (`us-central1-composer-dev-33ed7046-bucket`) for storing raw and cleaned data.
  - A service account with permissions for BigQuery and GCS.
  - A BigQuery dataset (`steam_data`) for storing tables.
- **Apache Airflow**:
  - Airflow instance (e.g., Cloud Composer) with the `google-cloud` package installed.
  - GCP connection (`google_cloud_default`) configured in Airflow.
- **Python Environment**:
  - Python 3.8+.
  - Required packages: `requests`, `google-cloud-storage`, `google-cloud-bigquery`, `pandas`, `sklearn`, `streamlit`, `plotly`, `seaborn`, `matplotlib`, `streamlit-components`.
- **SteamSpy API**: No API key required, but rate limits apply (handled in code).
- **Looker Studio**: Access to the dashboard at the specified URL.

### Installation
1. **Clone the Repository**:
   ```bash
   git clone <repository_url>
   cd <repository_directory>
   ```

2. **Install Dependencies**:
   ```bash
   pip install -r requirements.txt
   ```
   Create a `requirements.txt` with:
   ```
   requests
   google-cloud-storage
   google-cloud-bigquery
   pandas
   scikit-learn
   streamlit
   plotly
   seaborn
   matplotlib
   streamlit-components
   ```

3. **Configure Airflow**:
   - Copy `steam_dag.py` and `steam_etl.py` to the Airflow DAGs folder (e.g., `~/airflow/dags`).
   - Set up the `google_cloud_default` connection in Airflow with your GCP credentials.
   - Ensure the GCS bucket and BigQuery dataset exist.

4. **Run the Recommendation Engine**:
   ```bash
   streamlit run recommendation_engine.py
   ```

5. **Access Looker Studio Dashboard**:
   - Ensure access to the dashboard URL: `https://lookerstudio.google.com/embed/reporting/9718b5a1-7e85-48cf-abfe-78d10b499953/page/5SQSF`.

### Running the Pipeline
1. **Trigger the Airflow DAG**:
   - In Airflow UI, enable the `steam_scd_pipeline` DAG.
   - Run manually or wait for the daily schedule.
   - For initial load, pass `{"is_initial_load": true}` in the DAG run configuration to fetch all app IDs.
   - Optionally, set `max_apps` in the DAG run configuration to limit the number of apps processed.

2. **Monitor Execution**:
   - Check Airflow logs for task status.
   - Verify data in GCS (`raw/` and `processed/` folders) and BigQuery (`steam_data.cleaned_steam_games_staging` and `cleaned_steam_games`).

3. **Use the Recommendation Engine**:
   - Access the Streamlit app (default: `http://localhost:8501`).
   - Select a game from the dropdown and click "Get Recommendations" to view similar games.
   - Click "Show Data Analysis" to view the Looker Studio dashboard.

## Why This Implementation?

### ETL Pipeline (steam_dag.py, steam_etl.py)
- **Airflow**: Chosen for its ability to manage complex workflows, handle retries, and integrate with GCP.
- **BigQuery**: Selected for its scalability, support for `REPEATED` fields (genres, tags, languages), and partitioning/clustering for performance.
- **SCD Type 2**: Implemented to track changes in game metadata, enabling historical analysis (e.g., how a game’s price or reviews evolve).
- **GCS**: Used for intermediate storage due to its cost-effectiveness and integration with BigQuery’s external data loading.
- **Validation and Sampling**: Reduces API calls by validating app IDs and sampling existing IDs for incremental updates, optimizing cost and performance.
- **Batching and Parallelization**: Improves throughput by processing app IDs in batches with `ThreadPoolExecutor`.

### Recommendation Engine (recommendation_engine.py)
- **TF-IDF and Cosine Similarity**: Simple yet effective for text-based recommendations, avoiding the need for complex machine learning models.
- **Streamlit**: Enables rapid development of an interactive UI, suitable for demo purposes.
- **Caching**: Reduces BigQuery query costs and improves UI performance.
- **Looker Studio**: Provides a professional-grade visualization tool without additional coding.

## Potential Improvements
- **ETL Pipeline**:
  - Add error handling for specific API failure cases (e.g., rate limit exceeded).
  - Implement partitioning on `load_date` in the staging table for better query performance.
  - Add data quality checks (e.g., null value detection) before loading to BigQuery.
- **Recommendation Engine**:
  - Incorporate additional features (e.g., user reviews, playtime) for more accurate recommendations.
  - Allow filtering recommendations by price, ownership, or other criteria.
  - Optimize TF-IDF by tuning `max_features` or using word embeddings (e.g., BERT) for better text representation.
- **Scalability**:
  - Use Airflow’s `SubDagOperator` for parallel processing of large app ID batches.
  - Implement incremental caching in the ETL process to avoid reprocessing unchanged data.
- **Monitoring**:
  - Add Airflow sensors to monitor API availability.
  - Integrate logging with GCP Cloud Monitoring for better observability.
