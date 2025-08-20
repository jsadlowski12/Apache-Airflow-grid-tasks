# Apache Airflow Capstone Project

## Prerequisites

- Apache Airflow version 3.0.0
- Python version 3.12.11
- Docker
- uv

## Getting Started

### Step 1: Virtual Environment Setup

Create a folder on your local machine to setup virtual environment.

Copy the [docker-compose.yaml](docker-compose.yaml) file into the created folder.

Open terminal and navigate to the created folder.

Create virtual environment using the following command:

```bash
uv venv --python 3.12.11
```

### Step 2: Install Necessary Components

Start the virtual environment:

```bash
source .venv/bin/activate
```

Install Apache Airflow and postgres provider:

```bash
uv pip install apache-airflow==3.0.0
uv pip install apache-airflow-providers-postgres
```

### Step 3: Docker Setup

Setup all Airflow components using Docker:

```bash
docker compose up
```

When all containers finish setting up, there will be 4 folders created locally on your machine. Each folder has properly created volumes to connect the Airflow folders on Docker and your local machine.

After the previous steps, copy all Python module files:
- Copy files from repository `dags/` folder to the local `dags/` folder
- Copy files from repository `plugins/` folder to the local `plugins/` folder

### Step 4: HashiCorp Vault Setup

Add Slack token to Vault:

```bash
docker exec -it VAULT_DOCKER_ID sh

vault login ZyrP7NtNw0hbLUqu7N3IlTdO

vault secrets enable -path=airflow -version=2 kv

vault kv put airflow/variables/slack_token value=YOUR_SLACK_TOKEN
```

## How to Use

**[job_dags.py](dags/job_dags.py)** - Consists of dynamically created DAGs for inserting new rows into PostgreSQL database.

**[trigger_dag_without_slack_message.py](dags/trigger_dag_without_slack_message.py)** - Triggers the target DAG from job_dags.py when a `run.txt` file is created in the airflow-scheduler container in the `run` folder.

**[trigger_dag_with_slack_message.py](dags/trigger_dag_with_slack_message.py)** - Same functionality as above but also sends Slack message notifications to target channel. The Slack token is stored in Airflow connections page.

**[trigger_dag_with_slack_message_using_vault.py](dags/trigger_dag_with_slack_message_using_vault.py)** - Same logic as above but the token is stored in HashiCorp Vault.

**[postgres_sql_operators.py](plugins/postgres_sql_operators.py)** - File with PostgreSQLCountRowsOperator defined to query the number of rows in a given table.