# PostgreSQL  for Cost of Cup Project


## Table of Contents

1. [Introduction](#introduction)
2. [Prerequisites](#prerequisites)
3. [Database Setup](#database-setup)
4. [Configure Environment Variables](#envinornment-setup)


## Introduction

This project involves setting up a PostgreSQL database,the installation and the use of pgAdmin, and Python.

## Prerequisites

Before starting, ensure you have the following installed:

- PostgreSQL
- pgAdmin
- Python 3.x
- pip (Python package installer)

## Database Setup

1. **Install PostgreSQL and pgAdmin**:
    - Follow the instructions on the [PostgreSQL](https://www.postgresql.org/download/) and [pgAdmin](https://www.pgadmin.org/download/) websites to download and install the software.
  
2. **Start PostgreSQL Server**:
    - Ensure your PostgreSQL server is running. You can start it from your terminal or command prompt:
    ```sh
    pg_ctl -D /usr/local/var/postgres start
    ```

## Create a New Database

1. **Open pgAdmin** and connect to your PostgreSQL server.
2. **Right-click on `Databases`** and select `Create > Database...`.
3. **Enter the database name** (e.g., `hockey_stats`) and click `Save`.


## Configure the Environment Variables

Your project requires various environment variables to be set up for connecting to the PostgreSQL database and accessing data from an S3 bucket. These variables are stored in a `.env` file.

1. Copy the `.env.example` File

Start by copying the provided `.env.example` file to create your own `.env` file:

    ```bash
    cp .env.example .env
    

2. Open the .env file in a text editor and replace the placeholder values with your actual PostgreSQL database information:
```plaintext
    DATABASE_TYPE=postgresql
    DBAPI=psycopg2
    ENDPOINT=localhost       # Replace with your PostgreSQL server address
    USER=your_username       # Replace with your PostgreSQL username
    PASSWORD=your_password   # Replace with your PostgreSQL password
    PORT=5432                # Default PostgreSQL port is 5432
    DATABASE=hockey_stats    # Replace with your created database name
``````


3. AWS S3 Bucket Access Configuration. (Credentials can be obtained from project author)

    Update the following variables in your .env file with your AWS S3 credentials:
   ```plaintext
    AWS_ACCESS_KEY_ID=your_access_key_id     # Replace with your AWS Access Key ID
    AWS_SECRET_ACCESS_KEY=your_secret_key    # Replace with your AWS Secret Access Key
    AWS_S3_BUCKET_NAME=your_bucket_name      # Replace with your S3 bucket name
    AWS_REGION=your_region                   # Replace with the AWS region of your bucket
   ``````
                

3. Save the .env File
   Once you have filled in all the required details, save the .env file.





