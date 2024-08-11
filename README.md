# PostgreSQL  for Cost of Cup Project


## Table of Contents

1. [Introduction](#introduction)
2. [Prerequisites](#prerequisites)
3. [Database Setup](#database-setup)


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

Now that you have created your database, you need to configure the environment variables that your project will use to connect to this database.

## Configure the Environment Variables

1. **Copy the `.env.example` file** to create your own `.env` file:
   ```bash
   cp .env.example .env

