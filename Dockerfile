FROM python:3.9-slim

# Set working directory
WORKDIR /usr/app/dbt

# Install system dependencies
RUN apt-get update && apt-get install -y \
    git \
    && rm -rf /var/lib/apt/lists/*

# Install dbt and other Python packages
COPY Pipfile Pipfile.lock ./
RUN pip install pipenv && \
    pipenv install --deploy --system

# Install additional required packages
RUN pip install networkx dbt-core dbt-postgres dbt-mysql

# Copy dbt project files (including pre-installed packages)
COPY . .

# Set environment variables
ENV DBT_PROFILES_DIR=/usr/app/dbt
ENV DBT_PROJECT_DIR=/usr/app/dbt

# Default command
CMD ["dbt", "run"] 