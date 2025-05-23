FROM python:3.9-slim

# Set working directory
WORKDIR /usr/app/dbt

# Install system dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    default-libmysqlclient-dev \
    pkg-config \
    git \
    && rm -rf /var/lib/apt/lists/*

# Install dbt and other Python packages
COPY Pipfile Pipfile.lock ./
RUN pip install pipenv && \
    pipenv install --deploy --system

# Install additional required packages
RUN pip install networkx dbt-core==1.7.9 dbt-postgres==1.7.9 dbt-mysql==1.7.0 python-dotenv

# Copy dbt project files (including pre-installed packages)
COPY . .

# Debug: List contents to verify files
RUN echo "Listing project root:" && \
    ls -la && \
    echo "\nListing macros directory:" && \
    ls -la macros/

# Set environment variables
ENV DBT_PROFILES_DIR=/usr/app/dbt
ENV DBT_PROJECT_DIR=/usr/app/dbt

# Default command
CMD ["dbt", "run"] 