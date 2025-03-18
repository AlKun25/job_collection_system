FROM apache/airflow:2.10.5-python3.10

USER root

# Install system dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    git \
    firefox-esr \
    wget \
    unzip \
    xvfb \
    libx11-xcb1 \
    libdbus-glib-1-2 \
    libxt6 \
    libgtk-3-0 \
    libasound2 \
    libxcomposite1 \
    libxdamage1 \
    libcanberra-gtk3-module \
    xauth && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Install Firefox GeckoDriver
RUN wget -q "https://github.com/mozilla/geckodriver/releases/download/v0.31.0/geckodriver-v0.31.0-linux64.tar.gz" -O /tmp/geckodriver.tar.gz && \
    tar -xzf /tmp/geckodriver.tar.gz -C /usr/local/bin && \
    rm /tmp/geckodriver.tar.gz && \
    chmod +x /usr/local/bin/geckodriver

# Verify installation
RUN which firefox && which geckodriver

# Create a wrapper script for Firefox to use with Selenium
RUN echo '#!/bin/bash\nexec xvfb-run -a firefox "$@"' > /usr/local/bin/firefox-xvfb \
    && chmod +x /usr/local/bin/firefox-xvfb

# Create directories and set permissions
RUN mkdir -p /tmp/.X11-unix && chmod 1777 /tmp/.X11-unix

USER airflow

# Install Python packages
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Install additional Airflow packages
RUN pip install --no-cache-dir \
    apache-airflow-providers-postgres \
    apache-airflow-providers-apache-spark

# Set environment variables for Selenium
ENV MOZ_HEADLESS=1
ENV SE_OPTS="--no-sandbox"
ENV PYTHONUNBUFFERED=1
ENV DISPLAY=:99

