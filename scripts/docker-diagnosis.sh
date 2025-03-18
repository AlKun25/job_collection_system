#!/bin/bash
# These commands help diagnose and fix common issues with job_board_discovery DAG in Docker

# 1. Check container status
echo "==== Checking container status ===="
docker ps

# 2. Create necessary directories with proper permissions
echo "==== Creating directories with proper permissions ===="
docker exec airflow-scheduler mkdir -p /opt/airflow/data/job_boards
docker exec airflow-scheduler chmod 777 /opt/airflow/data/job_boards

# 3. Check if geckodriver is installed and working
echo "==== Checking geckodriver installation ===="
docker exec airflow-scheduler which geckodriver
docker exec airflow-scheduler geckodriver --version

# 4. Check Firefox installation
echo "==== Checking Firefox installation ===="
docker exec airflow-scheduler which firefox || echo "Firefox not found"
docker exec airflow-scheduler firefox --version || echo "Firefox version check failed"

# 5. Check PostgreSQL database connection and tables
echo "==== Checking PostgreSQL database ===="
docker exec postgres psql -U airflow -d job_collection -c "\dt"
docker exec postgres psql -U airflow -d job_collection -c "SELECT * FROM information_schema.tables WHERE table_schema = 'public'"

# 6. Check companies table structure
echo "==== Checking companies table structure ===="
docker exec postgres psql -U airflow -d job_collection -c "\d companies"

# 7. Check if there are any records in the companies table
echo "==== Checking companies table records ===="
docker exec postgres psql -U airflow -d job_collection -c "SELECT COUNT(*) FROM companies"

# 8. Check Airflow DAG status
echo "==== Checking Airflow DAGs ===="
docker exec airflow-scheduler airflow dags list
docker exec airflow-scheduler airflow dags list-runs -d job_board_discovery

# 9. Get logs from the most recent DAG run
echo "==== Getting logs from most recent DAG run ===="
LATEST_RUN=$(docker exec airflow-scheduler airflow dags list-runs -d job_board_discovery --output json | grep -m 1 execution_date | awk -F'"' '{print $4}')
if [ ! -z "$LATEST_RUN" ]; then
  echo "Latest run: $LATEST_RUN"
  docker exec airflow-scheduler airflow tasks list job_board_discovery --tree
  
  # Get logs for save_companies_to_db task
  docker exec airflow-scheduler airflow tasks logs -t save_companies_to_db -d job_board_discovery -e "$LATEST_RUN"
else
  echo "No DAG runs found"
fi

# 10. Check Airflow task state
echo "==== Checking Airflow task state ===="
docker exec airflow-scheduler airflow tasks states-for-dag-run -d job_board_discovery -e "$LATEST_RUN"

# 11. Copy and run the debug script
echo "==== Running debug script ===="
cat > docker-debug-script.py << 'EOF'
# Script content from the Docker Debug Script artifact will go here
EOF

docker cp docker-debug-script.py airflow-scheduler:/opt/airflow/docker-debug-script.py
docker exec airflow-scheduler python /opt/airflow/docker-debug-script.py

# 12. Clear the task state and rerun (if needed)
echo "==== Commands to clear task state and rerun (use as needed) ===="
echo "docker exec airflow-scheduler airflow tasks clear -t save_companies_to_db -d job_board_discovery -e $LATEST_RUN -y"
echo "docker exec airflow-scheduler airflow dags backfill -s $LATEST_RUN -e $LATEST_RUN job_board_discovery"

# 13. Show Airflow logs
echo "==== Airflow logs ===="
docker logs airflow-scheduler --tail 100

# 14. Check if job_board_search.py exists and has the right content
echo "==== Checking job_board_search.py ===="
docker exec airflow-scheduler ls -la /opt/airflow/dags/job_utils/job_board_search.py || echo "File not found"
docker exec airflow-scheduler head -n 20 /opt/airflow/dags/job_utils/job_board_search.py || echo "Cannot read file"

# 15. Check disk space
echo "==== Checking disk space ===="
docker exec airflow-scheduler df -h

# 16. Restart containers (if needed)
echo "==== Commands to restart containers (use as needed) ===="
echo "docker-compose restart airflow-scheduler airflow-webserver"