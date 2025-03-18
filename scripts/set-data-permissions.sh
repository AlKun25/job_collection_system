#!/bin/bash
# Script to set permissions for data directories while excluding postgres folder

# Define colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Define the base data directory
DATA_DIR="/opt/airflow/data"

# Define directories to set permissions for
DIRECTORIES=(
  "job_postings/descriptions"
  "job_postings/keywords"
  "job_boards"
  "resumes"
)

echo -e "${YELLOW}Setting permissions for data directories...${NC}"

# Create directories if they don't exist and set permissions
for dir in "${DIRECTORIES[@]}"; do
  full_path="${DATA_DIR}/${dir}"
  
  # Create directory if it doesn't exist
  if [ ! -d "$full_path" ]; then
    echo -e "Creating directory: ${full_path}"
    mkdir -p "$full_path"
  fi
  
  # Set permissions
  echo -e "Setting permissions for: ${full_path}"
  chmod -R 777 "$full_path"
  
  # Verify permissions were set
  if [ $? -eq 0 ]; then
    echo -e "${GREEN}✓ Successfully set permissions for ${full_path}${NC}"
  else
    echo -e "${RED}✗ Failed to set permissions for ${full_path}${NC}"
  fi
done

# Print summary
echo -e "\n${GREEN}=== Permissions Summary ===${NC}"
for dir in "${DIRECTORIES[@]}"; do
  full_path="${DATA_DIR}/${dir}"
  if [ -d "$full_path" ]; then
    ls -ld "$full_path"
  else
    echo -e "${RED}Directory not found: ${full_path}${NC}"
  fi
done

echo -e "\n${YELLOW}Note: Postgres directory permissions were not modified${NC}"