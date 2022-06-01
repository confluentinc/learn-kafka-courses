#!/bin/bash

# Nuke all running Docker containers
docker rm -f $(docker ps -aq) 1>/dev/null 2>&1
docker system prune -f --volumes 1>/dev/null

cd ~/learn-kafka-courses/fund-kafka-security

# Cleanup files
find . \( -type f -name "*.key" -o -name "*.crt" -o -name "*.csr" -o -name "*.pem" -o -name "*.p12" -o -name "*.pkcs12" -o -name "*_creds" \)  -delete

# Restore Module 1 Activity docker-compose.yml
cp ~/learn-kafka-courses/fund-kafka-security/lab-initial-files/mod1-docker-compose.yml ~/learn-kafka-courses/fund-kafka-security/docker-compose.yml
