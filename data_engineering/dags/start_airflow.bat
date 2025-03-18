@echo off
echo Starting Airflow with Docker...

REM Set the AIRFLOW_UID environment variable
set AIRFLOW_UID=50000

REM Start Airflow using Docker Compose
docker-compose up -d

echo.
echo Airflow is starting up...
echo Webserver will be available at: http://localhost:8081
echo Username: airflow
echo Password: airflow
echo.
echo Press any key to view the container status...
pause

REM Show container status
docker-compose ps

echo.
echo Press any key to exit...
pause
