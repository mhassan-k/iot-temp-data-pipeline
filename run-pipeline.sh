#!/bin/bash

# Modern IoT Temperature Data Pipeline - Quick Start Script
echo "🌡️  Modern IoT Temperature Data Pipeline"
echo "========================================"
echo "🚀 dlt + dbt Cosmos + Airflow 2.10"
echo "========================================"

# Function to check if Docker is running
check_docker() {
    if ! docker info > /dev/null 2>&1; then
        echo "❌ Docker is not running. Please start Docker and try again."
        exit 1
    fi
    echo "✅ Docker is running"
}

# Function to check if ports are available
check_ports() {
    if lsof -Pi :5432 -sTCP:LISTEN -t >/dev/null ; then
        echo "⚠️  Port 5432 is already in use. Please stop any PostgreSQL instances."
        echo "   You can run: docker stop \$(docker ps -q --filter \"ancestor=postgres\")"
        read -p "Continue anyway? (y/N): " -n 1 -r
        echo
        if [[ ! $REPLY =~ ^[Yy]$ ]]; then
            exit 1
        fi
    fi
}

# Function to start the pipeline
start_pipeline() {
    echo "🚀 Starting IoT Temperature Data Pipeline..."
    
    # Build and start services
    docker-compose up --build -d
    
    echo "⏳ Waiting for services to be ready..."
    sleep 10
    
    # Check if services are running
    if docker-compose ps | grep -q "Up"; then
        echo "✅ Pipeline services are running!"
        echo ""
        echo "📊 Access Information:"
        echo "   PostgreSQL Database: localhost:5432"
        echo "   - Database: iot_temperature_db"
        echo "   - User: iot_user"
        echo "   - Password: iot_password"
        echo ""
        echo "📁 Sample data files are in: ./landing_zone/"
        echo "📝 Logs are available in: ./logs/"
        echo ""
        echo "🔍 To check pipeline status:"
        echo "   docker-compose logs airflow_webserver"
        echo "   docker-compose logs airflow_scheduler"
        echo ""
        echo "🛑 To stop the pipeline:"
        echo "   docker-compose down"
    else
        echo "❌ Failed to start services. Check logs:"
        docker-compose logs
        exit 1
    fi
}

# Function to stop the pipeline
stop_pipeline() {
    echo "🛑 Stopping IoT Temperature Data Pipeline..."
    docker-compose down
    echo "✅ Pipeline stopped"
}

# Function to show logs
show_logs() {
    echo "📝 Pipeline Logs:"
    echo "=================="
    docker-compose logs airflow_webserver airflow_scheduler
}

# Function to show status
show_status() {
    echo "📊 Pipeline Status:"
    echo "==================="
    docker-compose ps
}

# Function to show database stats
show_stats() {
    echo "📈 Database Statistics:"
    echo "======================="
    docker-compose exec postgres psql -U iot_user -d iot_temperature_db -c "
    SELECT 
        'Raw Records' as table_name,
        COUNT(*) as record_count 
    FROM dlt_raw.raw_temperature_readings
    UNION ALL
    SELECT 
        'Marts Records' as table_name,
        COUNT(*) as record_count 
    FROM dbt_marts.mart_temperature_readings
    UNION ALL
    SELECT 
        'Anomalies Detected' as table_name,
        COUNT(*) as record_count 
    FROM dbt_marts.mart_temperature_readings 
    WHERE is_anomaly = true;
    "
}

# Main script logic
case "${1:-start}" in
    "start")
        check_docker
        check_ports
        start_pipeline
        ;;
    "stop")
        stop_pipeline
        ;;
    "restart")
        stop_pipeline
        sleep 2
        check_docker
        start_pipeline
        ;;
    "logs")
        show_logs
        ;;
    "status")
        show_status
        ;;
    "stats")
        show_stats
        ;;
    "help"|"-h"|"--help")
        echo "Usage: $0 [command]"
        echo ""
        echo "Commands:"
        echo "  start    - Start the pipeline (default)"
        echo "  stop     - Stop the pipeline"
        echo "  restart  - Restart the pipeline"
        echo "  logs     - Show pipeline logs"
        echo "  status   - Show service status"
        echo "  stats    - Show database statistics"
        echo "  help     - Show this help message"
        ;;
    *)
        echo "❌ Unknown command: $1"
        echo "Run '$0 help' for usage information"
        exit 1
        ;;
esac