#!/bin/bash
# Operate Optima Quick Start Script for Linux/Mac
# Automates the complete setup and execution process

set -e

echo ""
echo "================================================================================"
echo "               🚀 OPERATE OPTIMA ETL PIPELINE - QUICK START"
echo "================================================================================"
echo ""

# Check if Python is installed
if ! command -v python3 &> /dev/null; then
    echo "❌ Python 3 is not installed or not in PATH"
    echo "Please install Python 3.8+ and try again"
    exit 1
fi

echo "✅ Python 3 detected"
echo ""

# Make script executable
chmod +x "$0"

# Run the setup script
echo "🔧 Setting up environment..."
python3 setup.py
if [ $? -ne 0 ]; then
    echo "❌ Setup failed"
    exit 1
fi

show_menu() {
    echo ""
    echo "🎯 Choose an option:"
    echo "1. Run ETL Pipeline"
    echo "2. Run Tests"
    echo "3. Run Performance Benchmark"
    echo "4. Start Web Interface"
    echo "5. Exit"
    echo ""
    read -p "Enter your choice (1-5): " choice
}

run_pipeline() {
    echo ""
    echo "🚀 Running ETL Pipeline..."
    source venv/bin/activate
    python pipeline.py
    if [ $? -ne 0 ]; then
        echo "❌ Pipeline execution failed"
    else
        echo "✅ Pipeline completed successfully!"
    fi
    deactivate
}

run_tests() {
    echo ""
    echo "🧪 Running Test Suite..."
    source venv/bin/activate
    python -m pytest tests/ -v
    if [ $? -ne 0 ]; then
        echo "❌ Some tests failed"
    else
        echo "✅ All tests passed!"
    fi
    deactivate
}

run_benchmark() {
    echo ""
    echo "📊 Running Performance Benchmark..."
    source venv/bin/activate
    python benchmark.py
    if [ $? -ne 0 ]; then
        echo "❌ Benchmark failed"
    else
        echo "✅ Benchmark completed!"
    fi
    deactivate
}

start_web() {
    echo ""
    echo "🌐 Starting Web Interface..."
    echo "Opening web interface at http://localhost:8000"
    echo "Press Ctrl+C to stop the server"
    
    # Try to open browser (works on most systems)
    if command -v xdg-open &> /dev/null; then
        xdg-open http://localhost:8000 &
    elif command -v open &> /dev/null; then
        open http://localhost:8000 &
    fi
    
    source venv/bin/activate
    python -m http.server 8000 --directory web
    deactivate
}

# Main loop
while true; do
    show_menu
    
    case $choice in
        1)
            run_pipeline
            ;;
        2)
            run_tests
            ;;
        3)
            run_benchmark
            ;;
        4)
            start_web
            ;;
        5)
            echo ""
            echo "👋 Thank you for using Operate Optima!"
            echo ""
            exit 0
            ;;
        *)
            echo "❌ Invalid choice. Please enter 1-5."
            ;;
    esac
done
