@echo off
REM Operate Optima Quick Start Script for Windows
REM Automates the complete setup and execution process

echo.
echo ================================================================================
echo               🚀 OPERATE OPTIMA ETL PIPELINE - QUICK START
echo ================================================================================
echo.

REM Check if Python is installed
python --version >nul 2>&1
if %errorlevel% neq 0 (
    echo ❌ Python is not installed or not in PATH
    echo Please install Python 3.8+ and try again
    pause
    exit /b 1
)

echo ✅ Python detected
echo.

REM Run the setup script
echo 🔧 Setting up environment...
python setup.py
if %errorlevel% neq 0 (
    echo ❌ Setup failed
    pause
    exit /b 1
)

echo.
echo 🎯 Choose an option:
echo 1. Run ETL Pipeline
echo 2. Run Tests
echo 3. Run Performance Benchmark
echo 4. Start Web Interface
echo 5. Exit
echo.
set /p choice="Enter your choice (1-5): "

if "%choice%"=="1" goto run_pipeline
if "%choice%"=="2" goto run_tests
if "%choice%"=="3" goto run_benchmark
if "%choice%"=="4" goto start_web
if "%choice%"=="5" goto exit

:run_pipeline
echo.
echo 🚀 Running ETL Pipeline...
venv\Scripts\python pipeline.py
if %errorlevel% neq 0 (
    echo ❌ Pipeline execution failed
) else (
    echo ✅ Pipeline completed successfully!
)
goto menu

:run_tests
echo.
echo 🧪 Running Test Suite...
venv\Scripts\python -m pytest tests/ -v
if %errorlevel% neq 0 (
    echo ❌ Some tests failed
) else (
    echo ✅ All tests passed!
)
goto menu

:run_benchmark
echo.
echo 📊 Running Performance Benchmark...
venv\Scripts\python benchmark.py
if %errorlevel% neq 0 (
    echo ❌ Benchmark failed
) else (
    echo ✅ Benchmark completed!
)
goto menu

:start_web
echo.
echo 🌐 Starting Web Interface...
echo Opening web interface at http://localhost:8000
echo Press Ctrl+C to stop the server
start http://localhost:8000
venv\Scripts\python -m http.server 8000 --directory web
goto menu

:menu
echo.
echo 🎯 What would you like to do next?
echo 1. Run ETL Pipeline
echo 2. Run Tests  
echo 3. Run Performance Benchmark
echo 4. Start Web Interface
echo 5. Exit
echo.
set /p choice="Enter your choice (1-5): "

if "%choice%"=="1" goto run_pipeline
if "%choice%"=="2" goto run_tests
if "%choice%"=="3" goto run_benchmark
if "%choice%"=="4" goto start_web
if "%choice%"=="5" goto exit

:exit
echo.
echo 👋 Thank you for using Operate Optima!
echo.
pause
