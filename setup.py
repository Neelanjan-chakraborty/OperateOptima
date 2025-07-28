"""
Setup script for Operate Optima ETL Pipeline
Automates environment setup and dependency installation
"""

import subprocess
import sys
import os
from pathlib import Path

def run_command(command, description):
    """Run a command and handle errors"""
    print(f"🔧 {description}...")
    try:
        result = subprocess.run(command, shell=True, check=True, capture_output=True, text=True)
        print(f"✅ {description} completed successfully")
        return True
    except subprocess.CalledProcessError as e:
        print(f"❌ {description} failed: {e.stderr}")
        return False

def check_python_version():
    """Check if Python version is 3.8 or higher"""
    version = sys.version_info
    if version.major < 3 or (version.major == 3 and version.minor < 8):
        print("❌ Python 3.8 or higher is required")
        return False
    print(f"✅ Python {version.major}.{version.minor}.{version.micro} detected")
    return True

def setup_environment():
    """Set up the development environment"""
    print("🚀 Setting up Operate Optima ETL Pipeline Environment")
    print("=" * 60)
    
    # Check Python version
    if not check_python_version():
        return False
    
    # Create virtual environment
    if not os.path.exists("venv"):
        if not run_command("python -m venv venv", "Creating virtual environment"):
            return False
    else:
        print("✅ Virtual environment already exists")
    
    # Activate virtual environment and install dependencies
    if sys.platform == "win32":
        activate_cmd = "venv\\Scripts\\activate"
        pip_cmd = "venv\\Scripts\\pip"
    else:
        activate_cmd = "source venv/bin/activate"
        pip_cmd = "venv/bin/pip"
    
    # Upgrade pip
    if not run_command(f"{pip_cmd} install --upgrade pip", "Upgrading pip"):
        return False
    
    # Install requirements
    if not run_command(f"{pip_cmd} install -r requirements.txt", "Installing dependencies"):
        return False
    
    # Create sample data if it doesn't exist
    create_sample_data_if_missing()
    
    print("\n🎉 Environment setup completed successfully!")
    print("\n📋 Next steps:")
    print("1. Activate the virtual environment:")
    if sys.platform == "win32":
        print("   venv\\Scripts\\activate")
    else:
        print("   source venv/bin/activate")
    print("2. Run the pipeline:")
    print("   python pipeline.py")
    print("3. Open the landing page:")
    print("   python -m http.server 8000 --directory web")
    
    return True

def create_sample_data_if_missing():
    """Create sample data file if missing"""
    raw_data_path = Path("data/raw/SampleSalesData.csv")
    
    if not raw_data_path.exists():
        print("🔧 Creating sample data file...")
        
        # Ensure directory exists
        raw_data_path.parent.mkdir(parents=True, exist_ok=True)
        
        sample_data = """OrderDate,Region,Rep,Item,Units,Unit Cost,Total
1/6/19,East,Jones,Pencil,95,1.99,189.05
1/23/19,Central,Kivell,Binder,50,19.99,999.50
2/9/19,Central,Jardine,Pencil,36,4.99,179.64
2/26/19,Central,Gill,Pen,27,19.99,539.73
3/15/19,West,Sorvino,Pencil,56,2.99,167.44
4/1/19,East,Jones,Binder,60,4.99,299.40
4/18/19,Central,Andrews,Pencil,75,1.99,149.25
5/5/19,Central,Jardine,Pencil,90,4.99,449.10
5/22/19,West,Thompson,Pencil,32,1.99,63.68
6/8/19,East,Jones,Binder,60,8.99,539.40
6/25/19,Central,Morgan,Pencil,90,4.99,449.10
7/12/19,East,Howard,Binder,29,1.99,57.71
7/29/19,East,Parent,Binder,81,19.99,1619.19
8/15/19,East,Jones,Pencil,35,4.99,174.65
9/1/19,Central,Smith,Desk,2,125.00,250.00
9/18/19,East,Jones,Pen Set,16,15.99,255.84
10/5/19,Central,Morgan,Binder,28,8.99,251.72
10/22/19,East,Jones,Pen,64,8.99,575.36
11/8/19,East,Parent,Pen,15,19.99,299.85
11/25/19,Central,Kivell,Pen Set,96,4.99,479.04
12/12/19,Central,Smith,Pencil,67,1.29,86.43"""
        
        with open(raw_data_path, 'w') as f:
            f.write(sample_data)
        
        print(f"✅ Sample data created at: {raw_data_path}")
    else:
        print("✅ Sample data file already exists")

if __name__ == "__main__":
    setup_environment()
