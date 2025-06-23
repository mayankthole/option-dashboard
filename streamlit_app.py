
#!/usr/bin/env python3
"""
Script to run the Streamlit dashboard with enhanced features
"""

import subprocess
import sys
import os
import time

def check_dependencies():
    """Check and install required dependencies"""
    required_packages = [
        'streamlit',
        'plotly', 
        'pandas',
        'sqlalchemy',
        'psycopg2',
        'numpy'
    ]
    
    missing_packages = []
    
    for package in required_packages:
        try:
            __import__(package)
            print(f"✅ {package} is installed")
        except ImportError:
            missing_packages.append(package)
            print(f"❌ {package} not found")
    
    if missing_packages:
        print(f"\n📦 Installing missing packages: {', '.join(missing_packages)}")
        try:
            subprocess.check_call([sys.executable, "-m", "pip", "install", "-r", "requirements_dashboard.txt"])
            print("✅ All packages installed successfully")
            return True
        except subprocess.CalledProcessError as e:
            print(f"❌ Error installing packages: {e}")
            return False
    else:
        print("✅ All required packages are already installed")
        return True

def check_database_connection():
    """Check if database connection is available"""
    try:
        from database import engine
        from sqlalchemy import text
        
        with engine.connect() as conn:
            result = conn.execute(text("SELECT 1"))
            result.fetchone()  # Execute the query
        print("✅ Database connection successful")
        return True
    except Exception as e:
        print(f"❌ Database connection failed: {e}")
        print("💡 Make sure your database is running and .env file is configured")
        return False

def run_dashboard():
    """Run the Streamlit dashboard with enhanced setup"""
    
    print("🚀 Option Chain Dashboard Setup")
    print("=" * 50)
    
    # Check dependencies
    print("\n📦 Checking dependencies...")
    if not check_dependencies():
        print("❌ Failed to install dependencies. Please check your internet connection.")
        return
    
    # Check database connection
    print("\n🗄️ Checking database connection...")
    if not check_database_connection():
        print("❌ Cannot connect to database. Please check your configuration.")
        return
    
    # Run the dashboard
    print("\n🎯 Starting Option Chain Dashboard...")
    print("📊 Dashboard will open in your browser at: http://localhost:8501")
    print("🔄 Press Ctrl+C to stop the dashboard")
    print("=" * 50)
    print()
    
    try:
        # Run streamlit with enhanced configuration
        subprocess.run([
            sys.executable, "-m", "streamlit", "run", "dashboard.py",
            "--server.port", "8501",
            "--server.address", "localhost",
            "--browser.gatherUsageStats", "false",
            "--server.maxUploadSize", "200",
            "--server.enableCORS", "false",
            "--server.enableXsrfProtection", "false"
        ])
    except KeyboardInterrupt:
        print("\n🛑 Dashboard stopped by user")
    except Exception as e:
        print(f"\n❌ Error running dashboard: {e}")

def main():
    """Main function with error handling"""
    try:
        run_dashboard()
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        print("💡 Please check your setup and try again")

if __name__ == "__main__":
    main() 
