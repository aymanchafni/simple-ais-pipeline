# 🚢 Tanger Med AIS Data Pipeline

A maritime traffic analysis system that processes ship tracking data (AIS) for port traffic monitoring.

## What it does

- Downloads ship tracking data from NOAA or local files
- Cleans and processes the data
- Stores it in a PostgreSQL database
- Provides a REST API to query the data
- Shows interactive charts in a web dashboard

## Quick Start

### 1. Get the code and configure data loading
```bash
git clone <this-repo>
cd tanger-med-ais-pipeline
```

**Configure data loading (optional):**
Edit `scripts/docker_entrypoint.sh` and modify the pipeline command:
```bash
# Default: downloads NOAA data (commented out by default)
python scripts/run_pipeline.py --noaa-year 2024 --noaa-zone "01_01" --max-records 100000

# loads sample data instead
#python scripts/run_pipeline.py --local-file sample_data/sample_ais.csv --max-records 100 --verbose 

# Other options you can use:
# python scripts/run_pipeline.py --local-file /path/to/your/data.csv --max-records 10000
# python scripts/run_pipeline.py --url "https://example.com/ais_data.zip" --max-records 50000
```

**Alternative: Use Airflow instead**  
Skip auto-loading entirely by commenting out ALL pipeline commands above. This lets Airflow handle scheduled data processing via the web interface at http://localhost:8080 (available with `--profile full`).

### 2. Start the system
```bash
# Basic setup
docker-compose up -d

# Or with dashboard and Airflow
docker-compose --profile full up -d
```

### 3. Access the services
- **API**: http://localhost:8000
- **Dashboard**: http://localhost:8501 (with `--profile full`)
- **Database**: localhost:5432
- **Airflow**: http://localhost:8080 (with `--profile full`)

### 4. View your data
Check the dashboard or try the API:
```bash
curl http://localhost:8000/vessels
```

## Technology Stack

### Why these tools?
- **PostgreSQL**: Handles millions of GPS coordinates efficiently with spatial indexing and time-series optimization
- **FastAPI**: High-performance Python API with automatic documentation and type validation
- **Streamlit**: Rapid dashboard development with interactive charts - no frontend coding needed
- **Docker**: Consistent deployment across environments with all dependencies included

### 📁 Main Components
- **API** (`src/api/`): REST endpoints to get vessel data
- **Data Processing** (`src/`): Loads and cleans AIS data
- **Dashboard** (`dashboard/`): Web interface with charts
- **Database**: PostgreSQL with optimized tables
- **Scripts** (`scripts/`): Command-line tools

### 🔄 Data Flow
1. **Load data** from NOAA website or CSV files
2. **Clean data** (remove invalid positions, duplicates)
3. **Calculate metrics** (distance traveled, time at dock)
4. **Store in database** 
5. **Serve via API** and dashboard

## Usage

### Processing your own data
```bash
# Local CSV file
docker-compose exec app python scripts/run_pipeline.py --local-file data/my_data.csv

# Download from NOAA (2024 data)
docker-compose exec app python scripts/run_pipeline.py --noaa-year 2024 --noaa-zone "01_01"
```

### Using the API
```bash
# List all vessels
curl http://localhost:8000/vessels

# Get specific vessel details
curl http://localhost:8000/vessels/219018671

# Get traffic statistics
curl http://localhost:8000/statistics

# Search vessels by name
curl "http://localhost:8000/vessels/search?name=MAERSK"
```

### Dashboard Features
- **Overview**: Fleet statistics and top vessels
- **Vessels**: Browse all ships with details
- **Metrics**: Charts showing movement patterns
- **Search**: Find specific vessels

## Configuration

### Environment Variables (.env file)
```bash
DB_HOST=localhost
DB_PORT=5432
DB_NAME=tanger_med
DB_USER=postgres
DB_PASSWORD=password
```
This is only for demo purposes, not recommended to have .env file in the repo.

### Data Sources
- **NOAA AIS Data**: Automatic download from https://coast.noaa.gov/htdata/CMSP/AISDataHandler/
- **Local Files**: CSV or ZIP files
- **Sample Data**: Included in `sample_data/sample_ais.csv`

## Development

### Running locally (without Docker)
```bash
# Install dependencies
pip install -r requirements.txt

# Start database
docker-compose up -d postgres

# Initialize database
python scripts/init_db.py

# Start API
python -m uvicorn src.api.main:app --reload

# Start dashboard
streamlit run dashboard/app.py
```

### Project Structure
```
├── src/                 # Main application code
├── scripts/            # Command-line tools
├── dashboard/          # Web dashboard
├── sample_data/        # Test data
├── docker-compose.yml  # Container setup
└── requirements.txt    # Python dependencies
```

## Troubleshooting

### Common Issues

**Services won't start:**
```bash
docker-compose down
docker-compose up -d
```

**No data showing:**
```bash
# Check if data loaded
docker-compose logs data-init

# Manually load sample data
docker-compose exec app python scripts/run_pipeline.py --local-file sample_data/sample_ais.csv
```

**API not responding:**
```bash
# Check API status
curl http://localhost:8000/health

# View logs
docker-compose logs app
```

**Database connection issues:**
```bash
# Check database
docker-compose logs postgres

# Test connection
docker-compose exec postgres psql -U postgres -d tanger_med -c "SELECT COUNT(*) FROM ais_data;"
```

## Requirements

- Docker & Docker Compose
- 4GB RAM minimum
- 10GB free disk space

## Support

- Check the logs: `docker-compose logs <service-name>`
- Open an issue on GitHub
- Review the API docs at http://localhost:8000/docs

---

**Built for maritime traffic analysis** 🌊
