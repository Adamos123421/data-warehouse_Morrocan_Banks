# Moroccan Bank Data Warehouse

A comprehensive data warehouse solution for collecting, processing, and analyzing customer reviews from major Moroccan banks using Google Maps API, Apache Airflow, PostgreSQL, and advanced NLP techniques.

## 🏗️ Architecture Overview

This project implements a modern data warehouse architecture with the following components:

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Google Maps   │    │   Apache        │    │   PostgreSQL    │
│   API           │───▶│   Airflow       │───▶│   Database      │
│   (Data Source) │    │   (Orchestration)│    │   (Data Store)  │
└─────────────────┘    └─────────────────┘    └─────────────────┘
                                │
                                ▼
                       ┌─────────────────┐
                       │   NLP Pipeline  │
                       │   (Analysis)    │
                       └─────────────────┘
                                │
                                ▼
                       ┌─────────────────┐
                       │   Star Schema   │
                       │   (Analytics)   │
                       └─────────────────┘
```

## 🎯 Project Objectives

- **Automated Data Collection**: Daily collection of customer reviews from major Moroccan banks
- **Multi-language Support**: Handle French, Arabic, and English reviews
- **Sentiment Analysis**: Analyze customer sentiment and satisfaction trends
- **Geographic Analysis**: Track performance across different cities and regions
- **Real-time Monitoring**: Comprehensive monitoring and alerting system
- **Scalable Architecture**: Containerized deployment with Docker

## 🏦 Supported Banks

The system collects data from the following major Moroccan banks:

- Attijariwafa Bank
- Banque Populaire
- BMCE Bank
- Crédit Agricole du Maroc
- BMCI
- Société Générale Maroc
- CIH Bank
- CDM
- Al Barid Bank

## 🏙️ Geographic Coverage

Data is collected from major Moroccan cities including:
- Casablanca, Rabat, Fès, Marrakech, Agadir
- Tangier, Meknès, Oujda, Kenitra, Tetouan
- Safi, Mohammedia

## 🛠️ Technology Stack

### Core Technologies
- **Apache Airflow 2.7.3**: Workflow orchestration and scheduling
- **PostgreSQL 15**: Primary data warehouse
- **Redis**: Message broker for Airflow Celery
- **Docker & Docker Compose**: Containerization and deployment

### Data Processing
- **Python 3.9+**: Core programming language
- **Pandas**: Data manipulation and analysis
- **TextBlob**: Natural language processing
- **Scikit-learn**: Machine learning and topic modeling
- **Google Maps API**: Data collection source

### Monitoring & Management
- **pgAdmin**: PostgreSQL database management
- **Flower**: Celery task monitoring
- **Airflow Web UI**: Workflow monitoring and management

## 📁 Project Structure

```
moroccan-bank-data-warehouse/
├── dags/                          # Airflow DAGs
│   ├── morocco_banks_collection.py           # Data collection pipeline
│   ├── complete_data_pipeline_fixed.py       # ETL pipeline
│   ├── phase2_transformation_pipeline.py     # Advanced transformation
│   ├── nlp_processor_simple.py              # NLP processing
│   └── run_phase2_transformation.py         # Phase 2 execution
├── sql/                          # Database scripts
├── config/                       # Configuration files
├── logs/                         # Airflow logs
├── plugins/                      # Airflow plugins
├── src/                          # Source code
└── docker-compose.yml           # Docker orchestration
```

## 🚀 Quick Start

### Prerequisites

1. **Docker & Docker Compose**: Ensure Docker is installed and running
2. **Google Maps API Key**: Obtain an API key from Google Cloud Console
3. **System Requirements**: 
   - Minimum 8GB RAM
   - 20GB free disk space
   - Docker Desktop with 4GB+ memory allocation

### Installation & Setup

1. **Clone the Repository**
   ```bash
   git clone <repository-url>
   cd moroccan-bank-data-warehouse
   ```

2. **Configure Environment**
   ```bash
   # Set your Google Maps API key
   export GOOGLE_MAPS_API_KEY="your_api_key_here"
   ```

3. **Start the Services**
   ```bash
   docker-compose up -d
   ```

4. **Initialize Airflow**
   ```bash
   # Wait for services to be healthy, then initialize
   docker-compose exec airflow-webserver airflow db init
   ```

5. **Access the Applications**
   - **Airflow Web UI**: http://localhost:8081 (admin/admin)
   - **pgAdmin**: http://localhost:5050 (admin@morocco-bank.com/admin)
   - **Flower**: http://localhost:5555

## 📊 Data Pipeline Overview

### Phase 1: Data Collection (`morocco_banks_collection`)

**Schedule**: Daily at 2:00 AM

**Process**:
1. **Bank Discovery**: Searches for bank branches across major Moroccan cities
2. **Review Collection**: Collects up to 50 reviews per location
3. **Data Storage**: Stores raw data in PostgreSQL with metadata
4. **Quality Checks**: Validates data completeness and format

**Key Features**:
- Rate limiting to respect API quotas
- Automatic retry mechanisms
- Multi-language review collection
- Geographic coordinate tracking

### Phase 2: Data Transformation (`complete_data_pipeline_fixed`)

**Schedule**: Daily at 6:00 AM

**Process**:
1. **Data Validation**: Checks data availability and quality
2. **Star Schema Creation**: Builds dimensional model
3. **Data Cleansing**: Removes duplicates and invalid records
4. **Aggregation**: Creates summary tables and views

**Star Schema Components**:
- **Fact Table**: `fact_reviews` - Core review data
- **Dimension Tables**:
  - `dim_banks` - Bank information
  - `dim_locations` - Geographic data
  - `dim_time` - Temporal dimensions
  - `dim_reviewers` - Reviewer profiles

### Phase 3: Advanced Analytics (`phase2_transformation_pipeline`)

**Schedule**: Daily at 8:00 AM

**Process**:
1. **NLP Processing**: Sentiment analysis and topic modeling
2. **Language Detection**: Identifies review languages
3. **Text Translation**: Translates non-English reviews
4. **Insight Generation**: Creates analytical views

**Analytics Features**:
- Sentiment scoring (-1 to +1 scale)
- Topic categorization (8 predefined topics)
- Language distribution analysis
- Temporal trend analysis

## 🔧 Configuration

### Environment Variables

Key configuration parameters in `docker-compose.yml`:

```yaml
# API Configuration
GOOGLE_MAPS_API_KEY: 'your_api_key_here'
MAX_REVIEWS_PER_LOCATION: '50'
COLLECTION_DELAY_SECONDS: '2'
RETRY_ATTEMPTS: '3'

# Database Configuration
DB_HOST: 'postgres'
DB_PORT: '5432'
DB_NAME: 'morocco_bank_reviews'
DB_USER: 'morocco_app'
DB_PASSWORD: 'secure_password_here'

# Application Settings
LOG_LEVEL: 'INFO'
ENVIRONMENT: 'development'
```

### Database Schema

The system uses a multi-schema approach:

- **`raw_data`**: Raw collected data
- **`staging`**: Intermediate processing data
- **`analytics`**: Final analytical tables
- **`views`**: Business intelligence views

## 📈 Monitoring & Observability

### Airflow Monitoring
- **DAG Status**: Monitor pipeline execution in Airflow UI
- **Task Logs**: Detailed logs for each processing step
- **XCom**: Data passing between tasks
- **SLA Monitoring**: Service level agreement tracking

### Database Monitoring
- **pgAdmin**: Visual database management
- **Query Performance**: Monitor slow queries
- **Data Quality**: Automated quality checks

### System Health
- **Health Checks**: All services include health monitoring
- **Resource Usage**: Monitor CPU, memory, and disk usage
- **Error Alerting**: Email notifications for failures

## 🔍 Data Quality & Validation

### Automated Checks
- **Data Completeness**: Ensures all required fields are present
- **Data Types**: Validates field formats and types
- **Duplicate Detection**: Identifies and handles duplicate reviews
- **API Response Validation**: Verifies Google Maps API responses

### Quality Metrics
- **Collection Success Rate**: Percentage of successful API calls
- **Data Freshness**: Time since last successful collection
- **Coverage Metrics**: Number of banks and locations covered
- **Review Volume**: Daily/monthly review counts

## 🚨 Troubleshooting

### Common Issues

1. **API Rate Limiting**
   ```bash
   # Check API quota usage
   docker-compose logs airflow-scheduler | grep "quota"
   ```

2. **Database Connection Issues**
   ```bash
   # Test database connectivity
   docker-compose exec postgres psql -U morocco_app -d morocco_bank_reviews
   ```

3. **Airflow Task Failures**
   ```bash
   # Check task logs
   docker-compose logs airflow-scheduler
   ```

### Debug Commands

```bash
# View all service logs
docker-compose logs

# Restart specific service
docker-compose restart airflow-scheduler

# Check service health
docker-compose ps

# Access Airflow CLI
docker-compose exec airflow-webserver airflow dags list
```

## 📊 Analytics & Insights

### Available Views

1. **`vw_bank_performance_dashboard`**
   - Overall bank performance metrics
   - Average ratings by bank
   - Review volume trends

2. **`vw_geographic_analysis`**
   - Performance by city/region
   - Geographic distribution of reviews
   - Regional sentiment analysis

3. **`vw_monthly_trends`**
   - Monthly performance trends
   - Seasonal patterns
   - Year-over-year comparisons

### Sample Queries

```sql
-- Top performing banks
SELECT bank_name, AVG(rating) as avg_rating, COUNT(*) as review_count
FROM analytics.fact_reviews
GROUP BY bank_name
ORDER BY avg_rating DESC;

-- Sentiment analysis by city
SELECT city, AVG(sentiment_score) as avg_sentiment
FROM analytics.fact_reviews fr
JOIN analytics.dim_locations dl ON fr.location_id = dl.location_id
GROUP BY city
ORDER BY avg_sentiment DESC;
```

## 🔐 Security Considerations

### API Security
- API keys stored as environment variables
- Rate limiting to prevent abuse
- Secure API key rotation procedures

### Database Security
- Isolated network for database services
- Strong password policies
- Regular security updates

### Access Control
- Role-based access in Airflow
- Database user permissions
- Audit logging for data access

## 🚀 Scaling & Performance

### Horizontal Scaling
- Multiple Airflow workers for parallel processing
- Redis clustering for high availability
- Database read replicas for analytics

### Performance Optimization
- Database indexing on frequently queried columns
- Partitioning for large tables
- Query optimization and caching

### Resource Management
- Memory limits for containers
- CPU allocation for compute-intensive tasks
- Storage optimization for logs and data

## 📝 Contributing

### Development Setup
1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests and documentation
5. Submit a pull request

### Code Standards
- Follow PEP 8 for Python code
- Add docstrings for all functions
- Include type hints where appropriate
- Write comprehensive tests

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🤝 Support

For support and questions:
- Create an issue in the repository
- Check the troubleshooting section
- Review Airflow and PostgreSQL documentation

## 🔄 Version History

- **v1.0.0**: Initial release with basic data collection
- **v1.1.0**: Added NLP processing and sentiment analysis
- **v1.2.0**: Implemented star schema and advanced analytics
- **v1.3.0**: Enhanced monitoring and error handling

---

**Note**: This documentation is maintained alongside the codebase. For the most up-to-date information, always refer to the latest version in the repository.
