# HedgeFund Agent

A sophisticated financial content generation platform that autonomously creates and publishes market commentary, deep-dive analyses, and comprehensive market briefings. Built for institutional-grade financial analysis with automated social media distribution and portfolio management insights.

## 🏗️ Architecture

The HedgeFund Agent follows a modern microservices-inspired architecture with clear separation of concerns:

```
hedgefund_agent/
├── core/                    # Core business logic and models
│   ├── content_engine.py    # Main orchestration engine
│   └── models.py           # Data models and enums
├── services/               # Business services layer
│   ├── database_service.py  # PostgreSQL operations
│   ├── gpt_service.py      # Azure OpenAI integration
│   ├── market_client.py    # Market data API client
│   ├── publishing_service.py # Twitter/X publishing
│   ├── notion_publisher.py # Notion integration
│   └── telegram_notifier.py # Operational notifications
├── generators/             # Content generation engines
│   ├── commentary_generator.py # Single-tweet commentary
│   ├── deep_dive_generator.py  # Multi-tweet threads
│   └── briefing_generator.py   # Market briefings
├── config/                 # Configuration management
│   ├── settings.py         # Environment configuration
│   └── sentiment_config.py # Market sentiment weights
└── scheduler.py           # Production job scheduler
```

## 🚀 Key Features

### Content Generation
- **Market Commentary**: AI-powered single-tweet market analysis with real-time price data
- **Deep Dive Threads**: Multi-part analytical threads on high-impact market events  
- **Market Briefings**: Comprehensive morning briefings with sentiment analysis and economic calendars
- **Intelligent Diversity**: Notion-backed content memory prevents repetitive themes and categories

### Publishing & Distribution
- **Multi-Platform Publishing**: Automated posting to Twitter/X and Notion databases
- **Website Integration**: HTTP server providing real-time market data for website consumption
- **Engagement Tracking**: Automatic engagement metrics collection and analysis

### Market Intelligence
- **Real-Time Data**: Integration with multiple market data providers (Finnhub, Yahoo Finance, IG Index)
- **Sentiment Analysis**: Multi-asset sentiment scoring across equities, forex, crypto, and bonds
- **News Aggregation**: RSS feed processing with GPT-based relevance scoring
- **Economic Calendar**: Automated integration of earnings, IPOs, and macro events

### Operational Excellence
- **Production Scheduler**: BST/GMT-aware scheduling with 15 daily publications
- **Health Monitoring**: Comprehensive system health checks and Telegram notifications
- **Database Management**: PostgreSQL with proper schema management and migrations
- **Error Handling**: Robust error handling with automatic retries and notifications

## 📊 Content Schedule

### Weekdays (15 publications)
- **4 Market Briefings**: 07:30, 14:15, 17:00, 21:45 BST
- **9 Commentary Posts**: Throughout trading hours
- **2 Deep Dive Threads**: Monday, Wednesday, Friday evenings

### Weekends (11 publications)  
- **3 Market Briefings**: Reduced weekend schedule
- **6 Commentary Posts**: Lighter weekend coverage
- **2 Deep Dive Threads**: Saturday and Sunday

## 🛠️ Tech Stack

### Core Technologies
- **Python 3.9+**: Primary development language
- **PostgreSQL**: Primary database with shared schema architecture
- **Azure OpenAI**: GPT-4 for content generation with institutional prompting
- **FastAPI**: HTTP server for website integration

### External Integrations
- **Twitter API v2**: Content publishing and engagement tracking
- **Notion API**: Content storage and website database integration
- **Telegram Bot API**: Operational notifications and health monitoring
- **Market Data APIs**: Finnhub, Yahoo Finance, IG Index for real-time data

### Infrastructure
- **Supervisor**: Process management for production deployment
- **Schedule**: Python-based job scheduling with timezone handling
- **Docker-Ready**: Containerization support for scalable deployment

## 🚦 Getting Started

### Prerequisites
```bash
# System requirements
Python 3.9+
PostgreSQL 12+
Git
```

### Installation

1. **Clone the repository**
```bash
git clone <repository-url>
cd hedgefund_agent
```

2. **Create virtual environment**
```bash
python3 -m venv venv
source venv/bin/activate  # Linux/Mac
# or
venv\Scripts\activate     # Windows
```

3. **Install dependencies**
```bash
pip install -r requirements.txt
```

4. **Environment Configuration**
```bash
cp .env.example .env
# Edit .env with your API keys and database credentials
```

5. **Database Setup**
```bash
# Create PostgreSQL database and run migrations
# See DEPLOYMENT.md for detailed database setup
```

### Environment Variables

Create a `.env` file with the following variables:

```env
# Database
DB_HOST=localhost
DB_PORT=5432
DB_NAME=agents_platform
DB_USER=admin
DB_PASSWORD=your_secure_password

# Azure OpenAI
AZURE_OPENAI_API_KEY=your_azure_openai_key
AZURE_DEPLOYMENT_ID=your_deployment_id
AZURE_API_VERSION=2024-02-15-preview
AZURE_RESOURCE_NAME=your_resource_name

# Twitter API v2
TWITTER_CONSUMER_KEY=your_consumer_key
TWITTER_CONSUMER_SECRET=your_consumer_secret
TWITTER_ACCESS_TOKEN=your_access_token
TWITTER_ACCESS_TOKEN_SECRET=your_access_token_secret

# Notion Integration
NOTION_API_KEY=your_notion_api_key
HEDGEFUND_TWEET_DB_ID=your_tweet_database_id
NOTION_PDF_DATABASE_ID=your_briefing_database_id

# Telegram Notifications
TELEGRAM_BOT_TOKEN=your_telegram_bot_token
TELEGRAM_CHAT_ID=your_chat_id

# Market Data Service
MARKET_DATA_SERVICE_URL=http://localhost:8001
```

## 🏃‍♂️ Running the System

### Development Mode
```bash
# Run individual components for testing
python -m services.headline_pipeline  # Test headline fetching
python -m generators.commentary_generator  # Test commentary generation
python -m core.content_engine  # Test full content pipeline
```

### Production Mode
```bash
# Using supervisor for process management
sudo ./setup_supervisor.sh

# Check status
sudo supervisorctl status hedgefund-scheduler

# View logs
tail -f logs/scheduler.log
```

### HTTP Server (Website Integration)
```bash
# Start standalone HTTP server
python hedgefund_http_server.py

# Access endpoints
curl http://localhost:3002/hedgefund-news-data
curl http://localhost:3002/health
```

## 📖 API Documentation

### HTTP Endpoints

#### News Data API
```
GET /hedgefund-news-data
```
Returns rotating market headlines with GPT-generated institutional commentary for website integration.

#### Health Check
```
GET /health
```
Comprehensive system health status including database connectivity, service status, and performance metrics.

### Content Generation API

The system exposes programmatic content generation through the ContentEngine:

```python
from core.content_engine import ContentEngine, ContentRequest, ContentType

engine = ContentEngine()

# Generate commentary
request = ContentRequest(content_type=ContentType.COMMENTARY)
result = await engine.generate_and_publish_content(request)

# Generate deep dive
request = ContentRequest(content_type=ContentType.DEEP_DIVE) 
result = await engine.generate_and_publish_content(request)
```

## 🏗️ Scalability Considerations

### Database Architecture
- **Shared Schema**: `hedgefund_agent` schema within shared `agents_platform` database
- **Connection Pooling**: Efficient PostgreSQL connection management
- **Indexing Strategy**: Optimized queries for headline scoring and retrieval

### Content Generation
- **Modular Generators**: Separate generators for different content types enable horizontal scaling
- **Async Processing**: Concurrent API calls and database operations
- **Rate Limit Handling**: Intelligent backoff strategies for external APIs

### Monitoring & Observability
- **Structured Logging**: Comprehensive logging with correlation IDs
- **Health Endpoints**: Real-time system health monitoring
- **Performance Metrics**: Database query performance and API response times

## 🔐 Security

- **API Key Management**: Environment-based secret management
- **Database Security**: Parameterized queries preventing SQL injection
- **Rate Limiting**: Built-in rate limiting for external API calls
- **Input Validation**: Comprehensive input validation for all user data

## 📈 Monitoring & Maintenance

### Logs
```bash
# Application logs
tail -f logs/scheduler.log

# Supervisor logs  
sudo tail -f /var/log/supervisor/supervisord.log

# System logs
journalctl -u postgresql -f
```

### Health Monitoring
- **Telegram Notifications**: Real-time alerts for system issues
- **Database Health**: Automatic connection testing and query performance monitoring  
- **API Health**: External service availability monitoring
- **Content Quality**: GPT response validation and fallback mechanisms

### Maintenance Tasks
- **Daily Cleanup**: Automated cleanup of old headlines and metrics
- **Performance Optimization**: Query optimization and index maintenance
- **Backup Strategy**: Automated database backups and recovery procedures

## 🤝 Contributing

### Development Workflow
1. Create feature branch from `main`
2. Implement changes with comprehensive tests
3. Update documentation for API changes
4. Submit PR with detailed description

### Code Standards
- **PEP 8**: Python style guide compliance
- **Type Hints**: Comprehensive type annotations
- **Documentation**: Docstrings for all public methods
- **Error Handling**: Comprehensive exception handling

### Testing Strategy
```bash
# Unit tests
python -m pytest tests/unit/

# Integration tests
python -m pytest tests/integration/

# End-to-end tests
python -m pytest tests/e2e/
```

## 📄 License

Proprietary - All rights reserved

## 🆘 Support

For technical support and questions:
- **Documentation**: See `/docs` folder for detailed guides
- **Issues**: Create GitHub issues for bugs and feature requests
- **Deployment**: See `DEPLOYMENT.md` for production deployment guide

---

## 📋 Quick Commands Reference

```bash
# Start system
sudo supervisorctl start hedgefund-scheduler

# Stop system  
sudo supervisorctl stop hedgefund-scheduler

# View logs
tail -f logs/scheduler.log

# Check health
curl http://localhost:3002/health

# Manual content generation
python -c "
from core.content_engine import publish_commentary_now
import asyncio
asyncio.run(publish_commentary_now())
"

# Database connection test
python -c "
from services.database_service import DatabaseService
from config.settings import DATABASE_CONFIG
db = DatabaseService(DATABASE_CONFIG)
print('✅ Database connected' if db.get_connection() else '❌ Database failed')
"
```