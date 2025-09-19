# HTD-Research-Agent

## Overview

**Purpose**: Autonomous financial content generation platform that creates and publishes institutional-grade market commentary, deep-dive analyses, and comprehensive market briefings for HTD Research social media presence.

**Main Functionality**:
- Generates single-tweet market commentary with real-time data enrichment
- Creates multi-part analytical threads (deep dives) on high-impact market events
- Produces comprehensive morning/evening market briefings with sentiment analysis
- Manages content diversity and deduplication using Notion-backed memory
- Provides real-time market data via HTTP API for website integration

**Current Status**: ✅ **Active** - Production deployment on local VM infrastructure

---

## Infrastructure

### Deployment Location
- **Infrastructure**: Docker containerized deployment (migrated from Azure)
- **Container**: `htd-agent` (part of multi-service Docker Compose stack)
- **Primary Port**: 3002 (HTTP API server)
- **Process Management**: Docker container with restart policy `unless-stopped`
- **Runtime**: Python 3.11-slim in containerized environment
- **Working Directory**: `/app` (inside container)

### Dependencies
**Database**:
- PostgreSQL container (`postgres:15`)
- Database: `agents_platform` 
- Schema: `hedgefund_agent` with shared tables in `shared` schema
- Connection: `postgres:5432` (internal Docker network)
- SSL disabled for internal container communication

**Containerized Services**:
- **Market Data Service**: `market-data-service:8001` (internal network)
- **Redis**: `redis:6379` (caching layer)
- **Frontend**: `frontend:3000` (website integration)

**External APIs**:
- Twitter API v2 (content publishing)
- Notion API (content storage & website integration)
- Telegram Bot API (operational notifications)
- Azure OpenAI (GPT-4 content generation)

**Container Network**:
- Network: `production-network` (Docker bridge)
- Internal service discovery via container names
- External access via exposed ports

### Resource Requirements
- **CPU**: Multi-core recommended (concurrent API calls, GPT processing)
- **Memory**: 2GB+ allocated to container (content generation, market data caching)
- **Disk**: Container storage + mounted log volume
- **Network**: Docker bridge network + internet access for external APIs
- **Container Restart Policy**: `unless-stopped` for high availability

---

## Key Functions

### Content Generation Jobs
1. **Market Commentary** (9x daily weekdays, 6x weekends)
   - Single-tweet institutional market analysis
   - Real-time market data enrichment with cashtags
   - Notion-backed diversity tracking to avoid repetition

2. **Deep Dive Threads** (3x weekly + weekends)
   - Multi-part analytical threads (2-4 tweets)
   - High-scoring headline analysis with GPT-powered insights
   - Complex market data integration

3. **Market Briefings** (4x daily weekdays, 3x weekends)
   - Comprehensive market analysis with sentiment scoring
   - Economic calendar integration (earnings, IPOs)
   - Multi-asset sentiment analysis across equities, forex, crypto, bonds
   - PDF-style Notion pages with institutional formatting

### Data Pipeline Jobs
1. **Headline Pipeline** (every 30 minutes)
   - RSS feed aggregation from 15+ financial news sources
   - GPT-based relevance scoring (1-10 scale)
   - Duplicate detection and theme tracking

2. **System Health Monitoring** (every 30 minutes)
   - Service health checks (database, APIs, HTTP server)
   - Performance metrics collection
   - Telegram notifications for failures

### Operational Frequency
- **Peak Activity**: 15 publications daily (weekdays)
- **Weekend Activity**: 11 publications daily
- **Maintenance**: Hourly heartbeats, daily cleanup
- **BST/GMT Awareness**: Automatic timezone adjustment for UK markets

---

## APIs/Endpoints

### Internal Endpoints (Port 3002)
```
GET /hedgefund-news-data
- Returns rotating market headlines with GPT-generated institutional commentary
- Used by DutchBrat website for real-time news display
- 5-minute rotation cycle with smart headline selection

GET /health
- Comprehensive system health status
- Database connectivity, service status, performance metrics
- Rotation info and headline statistics
```

### External API Integrations
**Twitter API v2**:
- `POST /2/tweets` - Single tweet and thread publishing
- Rate limiting with intelligent backoff
- Engagement tracking (planned)

**Notion API**:
- Database operations for content storage and website integration
- Rich content formatting for briefings
- Automatic content categorization

**Market Data Service API** (Internal Container Network):
- `POST /api/v1/prices/bulk` - Bulk price data fetching
- `GET /api/v1/news/company/{symbol}` - Company-specific news
- `GET /api/v1/calendar/earnings` - Earnings calendar
- `GET /api/v1/calendar/ipo` - IPO calendar
- **Internal URL**: `http://market-data-service:8001`

**Azure OpenAI**:
- GPT-4 content generation with institutional prompting
- Headline scoring and categorization
- Market sentiment analysis

### Authentication Methods
- **Environment-based secrets** via Docker environment variables and mounted env files
- **OAuth 1.0a** for Twitter API
- **Bearer tokens** for Notion and Azure OpenAI
- **SSL/TLS** for external communications (SSL disabled for internal container network)
- **Docker secrets management** via compose env_file configuration

---

## Data Storage

### Primary Database (PostgreSQL)
**Database**: `agents_platform`
**Schema**: `hedgefund_agent`

**Key Tables**:
```sql
-- News headlines with GPT scoring
headlines (id, headline, summary, score, category, source, url, used, created_at)

-- Content theme deduplication  
themes (id, theme, usage_count, first_used_at, last_used_at)

-- Briefing metadata and URLs
briefings (id, briefing_type, notion_page_id, title, website_url, tweet_url, json_content)

-- Symbol universe for market data
stock_universe (id, symbol, display_name, asset_type, epic, active)

-- Market data configuration
market_blocks_config (id, briefing_type, block_name, stock_universe_id, priority, active)

-- Content generation logging
content_log (id, content_type, theme, headline_id, success, url, details, created_at)
```

### File Storage
- **Logs**: `/app/logs/` (mounted volume to host: `./HTD-Research-Agent/logs:/app/logs`)
- **Configuration**: Docker environment variables + mounted env files:
  - `./config/secrets/api-keys.env`
  - `./config/services/htd-research.env`
- **Application Code**: Containerized in `/app`

### Backup Procedures
- **Database**: Automated PostgreSQL backups via container management
- **Configuration**: Version controlled + mounted env files
- **Logs**: Accessible via mounted volume on host system
- **Container State**: Managed via Docker Compose with restart policies

---

## Monitoring & Logging

### Logging Mechanisms
**Primary Logger**: Python logging with structured format
```
%(asctime)s - %(name)s - %(levelname)s - %(message)s
```

**Log Files**:
- `logs/scheduler.log` - Main application logs (mounted volume)
- Docker container logs: `docker logs htd-agent`
- Docker Compose logs: `docker-compose logs htd-agent`

**Log Levels**:
- **INFO**: Normal operations, job completion
- **WARNING**: Non-critical issues, API delays
- **ERROR**: Failed operations, API errors
- **CRITICAL**: System failures requiring intervention

### Error Handling
**Graceful Degradation**:
- Failed external APIs don't crash scheduler
- Market data failures use cached/fallback data
- GPT failures use template responses
- Database issues logged but don't stop other jobs

**Retry Logic**:
- Exponential backoff for API calls
- 3 retry attempts for market data
- Rate limit respect with intelligent delays

### Performance Metrics
**Tracked Metrics**:
- Job completion rates and timing
- API response times and failure rates
- Content generation success rates
- Database query performance
- Memory and CPU usage

**Telegram Notifications**:
- **Real-time alerts** for job failures
- **Hourly heartbeats** with system status
- **Daily summaries** with performance metrics
- **Critical errors** with immediate alerts

---

## Failure Modes

### Known Issues/Failure Points

1. **Market Data Service Dependency**
   - **Issue**: HTTP server depends on Market Data Service (port 8001)
   - **Impact**: Website news feed becomes unavailable
   - **Recovery**: Automatic retry with fallback to cached data

2. **External API Rate Limits**
   - **Issue**: Twitter/Notion/Azure OpenAI rate limiting
   - **Impact**: Content publishing delays
   - **Recovery**: Intelligent backoff, queue management

3. **Database Connection Issues**
   - **Issue**: PostgreSQL connectivity problems
   - **Impact**: Content logging and headline pipeline failures
   - **Recovery**: Connection pooling, automatic reconnection

4. **GPT Content Generation Failures**
   - **Issue**: Azure OpenAI service issues or rate limits
   - **Impact**: Poor quality or missing content
   - **Recovery**: Fallback templates, retry logic

### Recovery Procedures

**Container Management**:
```bash
# Check container status
docker ps | grep htd-agent
docker-compose ps htd-agent

# View logs
docker logs htd-agent
docker-compose logs htd-agent

# Restart container
docker-compose restart htd-agent

# Rebuild and restart
docker-compose up -d --build htd-agent
```

**Full Stack Recovery**:
```bash
# Restart entire stack
docker-compose down
docker-compose up -d

# Check service dependencies
docker-compose logs postgres
docker-compose logs market-data-service
```

### Critical Dependencies
1. **PostgreSQL Container** - Core data storage (`postgres:15`)
2. **Market Data Service Container** - Real-time price data (`market-data-service:8001`)
3. **Redis Container** - Caching layer (`redis:6379`)
4. **Docker Network** - Internal service communication (`production-network`)
5. **External APIs** - Twitter, Notion, Azure OpenAI
6. **Environment Configuration** - Mounted env files and container variables

---

## Recent Changes

### Migration from Azure (Q3 2025)
- **Completed**: Full migration from Azure App Service to Docker containerized deployment
- **Updated**: Database connection to containerized PostgreSQL with SSL disabled for internal network
- **Modified**: Container-based process management replacing Supervisor
- **Enhanced**: Docker Compose orchestration with multi-service stack integration
- **Improved**: Service discovery via container networking instead of localhost connections

### Container Architecture Implementation
- **Added**: Dockerfile with Python 3.11-slim base image
- **Implemented**: Docker Compose integration with service dependencies
- **Enhanced**: Environment variable management via mounted config files
- **Optimized**: Internal network communication between containerized services

### Architecture Improvements
- **Added**: Market Data Service integration for unified data access
- **Improved**: Error handling and retry logic across all services
- **Enhanced**: Telegram notification system with rich status reporting
- **Optimized**: Database queries and connection pooling

### Content Generation Enhancements
- **Upgraded**: GPT-4 integration with institutional prompting
- **Added**: Advanced sentiment analysis with multi-asset scoring
- **Improved**: Content diversity tracking via Notion memory
- **Enhanced**: Market data enrichment with real-time cashtag pricing

---

## Upcoming Planned Changes

### Short-Term (Next 4 weeks)
- **Container Optimization**: Resource allocation tuning and health checks
- **Monitoring Enhancement**: Container metrics integration with logging stack
- **Network Security**: Internal container network security hardening  
- **Backup Strategy**: Automated container volume backup procedures

### Medium-Term (1-3 months)
- **Kubernetes Migration**: Preparation for container orchestration platform
- **Service Mesh**: Implementation of advanced container networking
- **CI/CD Pipeline**: Automated container builds and deployments
- **Observability**: Distributed tracing and container monitoring

### Long-Term (3+ months)
- **Horizontal Scaling**: Container deployment with Docker/Kubernetes
- **Event-Driven Architecture**: Message queue integration (RabbitMQ/Kafka)
- **Advanced AI**: Machine learning-based content optimization
- **Multi-Platform**: Expansion to LinkedIn and other social platforms

---

## Quick Reference

### Essential Commands
```bash
# Container status and management
docker ps | grep htd-agent
docker-compose ps htd-agent
docker-compose logs htd-agent
docker-compose restart htd-agent

# View real-time logs
docker logs -f htd-agent
docker-compose logs -f htd-agent

# Access container shell for debugging
docker exec -it htd-agent bash

# Full stack management
docker-compose up -d
docker-compose down
docker-compose up -d --build htd-agent

# Test HTTP endpoints (from host)
curl http://localhost:3002/health
curl http://localhost:3002/hedgefund-news-data

# Database connectivity test (inside container)
docker exec htd-agent python -c "
from services.database_service import DatabaseService
from config.settings import DATABASE_CONFIG
db = DatabaseService(DATABASE_CONFIG)
print('✅ Database connected' if db.get_connection() else '❌ Database failed')
"

# Environment debugging
docker exec htd-agent env | grep -E "(DB_|MARKET_|TWITTER_|NOTION_)"
```

### Key Configuration Files
- **Docker Compose**: `docker-compose.yaml` (full stack orchestration)
- **Dockerfile**: `HTD-Research-Agent/Dockerfile`
- **Environment Files**: 
  - `./config/secrets/api-keys.env`
  - `./config/services/htd-research.env`
- **Application Config**: `config/settings.py` (inside container)

### Container Network Information
- **Internal Network**: `production-network` (Docker bridge)
- **Service Discovery**: Via container names (e.g., `postgres`, `market-data-service`)
- **Port Mapping**: Host `3002` → Container `3002`
- **Volume Mounts**: `./HTD-Research-Agent/logs:/app/logs`

### Emergency Contacts
- **Telegram Alerts**: Automated notifications to configured chat
- **Log Analysis**: All errors logged with correlation IDs
- **System Health**: Available via `/health` endpoint and hourly heartbeats