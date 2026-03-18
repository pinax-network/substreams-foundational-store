# Local Development Setup

This guide explains how to set up and run the foundational-store locally for development and testing.

## Prerequisites

- Docker and Docker Compose installed
- Go 1.24+ installed
- VS Code with Go extension (optional, for debugging)

## Quick Start

### 1. Start the Database

Start PostgreSQL and pgweb (web UI for Postgres):

```bash
docker-compose up -d
```

This will start:
- **PostgreSQL** on `localhost:5432`
  - Database: `foundational_store`
  - User: `fs`
  - Password: `devpassword`
- **pgweb** on `http://localhost:8081` (web interface to browse the database)

### 2. Verify Database is Running

Check the database is healthy:

```bash
docker-compose ps
```

You should see both services running. Visit http://localhost:8081 to access pgweb.

#### Using Command Line

```bash
go run ./cmd/foundational-store server \
  --dsn "postgres://fs:devpassword@localhost:5432/foundational_store?sslmode=disable" \
  --type-url "dex.foundational_store.v1.Pool" \
  --workers 100
```

With manifest (full setup):

```bash
go run ./cmd/foundational-store server \
  --dsn "postgres://fs:devpassword@localhost:5432/foundational_store?sslmode=disable" \
  --manifest-path /path/to/your/manifest.spkg \
  --type-url "dex.foundational_store.v1.Pool" \
  --output-module-name map_entries \
  --endpoint localhost:9000 \
  --plaintext \
  --cursor-file-path /tmp/state.cursor \
  --workers 100
```

## Database Management

### View Database Contents

Open http://localhost:8081 in your browser to use pgweb's web interface.

Or use psql:

```bash
docker-compose exec postgres psql -U fs -d foundational_store
```

### Reset Database (Start Fresh)

Stop and remove all data:

```bash
docker-compose down -v
```

Then start again:

```bash
docker-compose up -d
```

### View Database Logs

```bash
docker-compose logs -f postgres
```

### Stop Services

```bash
docker-compose stop
```

### Restart Services

```bash
docker-compose restart
```

## Testing the Batch Insert Optimization

To test the new batch insert performance:

1. Start the database: `docker-compose up -d`
2. Run the application with your manifest
3. Monitor performance in the logs
4. Check database activity in pgweb: http://localhost:8081

### Query to Check Entries

```sql
-- Count total entries
SELECT COUNT(*) FROM public.entries;

-- View recent entries
SELECT block_number, key, create_time
FROM public.entries
ORDER BY create_time DESC
LIMIT 10;

-- Check entries per block
SELECT block_number, COUNT(*) as entry_count
FROM public.entries
GROUP BY block_number
ORDER BY block_number DESC
LIMIT 20;
```

## Troubleshooting

### Port Already in Use

If port 5432 is already in use, edit `docker-compose.yml` and change:

```yaml
ports:
  - "5433:5432"  # Use 5433 on host instead
```

Then update your DSN to use port 5433.

### Connection Refused

Wait a few seconds for PostgreSQL to fully start. Check health:

```bash
docker-compose ps
```

### Database Schema Issues

The application automatically creates the schema on first run. If you need to manually inspect:

```bash
docker-compose exec postgres psql -U fs -d foundational_store -c "\dt public.*"
```

## Performance Monitoring

### Enable pprof

The application includes pprof on port 9999. Access it at:

- CPU profile: http://localhost:9999/debug/pprof/profile?seconds=30
- Heap profile: http://localhost:9999/debug/pprof/heap
- Goroutines: http://localhost:9999/debug/pprof/goroutine

### Monitor Connection Pool

Check active connections to PostgreSQL:

```sql
SELECT count(*) as connections, state
FROM pg_stat_activity
WHERE datname = 'foundational_store'
GROUP BY state;
```

## Development Workflow

1. **Make code changes**
2. **Stop the running app** (Ctrl+C or stop debugger)
3. **Restart** (F5 in VS Code or re-run command)
4. **Test changes**
5. **Reset database if needed**: `docker-compose down -v && docker-compose up -d`

## Clean Up

Remove everything (containers, volumes, networks):

```bash
docker-compose down -v
```
