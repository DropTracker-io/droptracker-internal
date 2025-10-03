<a id="readme-top"></a>

<br />
<div align="center">
  <a href="https://github.com/droptracker-io/droptracker-internal">
    <img src="https://www.droptracker.io/droptracker-small.gif" alt="Logo" width="80" height="80">
  </a>

<h3 align="center">DropTracker.io | Internal API & Discord Bots</h3>

  <p align="center">
    OSRS loot and achievement tracking with a Discord-first experience, a Quart API, and MySQL-backed persistence.
    <br><br>
    &middot;
    <a href="https://github.com/droptracker-io/droptracker-internal/issues/new?labels=bug&template=bug-report---.md">Report a Bug</a>
    &middot;
    <a href="https://github.com/droptracker-io/droptracker-internal/issues/new?labels=enhancement&template=feature-request---.md">Request Feature</a>
  </p>
</div>


<details>
  <summary>Table of Contents</summary>
  <ol>
    <li>
      <a href="#about-the-project">About The Project</a>
      <ul>
        <li><a href="#built-with">Built With</a></li>
        <li><a href="#repository-layout">Repository Layout</a></li>
      </ul>
    </li>
    <li>
      <a href="#getting-started">Getting Started</a>
      <ul>
        <li><a href="#prerequisites">Prerequisites</a></li>
        <li><a href="#installation">Installation</a></li>
        <li><a href="#environment-variables">Environment Variables (.env)</a></li>
        <li><a href="#database--alembic">Database & Alembic</a></li>
      </ul>
    </li>
    <li><a href="#running-services">Running Services</a></li>
    <li><a href="#contributing">Contributing</a></li>
    <li><a href="#license">License</a></li>
  </ol>
  </details>


## About The Project

DropTracker.io powers OSRS loot, achievements, and leaderboards across Discord communities. This repository contains:

- A primary Discord bot that also hosts a local Quart web app.
- A standalone API server for programmatic access and dashboards.
- A webhook bot optimized for RuneLite-style ingestion and reaction workflows.
- MySQL storage via SQLAlchemy, with Alembic migrations.
- Optional Redis-backed caches and utilities for Cloudflare, GitHub, WOM, and more.

### Built With

- Python 3.10+
- Quart (ASGI), Hypercorn
- SQLAlchemy, Alembic
- PyMySQL (MySQL)
- Redis
- discord-py-interactions
- PyJWT / Quart-JWT-Extended


### Repository Layout

- `main.py`: Main Discord bot and embedded Quart app (served via Hypercorn).
- `new_api.py`: Standalone Quart API server.
- `bots/webhook_bot.py`: Webhook-focused Discord bot.
- `api/worker.py`: Quart blueprint(s) and task endpoints.
- `db/models/`: SQLAlchemy models and engine/session setup (`db/models/base.py`).
- `db/ops.py`: High-level database operations and workflows.
- `services/`: Bot services (notifications, message handling, points, etc.).
- `utils/`: Shared utilities (WOM client, embeds, logger, Redis client, etc.).
- `lootboard/`: Lootboard generators and utilities.
- `monitor/`: Linux-only service supervisor (GNU screen-based CLI).


## Getting Started

### Prerequisites

- Python 3.10 or newer
- MySQL 8.x (local) with access to two schemas: `data` and `xenforo`
- Redis (optional but recommended)
- Git


### Installation

Clone and set up a virtual environment.

```powershell
git clone https://github.com/droptracker-io/droptracker-internal.git
cd droptracker-internal

# Windows (PowerShell)
python -m venv .venv
.\.venv\Scripts\Activate
python -m pip install --upgrade pip
pip install -r requirements.txt
```

On Linux/macOS:

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
pip install -r requirements.txt
```


### Environment Variables

Open and edit the `.env.example` file, replacing the variables with your own.
The minimal required variables are:
- DB_USER
- DB_PASS
- APP_SECRET_KEY (use a keygen)
- JWT_TOKEN_KEY (use a gen)

- BOT_TOKEN
- WEBHOOK_TOKEN
- DISCORD_GUILD_ID
- API_PORT

```env
# App mode
STATUS=dev

# Database (MySQL on localhost)
DB_USER=your_mysql_user
DB_PASS=your_mysql_password

# Secrets
APP_SECRET_KEY=replace_me
JWT_TOKEN_KEY=replace_me

# Discord tokens
BOT_TOKEN=your_production_bot_token
DEV_TOKEN=your_dev_bot_token
WEBHOOK_TOKEN=your_webhook_bot_token
DEV_WEBHOOK_TOKEN=optional_dev_webhook_token

# Discord config
DISCORD_GUILD_ID=your_primary_guild_id
TARGET_GUILDS=comma,separated,guild,ids   # optional
PROCESS_NITRO_BOOSTS=false
SHOULD_PROCESS_REACTIONS=false

# API
API_PORT=31323

# Optional integrations
LOGGER_TOKEN=optional
WOM_API_KEY=optional
CLOUDFLARE_API_TOKEN=optional
CLOUDFLARE_ZONE_ID=optional
CLOUDFLARE_RECORD_NAMES=optional.com,comma,separated
```

Notes:
- `db/models/base.py` expects MySQL on `localhost` and two schemas: `data` and `xenforo`.
- Redis will default to `127.0.0.1:6379` and uses `DB_PASS` as the Redis password if set.


### Database & Alembic

1) Create local schemas and a user in MySQL:

```sql
CREATE DATABASE data CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
CREATE DATABASE xenforo CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
CREATE USER 'dt'@'localhost' IDENTIFIED BY 'strong_password_here';
GRANT ALL PRIVILEGES ON data.* TO 'dt'@'localhost';
GRANT ALL PRIVILEGES ON xenforo.* TO 'dt'@'localhost';
FLUSH PRIVILEGES;
```

2) Copy and edit Alembic config:

```bash
cp alembic.ini.template alembic.ini   # use copy on Windows
```

Open `alembic.ini` and set:

```
prepend_sys_path = .
sqlalchemy.url = mysql+pymysql://DB_USER:DB_PASS@localhost:3306/data
```

3) Initialize Alembic (if the `alembic/` directory does not exist yet):

```bash
alembic init alembic
```

In `alembic/env.py`, import your model metadata so autogenerate works:

```python
from db.models.base import Base
target_metadata = Base.metadata
```

4) Generate and apply migrations:

```bash
alembic revision --autogenerate -m "init"
alembic upgrade head
```

Tip (Windows): run Alembic from the repo root with the venv activated. The provided `prepend_sys_path = .` ensures the project root is on `sys.path`.


## Running Services

All commands assume your virtual environment is activated and you are in the repo root.

### Main Discord Bot + Embedded Web App

```bash
python main.py
```

- Uses `BOT_TOKEN` in production mode, or `DEV_TOKEN` when `STATUS=dev`.
- Hosts a local Quart app via Hypercorn on `127.0.0.1:8080` by default.

### Standalone API Server

```bash
python new_api.py
```

- Binds to `127.0.0.1:${API_PORT}` (default `31323`).

### Webhook Bot

```bash
python -m bots.webhook_bot
```

- Uses `WEBHOOK_TOKEN` in production mode, or `DEV_WEBHOOK_TOKEN` when `STATUS=dev`.

### Optional: Lootboard Generator

```bash
python _board_generator.py
```

### Optional (Linux): Process Supervisor

The `monitor/` module provides a GNU screen-based supervisor. Linux-only.

```bash
python -m monitor list
python -m monitor start core|api|webhooks|lootboards
python -m monitor status --json
python -m monitor logs core -n 200
```


## Contributing

Issues and PRs are welcome. Please open an issue first for substantial changes. When adding new models, prefer Alembic migrations (`alembic revision --autogenerate`) and keep `db/models/` as the single source of truth for schema.


## License

Distributed under the project license. See `LICENSE.txt` for more information.


Project Link: https://github.com/droptracker-io/droptracker-internal
