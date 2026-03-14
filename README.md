# VolGuard Intelligence Edition

Overnight options trading system for Indian markets (NIFTY).  
Quant engine + AI reasoning layer + FastAPI backend + React dashboard.

---

## Architecture

```
EC2 Instance
└── Docker Compose
    ├── volguard_backend  (FastAPI :8000 — internal only)
    └── volguard_frontend (nginx :80  — public)
          └── /api/* → proxy → backend
          └── /api/ws/* → proxy → backend WebSocket
```

---

## First-Time EC2 Setup

### 1. Prerequisites on EC2

```bash
# Install Docker
curl -fsSL https://get.docker.com | sh
sudo usermod -aG docker $USER
sudo systemctl enable docker
newgrp docker

# Install Docker Compose v2
sudo apt install -y docker-compose-plugin

# Install git and aws cli
sudo apt install -y git awscli
```

### 2. Clone and configure

```bash
sudo mkdir -p /opt/volguard
sudo chown $USER:$USER /opt/volguard
cd /opt/volguard

git clone https://github.com/YOUR_USERNAME/YOUR_REPO.git .

# Create env file from template
cp .env.example .env
nano .env   # Fill in all values
```

### 3. First deploy

```bash
cd /opt/volguard
docker compose build
docker compose up -d

# Watch logs
docker compose logs -f
```

### 4. Verify

```bash
# Backend health
curl http://localhost:8000/api/health

# Frontend (open in browser)
http://YOUR_EC2_PUBLIC_IP
```

---

## GitHub Actions CI/CD Setup

Add these secrets to your GitHub repo (Settings → Secrets → Actions):

| Secret | Value |
|--------|-------|
| `EC2_HOST` | Your EC2 public IP or domain |
| `EC2_USER` | `ubuntu` (or `ec2-user` for Amazon Linux) |
| `EC2_SSH_KEY` | Contents of your EC2 `.pem` private key |

Every push to `main` will automatically deploy.

---

## Daily Operations

### Token Update (every morning after Upstox approval)

After approving the 08:30 AM token request on your Upstox app:
1. Open the dashboard → SYSTEM tab
2. Paste the new token in **Daily Token Update**
3. Click **Update Token** — updates backend without restart

### Safe restart

```bash
cd /opt/volguard
docker compose down    # Volumes preserved
docker compose up -d
```

### ⚠️ NEVER run this in production

```bash
docker compose down -v   # DELETES all trade data and database
```

### View logs

```bash
docker compose logs -f volguard      # Backend logs
docker compose logs -f frontend      # nginx logs
```

---

## S3 Backup (recommended)

```bash
# Install on EC2 and add to crontab
crontab -e

# Add this line — backs up DB daily at 4:30 PM IST (11:00 UTC)
0 11 * * 1-5 docker exec volguard_backend sqlite3 /app/data/volguard.db ".backup '/app/data/volguard_backup.db'" && aws s3 cp /var/lib/docker/volumes/volguard_volguard_data/_data/volguard_backup.db s3://YOUR_BUCKET/volguard/$(date +\%Y\%m\%d).db
```

---

## Health Monitoring

```bash
# Telegram alert if backend goes down (add to crontab)
*/5 * * * * curl -sf http://localhost:8000/api/health > /dev/null || curl -s "https://api.telegram.org/bot${TELEGRAM_TOKEN}/sendMessage" -d "chat_id=${TELEGRAM_CHAT_ID}&text=🚨 VolGuard health check FAILED"
```

---

## Environment Variables

See `.env.example` for full reference.

**Required:**
- `UPSTOX_ACCESS_TOKEN` — daily Upstox token
- `UPSTOX_CLIENT_ID` + `UPSTOX_CLIENT_SECRET` — for daily token renewal

**Strongly recommended:**
- `TELEGRAM_TOKEN` + `TELEGRAM_CHAT_ID` — real-time alerts

**Optional (AI layer):**
- `GROQ_API_KEY` — free tier, preferred
- `ANTHROPIC_API_KEY` — fallback, ~$3-6/month

---

## Security Notes

- Port 8000 is **not exposed** publicly — nginx is the only entry point
- Upstox token travels as HTTP header, never as URL query param
- Run `sudo ufw allow 80` and `sudo ufw enable` — block everything else
- For HTTPS: `sudo apt install certbot && sudo certbot --nginx -d yourdomain.com`
