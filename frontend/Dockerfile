### Stage 1: Build the React app
FROM node:20-alpine AS builder

WORKDIR /app

# Cache deps separately from source
COPY package*.json ./
RUN npm install

COPY . .

# VITE_API_BASE intentionally empty — nginx proxies all /api calls
# so the frontend and backend are same-origin (no CORS, no token in URL)
RUN VITE_API_BASE="" npm run build

### Stage 2: Serve with nginx
FROM nginx:1.27-alpine

# Remove default nginx config
RUN rm /etc/nginx/conf.d/default.conf

# Copy built app
COPY --from=builder /app/dist /usr/share/nginx/html

# nginx config is bind-mounted from root at runtime (see docker-compose)
EXPOSE 80

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD wget -qO- http://localhost:80/ || exit 1

CMD ["nginx", "-g", "daemon off;"]
