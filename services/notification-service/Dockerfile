# Multi-stage build for Notification Service (Node.js/TypeScript)

# Stage 1: Dependencies
FROM node:20-alpine AS deps
WORKDIR /app

COPY services/notification-service/package.json ./
RUN npm ci --only=production && npm cache clean --force

# Stage 2: Builder
FROM node:20-alpine AS builder
WORKDIR /app

COPY services/notification-service/package.json ./
RUN npm ci && npm cache clean --force

COPY services/notification-service/src ./src
COPY services/notification-service/tsconfig.json ./tsconfig.json

RUN npm run build

# Stage 3: Runtime
FROM node:20-alpine AS runtime

RUN apk --no-cache add ca-certificates tzdata

WORKDIR /app

RUN addgroup -g 1001 -S nodejs && \
    adduser -S nodejs -u 1001

COPY --from=deps --chown=nodejs:nodejs /app/node_modules ./node_modules
COPY --from=deps --chown=nodejs:nodejs /app/package.json ./package.json
COPY --from=builder --chown=nodejs:nodejs /app/dist ./dist

USER nodejs

EXPOSE 8084

HEALTHCHECK --interval=15s --timeout=5s --start-period=15s --retries=3 \
    CMD wget -qO- http://localhost:8084/health || exit 1

ENTRYPOINT ["node", "dist/index.js"]
