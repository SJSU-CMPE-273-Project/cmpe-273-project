# Same shape as the SMS adapter's image: tests run in their own stage, and the
# runtime image holds only production dependencies (OWASP: minimal attack surface).

# ---- deps (production only) ----------------------------------------------
FROM node:22-alpine AS deps
WORKDIR /app
COPY package.json package-lock.json ./
RUN npm ci --omit=dev --no-audit --no-fund

# ---- test (docker build --target test .) ---------------------------------
FROM deps AS test
COPY . .
CMD ["npm", "test"]

# ---- runtime -------------------------------------------------------------
FROM node:22-alpine
WORKDIR /app
COPY --from=deps /app/node_modules ./node_modules
COPY package.json index.js ./
COPY src ./src

# Run as the image's non-root `node` user (least privilege). Files stay owned
# by root, so the app can't modify its own code. Nothing is written to disk;
# logs go to stdout, so the container is stateless (ephemeral storage only).
USER node

ENV NODE_ENV=production PORT=8080
EXPOSE 8080

HEALTHCHECK --interval=10s --timeout=3s --retries=3 \
  CMD node -e "fetch('http://127.0.0.1:8080/health').then(r => process.exit(r.ok ? 0 : 1), () => process.exit(1))"

# node directly (not npm start), so SIGTERM from `docker stop` reaches the app.
CMD ["node", "index.js"]
