# One image for api and worker (different start commands). No signer in this image.
FROM node:22-bookworm-slim
WORKDIR /app
RUN corepack enable
COPY . .
RUN pnpm install --frozen-lockfile
ENV NODE_ENV=production MODE=PAPER LIVE_ENABLED=false
USER node
CMD ["pnpm", "--filter", "@solbot/api", "start"]
