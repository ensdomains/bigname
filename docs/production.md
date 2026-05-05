# Production

The public single-host deployment shape. Current production hostname: `bigname.taytems.xyz`.

## Public edge

Public traffic terminates at Caddy (`docker-compose.public.yml`, `docker/caddy/Caddyfile`). Caddy forwards to the internal API at `api:3000`.

Exposed routes (read-only):

- `GET /docs`
- `GET /openapi.json`
- `GET /healthz`
- `GET /v1/...`

There are no public admin or mutation routes. Worker, migration, PostgreSQL, MinIO, and indexer control surfaces aren't routed through Caddy.

## Environment

Server environment from `.env.server` plus these:

```sh
BIGNAME_IMAGE=ghcr.io/tateb/bigname:<tag>
BIGNAME_API_HOST=127.0.0.1
BIGNAME_API_PORT=3000
BIGNAME_PUBLIC_SITE_ADDRESS=api.example.com
BIGNAME_PUBLIC_HTTP_PORT=80
BIGNAME_PUBLIC_HTTPS_PORT=443
```

`BIGNAME_API_HOST=127.0.0.1` keeps direct host access on localhost only. Public access goes through Caddy on 80/443.

For a temporary HTTP-only deployment before DNS is ready:

```sh
BIGNAME_PUBLIC_SITE_ADDRESS=:80
```

When `BIGNAME_PUBLIC_SITE_ADDRESS` is a hostname with public DNS pointing at the server, Caddy automatically obtains and renews TLS certificates.

## Start

```sh
docker compose --env-file .env.server \
  -f docker-compose.server.yml \
  -f docker-compose.public.yml \
  up -d
```

For a local image build on a server checkout, replace `BIGNAME_IMAGE` with `bigname:local` in the environment.

## Verify

Internal:

```sh
curl -fsS http://127.0.0.1:3000/healthz
```

Public edge:

```sh
curl -fsS -I http://127.0.0.1/docs
curl -fsS -I http://127.0.0.1/openapi.json
```

For hostname/TLS deployments, replace `127.0.0.1` with the public hostname and `http` with `https`.

## Operations

- Keep PostgreSQL and MinIO unexposed at the host/network edge.
- Keep JSON-RPC providers reachable only from the containers that need them.
- Use host firewall or cloud security groups to allow public `80/tcp` and `443/tcp`. Allow `443/udp` for HTTP/3. Don't publish database, object-store, or execution-node admin ports.
- Caddy data lives in the `caddy-data` Docker volume. Preserve it across container recreates so certificate state survives restarts.
- Caddy sends HSTS and advertises HTTP/3 when the UDP port is published. The `/docs` page and OpenAPI JSON are cacheable for a short window; `v1` API responses are not edge-cached by this configuration.
