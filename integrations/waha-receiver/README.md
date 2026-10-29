# WAHA receiver

Receives WAHA `message` webhooks, stores messages (1:1 and group) in Postgres,
creates a CRM lead for each new 1:1 contact, and serves stored messages through
a small read API protected by `READ_API_KEY`.

Endpoints (all JSON):

- `POST /webhook?line=NAME` - WAHA webhook, signed with HMAC (`WAHA_HMAC_KEY`)
- `GET /health`
- `GET /api/groups?limit&offset&q`
- `PATCH /api/groups/:id` with `{"name": "...", "tracked": true|false}`
- `GET /api/groups/:id/messages?limit&before&q`
- `DELETE /api/groups/:id/messages`
- `GET /api/chats/:phone/messages?limit&before&q`

The API calls need `Authorization: Bearer <READ_API_KEY>`. Group ids contain `@`,
so URL-encode them (`120363@g.us` becomes `120363%40g.us`).

Environment: `DATABASE_URL`, `WAHA_HMAC_KEY`, `READ_API_KEY` (required);
`CRM_URL`, `CRM_API_KEY` (optional); `RETENTION_DAYS` (default 90); `PORT` (default 4000).
