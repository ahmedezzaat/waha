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
- `GET /api/analytics/overview` - group counts and message totals (today, 7d, 30d, all time)
- `GET /api/analytics/top-groups?days=7&limit=20&order=desc|asc` - busiest or quietest tracked groups
- `GET /api/analytics/groups/:id/top-senders?days=30&limit=20` - most active senders in a group
- `GET /api/analytics/keywords?words=a,b&days=30&group_id=` - keyword matches in total, per day and per group

- `GET|POST /api/agents`, `PATCH|DELETE /api/agents/:id` - agents (name + phone with country code). POST accepts `{"agents":[...]}` for bulk
- `GET /api/analytics/agents` - per agent: groups in total, solo (only agent in the group), shared, and who they share with; plus group totals
- `GET /api/analytics/agents/:id/groups?type=solo|shared` - the groups of one agent
- `GET /api/analytics/groups-without-agents` - groups where none of the agents is a member
- `POST /api/sync`, `GET /api/sync/status` - pull groups, names and members from WAHA now (also runs every `SYNC_HOURS`)

Group members come from WAHA's group list, where members are hidden WhatsApp IDs (`@lid`).
An agent's ID is learned from messages that carry both the ID and the phone number, so an
agent is only identified after writing once in any monitored group (`id_known` in the API).
Until then their group counts are under-reported, and other agents' "solo" counts can be too high.
`PATCH /api/agents/:id` with `{"lid": "...@lid"}` sets the ID by hand.

Overview and top-groups use a daily totals table that is kept after retention
deletes old messages. Top senders and keywords read stored messages, so they only
cover the retention window.

The API calls need `Authorization: Bearer <READ_API_KEY>`. Group ids contain `@`,
so URL-encode them (`120363@g.us` becomes `120363%40g.us`).

Environment: `DATABASE_URL`, `WAHA_HMAC_KEY`, `READ_API_KEY` (required);
`CRM_URL`, `CRM_API_KEY` (optional, enable lead creation for new 1:1 contacts);
`STORE_DIRECT_CHATS` (default `false`: only group messages are stored; set `true` to also store 1:1 chats);
`RETENTION_DAYS` (default 90); `ANALYTICS_TZ` (default `Africa/Cairo`, used for daily buckets);
`WAHA_API_KEY`, `WAHA_URL` (default `http://waha:3000`), `WAHA_SESSION` (default `default`), `SYNC_HOURS` (default 6) for the group sync; `PORT` (default 4000).
