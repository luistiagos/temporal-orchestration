# Integration Plan

## Phase 1: Run Temporal separately

Deploy this project without changing `digitalstoregamesbackend`.

Recommended processes:

- Temporal Server or Temporal Cloud.
- One or more `dsg_temporal.worker` processes.
- One `dsg_temporal.api` process behind HTTPS.

## Phase 2: Move manageleads orchestration

Add a small caller in the backend later:

1. When a lead is created or detected by the planner, expand the
   `RemarketStore` / `RemarketSettings` rows into a `sequence` payload.
2. Call `POST /v1/remarketing/workflows`.
3. When a purchase is approved, call
   `POST /v1/remarketing/workflows/{workflow_id}/purchase`.
4. Stop using the cron planner for leads that already have Temporal workflows.

The sequence should include the legacy identifiers in `metadata`, for example:

```json
{
  "step_id": "remarket-store-123",
  "order": 1,
  "channel": "email",
  "subject": "Seu acesso ainda esta esperando",
  "template": "...",
  "delay_minutes": 30,
  "metadata": {
    "remarket_store_id": 123,
    "remarket_setting_id": 99,
    "outbox_id": 456
  }
}
```

## Phase 3: Add idempotency to legacy dispatch endpoints

The Temporal activity already sends `Idempotency-Key` and `X-Idempotency-Key`.
The backend should persist that key before sending email or WhatsApp.

This is the piece that prevents duplicates when an HTTP timeout happens after
the provider accepted the message.

## Phase 4: Move WhatsApp chatbot orchestration

Send every Evolution webhook message to:

```text
POST /v1/whatsapp/messages
```

The workflow id is based on the canonical phone/JID, so all messages from the
same customer are serialized in one durable workflow.

The later chatbot activity should own:

- message dedup by provider msg_id;
- durable debounce;
- conversation lock by workflow id;
- LLM response generation;
- outbound send idempotency;
- human handoff signals;
- retry/reconciliation for unknown sends.

