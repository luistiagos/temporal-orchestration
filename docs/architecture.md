# Architecture

## Services

This project exposes two runtime processes:

- API process: receives HTTP requests from the existing backend or future apps.
- Worker process: polls Temporal task queues and runs workflows/activities.

Temporal Server stores workflow history, timers, retry state, and signals.

## Remarketing flow

Each lead/campaign pair becomes a workflow:

```text
workflow_id = remarketing-{tenant_id}-{campaign_id}-{lead_id}
```

The workflow:

1. Waits until each step is due.
2. Checks whether the lead purchased.
3. Dispatches the configured email or WhatsApp step.
4. Records the result in workflow state.
5. Stops if the lead purchased, the workflow was canceled, or the sequence ended.

If a provider call times out after crossing the side-effect boundary, the
activity returns `unknown`. The workflow then waits for manual confirmation or
manual retry. This avoids blind retries that can duplicate customer messages.

## WhatsApp flow

Each conversation becomes a workflow:

```text
workflow_id = whatsapp-{tenant_id}-{canonical_phone}
```

Incoming webhook messages should be sent to the API. The workflow keeps a
pending message buffer, waits a durable debounce window, and then processes the
batch through an activity.

The current WhatsApp activity is intentionally thin. It is ready for the second
migration phase, when the chatbot core and WhatsApp sender can move behind the
Temporal worker.

## Reuse by other projects

All workflow inputs include `tenant_id`, `campaign_id`, and `metadata`. A second
application can reuse the same worker by sending its own tenant id and adapters.

