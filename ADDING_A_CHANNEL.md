# Adding a fifth channel (e.g. Amazon)

The core never names a channel. A new one needs:

1. **An adapter class** with `verifyWebhook`, `normalize` (channel payload → canonical order, channel SKU → our SKU), `pollOrders` (polling channels only) and `pushStock(rows)`. Requests reach it through the generic `/webhooks/:channel` route, or through the poller.
2. **A config row:** rate limit, batch size, polling interval, BC customer code, and the channel's *blind window* (how late we hear of its sales plus how late it hears of ours). The blind window sets its buffers automatically: `buffer ≈ SKU's total peak sales/min × blind window`.
3. **`channel_listing` rows** for every SKU, with `published_qty` left empty. Enable the channel and the normal pusher does the first full sync, riskiest SKUs first. Run it in shadow mode first (compute and log, don't push).

Orders, inventory, the ledger, the outbox, the ERP worker and the ops screen don't change. One limit: buffers are per channel, but there's no per-channel *allocation* of scarce stock. Giving one channel a guaranteed share needs a new rule.
