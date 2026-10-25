# Pseudo-code: stock reservation (question 4)

TypeScript-flavoured pseudo-code for the order processor in the `worker` service. Each handler runs inside **one Postgres transaction at READ COMMITTED**. `tx` is that transaction. Helpers such as `appendLedger` and `setLine` are single-row inserts or updates.

```ts
// ORDER_CREATED: reserve each line or mark it SHORT. Idempotent, and safe under concurrency.
async function onOrderCreated(e: CanonicalOrder, tx: Tx) {
  // 1. Serialize all events of this order (CREATED, CANCELLED, ...), even before its row exists.
  await tx.query(`SELECT pg_advisory_xact_lock(hashtext($1))`, [`${e.channel}:${e.externalOrderId}`]);

  // 2. Business-level dedupe. A replay, or a CANCELLED that arrived first (tombstone), returns no row.
  const order = await tx.maybeOne(
    `INSERT INTO orders (order_no, channel, external_order_id, status, last_event_time)
     VALUES (next_order_no(), $1, $2, 'NEW', $3)
     ON CONFLICT (channel, external_order_id) DO NOTHING
     RETURNING id, order_no`, [e.channel, e.externalOrderId, e.eventTime]);
  if (!order) return;
  const lines = await insertLines(tx, order.id, e.items);  // SKUs already mapped by the channel adapter

  // 3. Reserve line by line, in SKU order, so two multi-line orders can't deadlock.
  //    Check and increment are ONE statement. Under READ COMMITTED, a waiting UPDATE re-evaluates
  //    its WHERE against the row the lock holder committed, so the last unit is taken exactly once.
  const reservedIds: string[] = [];
  for (const l of sortBy(lines, 'sku')) {
    const row = await tx.maybeOne(
      `UPDATE inventory SET reserved = reserved + $3, updated_at = now()
        WHERE sku = $1 AND location_id = $2 AND on_hand - reserved >= $3
        RETURNING on_hand, reserved`, [l.sku, l.locationId, l.qty]);
    if (row) {
      await appendLedger(tx, { lineId: l.id, reason: 'RESERVE', dReserved: +l.qty, after: row }); // UNIQUE (order_line_id, reason)
      await setLine(tx, l.id, 'RESERVED');
      reservedIds.push(l.id);
    } else {
      await appendLedger(tx, { lineId: l.id, reason: 'SHORTAGE', dReserved: 0 });  // audit only
      await setLine(tx, l.id, 'SHORT');                                            // joins the backorder queue
    }
  }

  // 4. Lines confirm independently. Reserved lines go to the ERP now (an outbox row in this same transaction).
  //    A SHORT line waits alone, so it never holds the order's other stock hostage.
  const allReserved = reservedIds.length === lines.length;
  await setOrder(tx, order.id, allReserved ? 'CONFIRMED' : reservedIds.length ? 'PARTIAL' : 'SHORT');
  if (reservedIds.length) {
    await tx.query(`INSERT INTO erp_outbox (ext_doc_no, kind, order_id, line_ids) VALUES ($1, 'CREATE', $2, $3)
                    ON CONFLICT (ext_doc_no, kind) DO NOTHING`, [order.order_no, order.id, reservedIds]);
  }
  await tx.query(`NOTIFY inventory_changed`);  // delivered only if this transaction commits
}

// ORDER_CANCELLED before dispatch: release reserved lines, then offer the freed units to waiting orders.
// (A channel cancelling a single line takes the same path for that one line.)
async function onOrderCancelled(e: CanonicalOrder, tx: Tx) {
  await tx.query(`SELECT pg_advisory_xact_lock(hashtext($1))`, [`${e.channel}:${e.externalOrderId}`]);
  const order = await markCancelled(tx, e);  // inserts a CANCELLED tombstone if CREATED never arrived
  if (!order.wasOpen) return;                // already cancelled, or shipped (that is a return, not a cancel)

  for (const l of sortBy(await linesWithStatus(tx, order.id, 'RESERVED'), 'sku')) {
    const row = await tx.one(
      `UPDATE inventory SET reserved = reserved - $3, updated_at = now()
        WHERE sku = $1 AND location_id = $2
        RETURNING on_hand, reserved`, [l.sku, l.locationId, l.qty]);
    await appendLedger(tx, { lineId: l.id, reason: 'RELEASE', dReserved: -l.qty, after: row }); // released once
    await setLine(tx, l.id, 'RELEASED');
    await allocateBackorders(tx, l.sku, l.locationId);
  }
  await setLines(tx, order.id, { from: 'SHORT', to: 'RELEASED' });  // its waiting lines leave the backorder queue

  // ERP side. A CREATE that no worker has picked up (no attempt, no lease) is cancelled in place.
  // Anything else may already be in BC, so queue a CANCEL that runs once that CREATE settles.
  // The ERP worker claims rows with FOR UPDATE SKIP LOCKED, so this UPDATE and a claim can't both win.
  await tx.query(
    `UPDATE erp_outbox SET status = 'CANCELLED'
      WHERE order_id = $1 AND kind = 'CREATE' AND status = 'PENDING' AND attempts = 0 AND lease_until IS NULL`,
    [order.id]);
  await tx.query(
    `INSERT INTO erp_outbox (ext_doc_no, kind, order_id)
     SELECT ext_doc_no, 'CANCEL', order_id FROM erp_outbox
      WHERE order_id = $1 AND kind = 'CREATE' AND status <> 'CANCELLED'
     ON CONFLICT (ext_doc_no, kind) DO NOTHING`, [order.id]);
  await tx.query(`NOTIFY inventory_changed`);
}

// Freed units (cancel, QC-passed return, goods receipt) go to the oldest SHORT lines before any channel sees them.
async function allocateBackorders(tx: Tx, sku: string, locationId: string) {
  const waiting = await tx.many(
    `SELECT ol.id, ol.line_no, ol.qty, o.id AS order_id, o.order_no, o.channel, o.external_order_id
       FROM order_line ol JOIN orders o ON o.id = ol.order_id
      WHERE ol.sku = $1 AND ol.location_id = $2 AND ol.alloc_status = 'SHORT'
        AND o.status IN ('SHORT', 'PARTIAL')
      ORDER BY o.created_at`, [sku, locationId]);
  for (const l of waiting) {
    // Never block on another order's lock, so there are no deadlocks. A busy order is caught by the 1-min sweep.
    const locked = await tx.one(`SELECT pg_try_advisory_xact_lock(hashtext($1)) AS ok`,
                                [`${l.channel}:${l.external_order_id}`]);
    if (!locked.ok || (await lineStatus(tx, l.id)) !== 'SHORT') continue;  // re-check under that order's lock
    const row = await tx.maybeOne(
      `UPDATE inventory SET reserved = reserved + $3, updated_at = now()
        WHERE sku = $1 AND location_id = $2 AND on_hand - reserved >= $3
        RETURNING on_hand, reserved`, [sku, locationId, l.qty]);
    if (!row) continue;  // this line doesn't fit; a smaller, later line still might
    await appendLedger(tx, { lineId: l.id, reason: 'RESERVE', dReserved: +l.qty, after: row });
    await setLine(tx, l.id, 'RESERVED');
    // The filled line becomes its own follow-up ERP document, with its own idempotency key.
    await tx.query(`INSERT INTO erp_outbox (ext_doc_no, kind, order_id, line_ids) VALUES ($1, 'CREATE', $2, $3)
                    ON CONFLICT (ext_doc_no, kind) DO NOTHING`, [`${l.order_no}-${l.line_no}`, l.order_id, [l.id]]);
    await refreshOrderStatus(tx, l.order_id);  // -> CONFIRMED once no line is SHORT
  }
}
```

## Website checkout hold (v1.1)

The website is the one channel we own, so it can reserve *before* taking payment. A hold is an ordinary reservation with an expiry. It uses the same conditional `UPDATE`, so it races safely with marketplace orders.

```ts
// Checkout starts: hold every line or tell the customer "out of stock". Nothing is charged yet.
async function holdCheckout(c: Checkout, tx: Tx) {
  for (const l of sortBy(c.lines, 'sku')) {
    const row = await tx.maybeOne(
      `UPDATE inventory SET reserved = reserved + $3, updated_at = now()
        WHERE sku = $1 AND location_id = $2 AND on_hand - reserved >= $3
        RETURNING on_hand, reserved`, [l.sku, l.locationId, l.qty]);
    if (!row) throw new OutOfStock(l.sku);   // the transaction rolls back, so nothing stays held
    await tx.query(`INSERT INTO holds (checkout_id, sku, location_id, qty, expires_at)
                    VALUES ($1, $2, $3, $4, now() + interval '10 minutes')`, [c.id, l.sku, l.locationId, l.qty]);
    await appendLedger(tx, { holdId: c.id, sku: l.sku, reason: 'HOLD', dReserved: +l.qty, after: row });
  }
}

// The website's ORDER_CREATED for that checkout turns each live hold into the order line's reservation
// (no second UPDATE). If the hold already expired, onOrderCreated reserves again, and a miss becomes SHORT.

// Sweeper, every minute: give back holds whose customer never paid.
async function expireHolds(tx: Tx) {
  for (const h of await tx.many(`SELECT * FROM holds WHERE expires_at < now() AND status = 'HELD'
                                 ORDER BY sku FOR UPDATE SKIP LOCKED`)) {
    const row = await tx.one(`UPDATE inventory SET reserved = reserved - $3
                              WHERE sku = $1 AND location_id = $2 RETURNING on_hand, reserved`,
                             [h.sku, h.location_id, h.qty]);
    await tx.query(`UPDATE holds SET status = 'EXPIRED' WHERE id = $1`, [h.id]);
    await appendLedger(tx, { holdId: h.checkout_id, sku: h.sku, reason: 'EXPIRE', dReserved: -h.qty, after: row });
  }
}
```

- **What it buys:** the website can never oversell, which matters because it's the channel that sells the last units.
- **What it costs:** the website team changes its checkout, and checkout now calls us. If our service is down or slow (more than 300 ms), checkout *fails open*: it proceeds without a hold and falls back to today's flow.

**Why this is safe**

- **Last unit, two buyers:** both transactions run the same conditional `UPDATE`. The second one waits for the first one's row lock, then re-checks `on_hand - reserved >= qty` against the committed row and updates 0 rows. No read-then-write gap exists, so no isolation level stronger than READ COMMITTED is needed.
- **Duplicates:** a replayed or concurrent duplicate CREATED hits `ON CONFLICT DO NOTHING`. A concurrent inserter waits on the unique index, then takes the conflict path. The ledger's `UNIQUE (order_line_id, reason)` blocks any double RESERVE or RELEASE.
- **Deadlocks:** inventory rows are always locked in SKU order. Other orders' locks are only ever *tried*, never waited on.
- **Cancel vs. an in-flight ERP post:** only a CREATE that was never claimed is cancelled in place. Any claimed CREATE gets a CANCEL that runs after it settles. The ERP worker looks each `ext_doc_no` up in BC before any retry, and no sooner than 10 min after the previous attempt (BC's request timeout is 8 min), so a CREATE that "timed out" but still committed is found rather than posted twice.
- **Hot-row cost:** the inventory row lock is taken late and held only until commit (a few ms), so one SKU can absorb 100+ reservations/s.
- **Crash at any point:** the transaction rolls back. The inbox event stays unprocessed and is retried, so nothing is half-applied.
