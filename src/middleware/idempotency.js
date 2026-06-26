// src/middleware/idempotency.js

'use strict';

const crypto = require('crypto');
const { pool } = require('../db');

// ─────────────────────────────────────────
// CONSTANTS
// ─────────────────────────────────────────

// How long a PENDING row must be before we treat it as crashed.
// Must exceed your p99 request processing time.
const PENDING_TIMEOUT_MS = parseInt(
  process.env.IDEMPOTENCY_PENDING_TIMEOUT_MS || '30000',
  10
);

// UUID v4 format: xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx
// where y is 8, 9, a, or b.
const UUID_V4_REGEX =
  /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;

// ─────────────────────────────────────────
// CANONICAL BODY HASH
// Sorts object keys recursively before serialising so that
// {"b":2,"a":1} and {"a":1,"b":2} produce the same hash.
// ─────────────────────────────────────────
function canonicalHash(body) {
  const canonical = sortKeysDeep(body);
  const json = JSON.stringify(canonical);
  return crypto.createHash('sha256').update(json).digest('hex');
}

function sortKeysDeep(value) {
  if (Array.isArray(value)) {
    return value.map(sortKeysDeep);
  }
  if (value !== null && typeof value === 'object') {
    return Object.keys(value)
      .sort()
      .reduce((acc, key) => {
        acc[key] = sortKeysDeep(value[key]);
        return acc;
      }, {});
  }
  return value;
}

// ─────────────────────────────────────────
// INSERT PENDING ROW
// Committed in its own connection (outside the wallet transaction)
// so it survives a crash mid-wallet-transaction.
// Returns the inserted row on success.
// Throws on unexpected DB errors.
// Returns null if a unique violation occurs (concurrent duplicate).
// ─────────────────────────────────────────
async function insertPendingKey({ idempotencyKey, endpoint, playerId, requestHash }) {
  try {
    const result = await pool.query(
      `INSERT INTO idempotency_keys
         (idempotency_key, endpoint, player_id, request_hash, status, locked_at)
       VALUES ($1, $2, $3, $4, 'PENDING', now())
       RETURNING *`,
      [idempotencyKey, endpoint, playerId, requestHash]
    );
    return result.rows[0];
  } catch (err) {
    // Postgres unique violation code = 23505
    if (err.code === '23505') {
      return null; // Concurrent duplicate — caller will re-SELECT
    }
    throw err;
  }
}

// ─────────────────────────────────────────
// FETCH EXISTING KEY
// ─────────────────────────────────────────
async function fetchExistingKey(idempotencyKey, endpoint) {
  const result = await pool.query(
    `SELECT * FROM idempotency_keys
     WHERE idempotency_key = $1 AND endpoint = $2`,
    [idempotencyKey, endpoint]
  );
  return result.rows[0] || null;
}

// ─────────────────────────────────────────
// MARK KEY FAILED
// Used when a stale PENDING row is detected.
// ─────────────────────────────────────────
async function markKeyFailed(id) {
  await pool.query(
    `UPDATE idempotency_keys
     SET status = 'FAILED'
     WHERE id = $1`,
    [id]
  );
}

// ─────────────────────────────────────────
// MARK KEY COMPLETED
// Called by the res.json interceptor after business logic succeeds.
// Stores the response body and status code for replay.
// ─────────────────────────────────────────
async function markKeyCompleted({ id, responseStatus, responseBody }) {
  await pool.query(
    `UPDATE idempotency_keys
     SET status          = 'COMPLETED',
         response_status = $2,
         response_body   = $3,
         completed_at    = now()
     WHERE id = $1`,
    [id, responseStatus, JSON.stringify(responseBody)]
  );
}

// ─────────────────────────────────────────
// DELETE FAILED KEY
// Clears a FAILED row so a fresh PENDING insert can succeed.
// ─────────────────────────────────────────
async function deleteKey(id) {
  await pool.query(
    `DELETE FROM idempotency_keys WHERE id = $1`,
    [id]
  );
}

// ─────────────────────────────────────────
// INTERCEPT RES.JSON
// Monkey-patches res.json so the response body and status code
// are captured and persisted before being sent to the client.
// This is the single enforcement point — business logic never
// needs to know about idempotency storage.
// ─────────────────────────────────────────
function interceptResponse(res, keyRecord) {
  const originalJson = res.json.bind(res);

  res.json = async function (body) {
    // Restore original immediately to prevent double-interception
    // if something calls res.json again (error handlers, etc.)
    res.json = originalJson;

    try {
      await markKeyCompleted({
        id:             keyRecord.id,
        responseStatus: res.statusCode,
        responseBody:   body,
      });
    } catch (err) {
      // Log but do not block the response.
      // The client gets their response; the key stays PENDING.
      // On retry, the stale PENDING will be recovered and the
      // operation re-executed — which is safe because the wallet
      // transaction either committed or rolled back atomically.
      console.error('[idempotency] Failed to mark key COMPLETED:', {
        keyId:   keyRecord.id,
        message: err.message,
      });
    }

    return originalJson(body);
  };
}

// ─────────────────────────────────────────
// MIDDLEWARE
// ─────────────────────────────────────────

/**
 * requireIdempotencyKey
 *
 * Attaches to all mutating endpoints (credit, purchase, claim).
 * Reads the endpoint name from res.locals.endpoint, which must be
 * set by the router before calling this middleware:
 *
 *   router.post('/credit', (req, res, next) => {
 *     res.locals.endpoint = 'credit';
 *     next();
 *   }, idempotency, creditHandler);
 *
 * Also reads res.locals.playerId, set by the router after
 * extracting :playerId from the URL.
 */
async function requireIdempotencyKey(req, res, next) {
  // ── Step 1: Header presence check ─────────────────────────────
  const idempotencyKey = req.headers['idempotency-key'];

  if (!idempotencyKey) {
    return res.status(400).json({
      error:   'MISSING_IDEMPOTENCY_KEY',
      message: 'Idempotency-Key header is required for this operation.',
    });
  }

  // ── Step 2: UUID v4 format validation ─────────────────────────
  if (!UUID_V4_REGEX.test(idempotencyKey)) {
    return res.status(400).json({
      error:   'INVALID_IDEMPOTENCY_KEY',
      message: 'Idempotency-Key must be a valid UUID v4.',
      received: idempotencyKey,
    });
  }

  // ── Step 3: Compute request hash ──────────────────────────────
  const requestHash = canonicalHash(req.body || {});

  // ── Step 4: Read context set by the router ────────────────────
  const endpoint = res.locals.endpoint;
  const playerId = res.locals.playerId;

  if (!endpoint) {
    // Programming error — router forgot to set res.locals.endpoint
    console.error('[idempotency] res.locals.endpoint not set by router');
    return res.status(500).json({
      error:   'INTERNAL_ERROR',
      message: 'Server configuration error.',
    });
  }

  // ── Step 5: Check for existing record ─────────────────────────
  try {
    let existing = await fetchExistingKey(idempotencyKey, endpoint);

    // ── NOT FOUND: attempt to insert a PENDING row ─────────────
    if (!existing) {
      const inserted = await insertPendingKey({
        idempotencyKey,
        endpoint,
        playerId,
        requestHash,
      });

      if (inserted) {
        // Happy path: we own this key, proceed to business logic.
        interceptResponse(res, inserted);
        return next();
      }

      // Unique violation: a concurrent request beat us.
      // Re-fetch to find out its current state.
      existing = await fetchExistingKey(idempotencyKey, endpoint);

      if (!existing) {
        // Should not happen — another process inserted then immediately
        // deleted the row. Treat as a transient error.
        return res.status(503).json({
          error:   'SERVICE_UNAVAILABLE',
          message: 'Transient conflict. Please retry.',
        });
      }
    }

    // ── FOUND: handle each state ────────────────────────────────

    // ── COMPLETED: validate hash, then replay ──────────────────
    if (existing.status === 'COMPLETED') {
      if (existing.request_hash !== requestHash) {
        return res.status(422).json({
          error:   'IDEMPOTENCY_KEY_MISMATCH',
          message:
            'This Idempotency-Key was used with a different request body. ' +
            'Use a new key for a different operation.',
        });
      }

      // Exact replay: same status code, same body.
      // The client receives an indistinguishable response from the original.
      return res
        .status(existing.response_status)
        .json(existing.response_body);
    }

    // ── PENDING: check for crash-stale row ─────────────────────
    if (existing.status === 'PENDING') {
      const lockedAt  = new Date(existing.locked_at).getTime();
      const ageMs     = Date.now() - lockedAt;

      if (ageMs < PENDING_TIMEOUT_MS) {
        // A live request is processing. Tell the client to wait.
        return res.status(409).json({
          error:   'REQUEST_IN_FLIGHT',
          message: 'A request with this Idempotency-Key is already being processed. ' +
                   'Please wait and retry.',
          retryAfterMs: PENDING_TIMEOUT_MS - ageMs,
        });
      }

      // Stale PENDING: the original process crashed.
      // Mark it FAILED, then fall through to FAILED handling below.
      await markKeyFailed(existing.id);
      existing = { ...existing, status: 'FAILED' };
    }

    // ── FAILED: delete and allow a fresh attempt ───────────────
    if (existing.status === 'FAILED') {
      await deleteKey(existing.id);

      // Insert a new PENDING row for this retry attempt.
      const inserted = await insertPendingKey({
        idempotencyKey,
        endpoint,
        playerId,
        requestHash,
      });

      if (!inserted) {
        // Another concurrent retry beat us to the new insert.
        return res.status(409).json({
          error:   'REQUEST_IN_FLIGHT',
          message: 'A concurrent retry is already in progress.',
        });
      }

      interceptResponse(res, inserted);
      return next();
    }

    // ── Unknown status: defensive fallback ─────────────────────
    console.error('[idempotency] Unknown key status:', existing.status);
    return res.status(500).json({
      error:   'INTERNAL_ERROR',
      message: 'Unexpected idempotency key state.',
    });

  } catch (err) {
    console.error('[idempotency] Middleware error:', {
      message: err.message,
      code:    err.code,
      stack:   err.stack,
    });
    return res.status(500).json({
      error:   'INTERNAL_ERROR',
      message: 'Failed to process idempotency key.',
    });
  }
}

module.exports = { requireIdempotencyKey };