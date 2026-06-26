require('dotenv').config();

// src/db.js

'use strict';

const { Pool } = require('pg');

// ─────────────────────────────────────────
// POOL CONFIGURATION
// All values read from environment variables.
// Fallback defaults match docker-compose.yml so local dev works immediately.
// ─────────────────────────────────────────
const pool = new Pool({
  host:     process.env.POSTGRES_HOST     || 'localhost',
  port:     parseInt(process.env.POSTGRES_PORT || '5432', 10),
  user:     process.env.POSTGRES_USER     || 'wallet_user',
  password: process.env.POSTGRES_PASSWORD || 'wallet_pass',
  database: process.env.POSTGRES_DB       || 'wallet_db',

  // Maximum number of connections in the pool.
  // Keep conservative — Postgres default max_connections is 100.
  // If you run multiple Node processes, max per process must be lower.
  max: parseInt(process.env.DB_POOL_MAX || '10', 10),

  // How long (ms) a connection can sit idle before being closed.
  // Frees Postgres backend processes during quiet periods.
  idleTimeoutMillis: parseInt(process.env.DB_IDLE_TIMEOUT || '30000', 10),

  // How long (ms) to wait for a connection from the pool before erroring.
  // Fail fast: better than a request hanging indefinitely.
  connectionTimeoutMillis: parseInt(process.env.DB_CONNECT_TIMEOUT || '5000', 10),

  // How long (ms) a single query may run before the pool kills it.
  // Prevents a rogue query from holding a connection forever.
  statement_timeout: parseInt(process.env.DB_STATEMENT_TIMEOUT || '30000', 10),
});

// ─────────────────────────────────────────
// POOL ERROR HANDLER
// pg emits 'error' on the pool when a backend connection dies unexpectedly
// (e.g. Postgres restart, idle connection dropped by a firewall).
// Without this listener, Node.js crashes on unhandled EventEmitter errors.
// With it, the pool logs the event and recovers by opening a new connection
// on the next request.
// ─────────────────────────────────────────
pool.on('error', (err, client) => {
  console.error('[db] Unexpected error on idle client:', {
    message: err.message,
    code:    err.code,
  });
  // Do not re-throw. The pool self-heals; the process stays alive.
});

pool.on('connect', () => {
  // Fires each time the pool opens a new physical connection.
  // Useful for debugging pool sizing in development.
  if (process.env.NODE_ENV !== 'production') {
    console.debug('[db] New client connection established');
  }
});

// ─────────────────────────────────────────
// CONNECTION TEST
// Called at application startup to verify credentials and reachability.
// Fails loudly with a useful message rather than silently starting a
// service that cannot reach its database.
// ─────────────────────────────────────────
async function testConnection() {
  let client;

  try {
    client = await pool.connect();

    const result = await client.query(
      'SELECT current_database() AS db, current_user AS usr, now() AS ts'
    );

    const { db, usr, ts } = result.rows[0];
    console.log('[db] Connection verified:', { database: db, user: usr, time: ts });

  } catch (err) {
    // Rethrow with context so the startup caller can decide to exit.
    throw new Error(`[db] Failed to connect to PostgreSQL: ${err.message}`);

  } finally {
    // Always release the client back to the pool, even if the query failed.
    // Forgetting this leaks a connection slot permanently.
    if (client) client.release();
  }
}

// ─────────────────────────────────────────
// GRACEFUL SHUTDOWN
// Called when the process receives SIGTERM or SIGINT.
// pool.end() drains the pool: waits for checked-out clients to finish
// their current query, then closes all connections cleanly.
// This prevents in-flight transactions from being abruptly dropped.
// ─────────────────────────────────────────
async function gracefulShutdown(signal) {
  console.log(`[db] Received ${signal}. Closing connection pool...`);

  try {
    await pool.end();
    console.log('[db] Connection pool closed. Goodbye.');
  } catch (err) {
    console.error('[db] Error during pool shutdown:', err.message);
    process.exit(1);
  }
}

// ─────────────────────────────────────────
// QUERY HELPER  (optional convenience wrapper)
// Wraps pool.query() so callers don't need to import the pool directly
// for simple one-shot queries. For transactions, callers must use
// pool.connect() directly to get a dedicated client and control
// BEGIN / COMMIT / ROLLBACK manually.
// ─────────────────────────────────────────
async function query(text, params) {
  const start = Date.now();

  try {
    const result = await pool.query(text, params);

    if (process.env.NODE_ENV !== 'production') {
      console.debug('[db] Query executed:', {
        text:     text.replace(/\s+/g, ' ').trim().slice(0, 120),
        duration: `${Date.now() - start}ms`,
        rows:     result.rowCount,
      });
    }

    return result;

  } catch (err) {
    console.error('[db] Query error:', {
      text:    text.replace(/\s+/g, ' ').trim().slice(0, 120),
      params,
      message: err.message,
      code:    err.code,
    });
    throw err;  // Re-throw: caller decides how to handle (rollback, respond, etc.)
  }
}

// ─────────────────────────────────────────
// TRANSACTION HELPER
// Acquires a dedicated client, runs the caller-supplied async function
// inside BEGIN/COMMIT, and rolls back automatically on any error.
// Releases the client in all cases via finally.
//
// Usage:
//   await withTransaction(async (client) => {
//     await client.query('UPDATE wallets ...');
//     await client.query('INSERT INTO wallet_transactions ...');
//   });
// ─────────────────────────────────────────
async function withTransaction(fn) {
  const client = await pool.connect();

  try {
    await client.query('BEGIN');
    const result = await fn(client);
    await client.query('COMMIT');
    return result;

  } catch (err) {
    // Always attempt rollback. If rollback itself fails, log and rethrow.
    try {
      await client.query('ROLLBACK');
    } catch (rollbackErr) {
      console.error('[db] Rollback failed:', rollbackErr.message);
    }
    throw err;  // Re-throw original error to the caller.

  } finally {
    // Release back to the pool unconditionally.
    // If this is skipped on any code path, the pool drains and hangs.
    client.release();
  }
}

// ─────────────────────────────────────────
// EXPORTS
// ─────────────────────────────────────────
module.exports = {
  pool,              // Raw pool: for callers that need manual transaction control
  query,             // One-shot query helper
  withTransaction,   // Transactional block helper
  testConnection,    // Startup health check
  gracefulShutdown,  // SIGTERM / SIGINT handler
};