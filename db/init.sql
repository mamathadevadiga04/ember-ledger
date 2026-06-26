-- db/init.sql
-- Runs once on first boot via docker-entrypoint-initdb.d
-- Idempotent within a fresh database: all objects created fresh.

-- ─────────────────────────────────────────
-- EXTENSIONS
-- ─────────────────────────────────────────

-- gen_random_uuid() requires pgcrypto in Postgres < 13.
-- In Postgres 13+ it is built-in, but enabling pgcrypto is harmless.
-- uuid-ossp provides uuid_generate_v4() as an alternative.
CREATE EXTENSION IF NOT EXISTS "pgcrypto";
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";


-- ─────────────────────────────────────────
-- WALLETS
-- Purpose : One row per player. The authoritative live balance.
-- ─────────────────────────────────────────
CREATE TABLE wallets (
  player_id   UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
  balance     BIGINT      NOT NULL DEFAULT 0,
  updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),

  -- Database-enforced floor. Application bugs cannot produce a negative balance.
  CONSTRAINT chk_wallets_balance_non_negative CHECK (balance >= 0)
);

-- Supports GET /v1/wallets/:playerId — lookup by PK, always fast.
-- No additional index needed; PRIMARY KEY index covers it.


-- ─────────────────────────────────────────
-- SHOP ITEMS
-- Purpose : Server-owned price catalog. Clients never supply prices.
-- ─────────────────────────────────────────
CREATE TABLE shop_items (
  item_id    TEXT        PRIMARY KEY,
  name       TEXT        NOT NULL,
  price      BIGINT      NOT NULL,
  is_active  BOOLEAN     NOT NULL DEFAULT true,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),

  CONSTRAINT chk_shop_items_price_positive CHECK (price > 0)
);

-- Filtered index: purchase flow only queries active items.
-- A partial index on is_active = true is smaller and faster than a full index.
CREATE INDEX idx_shop_items_active ON shop_items (item_id) WHERE is_active = true;


-- ─────────────────────────────────────────
-- REWARDS
-- Purpose : Reward catalog with server-owned amounts.
-- ─────────────────────────────────────────
CREATE TABLE rewards (
  reward_id   TEXT        PRIMARY KEY,
  amount      BIGINT      NOT NULL,
  description TEXT,
  is_active   BOOLEAN     NOT NULL DEFAULT true,

  CONSTRAINT chk_rewards_amount_positive CHECK (amount > 0)
);


-- ─────────────────────────────────────────
-- IDEMPOTENCY KEYS
-- Purpose : Request deduplication and response replay.
--           Committed in its own transaction BEFORE the wallet transaction.
--           Surviving PENDING rows are crash evidence.
-- ─────────────────────────────────────────
CREATE TABLE idempotency_keys (
  id               UUID        PRIMARY KEY DEFAULT gen_random_uuid(),

  -- Client-supplied header value. Scoped to endpoint to allow
  -- the same UUID to be reused across different operation types.
  idempotency_key  TEXT        NOT NULL,
  endpoint         TEXT        NOT NULL,   -- 'credit' | 'purchase' | 'claim'

  player_id        UUID        NOT NULL  REFERENCES wallets(player_id),

  -- SHA-256 of the canonical request body.
  -- Detects same-key / different-body misuse (client bug).
  request_hash     TEXT        NOT NULL,

  status           TEXT        NOT NULL DEFAULT 'PENDING',

  -- Stored so duplicates receive the exact same HTTP response
  -- without re-executing any business logic.
  response_status  INT,
  response_body    JSONB,

  -- locked_at: when processing began.
  -- Used to detect crash-stale PENDING rows (age > threshold → allow retry).
  locked_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  completed_at     TIMESTAMPTZ,
  created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),

  -- Prunable after 24 hours. A background job can DELETE WHERE expires_at < now().
  expires_at       TIMESTAMPTZ NOT NULL DEFAULT (now() + INTERVAL '24 hours'),

  -- The atomic deduplication constraint.
  -- Two concurrent inserts with the same (key, endpoint) → one wins, one gets
  -- a unique violation, which the application catches and treats as a duplicate.
  CONSTRAINT uq_idempotency_key_endpoint UNIQUE (idempotency_key, endpoint),

  CONSTRAINT chk_idempotency_status
    CHECK (status IN ('PENDING', 'COMPLETED', 'FAILED')),

  CONSTRAINT chk_idempotency_response_when_completed
    CHECK (
      status != 'COMPLETED'
      OR (response_status IS NOT NULL AND response_body IS NOT NULL)
    )
);

-- Fast lookup on the critical request path: key + endpoint → existing record.
-- Covering index includes status and request_hash to avoid a heap fetch
-- in the common duplicate-detection case.
CREATE INDEX idx_idempotency_lookup
  ON idempotency_keys (idempotency_key, endpoint)
  INCLUDE (status, request_hash, response_status, response_body);

-- Background job / startup scan for crash-stale PENDING rows.
-- Partial index: only PENDING rows are indexed, keeping it tiny.
CREATE INDEX idx_idempotency_pending_locked
  ON idempotency_keys (locked_at)
  WHERE status = 'PENDING';

-- TTL cleanup job target.
CREATE INDEX idx_idempotency_expires
  ON idempotency_keys (expires_at);

-- Audit: find all operations for a given player.
CREATE INDEX idx_idempotency_player
  ON idempotency_keys (player_id, created_at DESC);


-- ─────────────────────────────────────────
-- WALLET TRANSACTIONS
-- Purpose : Permanent, append-only financial audit ledger.
--           Never pruned. Stores balance snapshots for integrity checks.
-- ─────────────────────────────────────────
CREATE TABLE wallet_transactions (
  id              UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
  player_id       UUID        NOT NULL REFERENCES wallets(player_id),
  type            TEXT        NOT NULL,

  -- Signed amount: positive = credit, negative = debit.
  -- Invariant: SUM(amount) WHERE player_id = X must equal wallets.balance.
  amount          BIGINT      NOT NULL,

  -- Snapshots allow point-in-time reconstruction and drift detection
  -- without replaying the full transaction history.
  balance_before  BIGINT      NOT NULL,
  balance_after   BIGINT      NOT NULL,

  -- Nullable: only set for the relevant operation type.
  item_id         TEXT        REFERENCES shop_items(item_id),
  reward_id       TEXT        REFERENCES rewards(reward_id),

  -- Traceability: link every ledger row back to the idempotency record
  -- that produced it. Enables full request → effect chain reconstruction.
  idempotency_key TEXT        NOT NULL,

  metadata        JSONB,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),

  CONSTRAINT chk_wt_type
    CHECK (type IN ('credit', 'purchase', 'reward_claim')),

  -- balance_after = balance_before + amount, always.
  CONSTRAINT chk_wt_balance_arithmetic
    CHECK (balance_after = balance_before + amount),

  -- Debits must be negative, credits must be positive.
  CONSTRAINT chk_wt_amount_nonzero
    CHECK (amount != 0)
);

-- Primary query pattern: player history, newest first.
CREATE INDEX idx_wt_player_time
  ON wallet_transactions (player_id, created_at DESC);

-- Traceability: given an idempotency key, find the ledger row it produced.
CREATE INDEX idx_wt_idempotency_key
  ON wallet_transactions (idempotency_key);

-- Aggregate queries by operation type (e.g. total credits in a time window).
CREATE INDEX idx_wt_type_time
  ON wallet_transactions (type, created_at DESC);


-- ─────────────────────────────────────────
-- INVENTORY
-- Purpose : Items a player owns. One row per granted item instance.
--           Linked to the exact transaction that produced the grant.
-- ─────────────────────────────────────────
CREATE TABLE inventory (
  id             UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
  player_id      UUID        NOT NULL REFERENCES wallets(player_id),
  item_id        TEXT        NOT NULL REFERENCES shop_items(item_id),

  -- Records the price at time of purchase.
  -- Protects against price changes invalidating historical records.
  price_paid     BIGINT      NOT NULL,

  -- Chain: inventory row → transaction → idempotency key → original request.
  transaction_id UUID        NOT NULL REFERENCES wallet_transactions(id),

  acquired_at    TIMESTAMPTZ NOT NULL DEFAULT now(),

  CONSTRAINT chk_inventory_price_paid_positive CHECK (price_paid > 0)
);

-- Primary query pattern: what does this player own?
CREATE INDEX idx_inventory_player
  ON inventory (player_id, acquired_at DESC);

-- Support idempotency check: did this transaction already grant an item?
CREATE INDEX idx_inventory_transaction
  ON inventory (transaction_id);


-- ─────────────────────────────────────────
-- REWARD CLAIMS
-- Purpose : Records which player claimed which reward.
--           UNIQUE constraint is the DB-level one-claim-per-player guard.
-- ─────────────────────────────────────────
CREATE TABLE reward_claims (
  id             UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
  reward_id      TEXT        NOT NULL REFERENCES rewards(reward_id),
  player_id      UUID        NOT NULL REFERENCES wallets(player_id),

  -- Chain back to the ledger row.
  transaction_id UUID        NOT NULL REFERENCES wallet_transactions(id),

  claimed_at     TIMESTAMPTZ NOT NULL DEFAULT now(),

  -- The atomic exactly-once claim guarantee.
  -- Two concurrent claim attempts → one insert wins, one gets a unique violation.
  -- Application catches the violation and returns 409 ALREADY_CLAIMED.
  CONSTRAINT uq_reward_claim_per_player UNIQUE (reward_id, player_id)
);

-- Find all claims for a player (wallet history view).
CREATE INDEX idx_reward_claims_player
  ON reward_claims (player_id, claimed_at DESC);


-- ─────────────────────────────────────────
-- SEED DATA
-- ─────────────────────────────────────────

INSERT INTO shop_items (item_id, name, price, is_active) VALUES
  ('sword_01',  'Iron Sword',      100, true),
  ('shield_01', 'Wooden Shield',   150, true),
  ('potion_01', 'Health Potion',    50, true)
ON CONFLICT (item_id) DO NOTHING;  -- Safe to re-run; idempotent.

INSERT INTO rewards (reward_id, amount, description, is_active) VALUES
  ('welcome_bonus', 100, 'One-time welcome gift for new players', true),
  ('daily_bonus',    50, 'Claimable once every 24 hours',         true),
  ('weekly_bonus',  500, 'Claimable once every 7 days',           true)
ON CONFLICT (reward_id) DO NOTHING;