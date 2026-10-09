-- Target schema migration: db/migration/V0__naive.sql
-- Purpose: initial local accounts for chapter 2, step 1.1.
-- IMPORTANT: this is the only seed allowed to set accounts.balance directly.

INSERT INTO accounts (id, owner_ref, currency, balance)
VALUES
    ('00000000-0000-0000-0000-00000000000a', 'user-A', 'VND', 100000),
    ('00000000-0000-0000-0000-00000000000b', 'user-B', 'VND', 50000),
    ('00000000-0000-0000-0000-00000000000c', 'user-C', 'VND', 0)
ON CONFLICT DO NOTHING;
