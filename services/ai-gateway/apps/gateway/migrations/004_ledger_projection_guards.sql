CREATE OR REPLACE FUNCTION balanced_transaction() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE total bigint; count_entries integer; target uuid;
BEGIN
 IF TG_TABLE_NAME='ledger_entries' THEN target=NEW.transaction_id; ELSE target=NEW.id; END IF;
 SELECT coalesce(sum(amount_urub),0),count(*) INTO total,count_entries FROM ledger_entries WHERE transaction_id=target;
 IF total<>0 OR count_entries<2 THEN RAISE EXCEPTION 'unbalanced ledger transaction'; END IF;
 RETURN NULL;
END $$;
CREATE CONSTRAINT TRIGGER entries_must_stay_balanced AFTER INSERT ON ledger_entries
  DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION balanced_transaction();

CREATE FUNCTION wallet_projection_matches() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE recorded bigint; projected bigint;
BEGIN
 SELECT balance_urub INTO projected FROM wallets WHERE tenant_id=NEW.tenant_id;
 SELECT coalesce(sum(e.amount_urub),0) INTO recorded FROM ledger_entries e
 JOIN ledger_transactions t ON t.id=e.transaction_id WHERE t.tenant_id=NEW.tenant_id AND e.account='wallet';
 IF recorded<>projected THEN RAISE EXCEPTION 'wallet projection differs from ledger'; END IF;
 RETURN NULL;
END $$;
CREATE CONSTRAINT TRIGGER wallet_projection_consistent AFTER INSERT OR UPDATE ON wallets
  DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION wallet_projection_matches();
