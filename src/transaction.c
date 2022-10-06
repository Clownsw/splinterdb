#include "splinterdb/transaction.h"
#include "atomic_counter.h"
#include "splinterdb_private.h"
#include "transaction_util.h"
#include "platform_linux/poison.h"

typedef struct transactional_splinterdb_config {
   splinterdb_config kvsb_cfg;
} transactional_splinterdb_config;

typedef struct transactional_splinterdb {
   splinterdb                      *kvsb;
   transactional_splinterdb_config *tcfg;
   atomic_counter                   ts_allocator;
   transaction_table                active_transactions;
} transactional_splinterdb;

static int
transactional_splinterdb_create_or_open(const splinterdb_config   *kvsb_cfg,
                                        transactional_splinterdb **txn_kvsb,
                                        bool open_existing)
{
   transactional_splinterdb_config *txn_splinterdb_cfg;
   txn_splinterdb_cfg = TYPED_ZALLOC(0, txn_splinterdb_cfg);
   memcpy(txn_splinterdb_cfg, kvsb_cfg, sizeof(txn_splinterdb_cfg->kvsb_cfg));

   transactional_splinterdb *_txn_kvsb;
   _txn_kvsb       = TYPED_ZALLOC(0, _txn_kvsb);
   _txn_kvsb->tcfg = txn_splinterdb_cfg;

   int rc = splinterdb_create_or_open(
      &txn_splinterdb_cfg->kvsb_cfg, &_txn_kvsb->kvsb, open_existing);
   bool fail_to_create_splinterdb = (rc != 0);
   if (fail_to_create_splinterdb) {
      platform_free(0, _txn_kvsb);
      platform_free(0, txn_splinterdb_cfg);
      return rc;
   }

   atomic_counter_init(&_txn_kvsb->ts_allocator);
   transaction_table_init(&_txn_kvsb->active_transactions);

   *txn_kvsb = _txn_kvsb;

   return 0;
}

int
transactional_splinterdb_create(const splinterdb_config   *kvsb_cfg,
                                transactional_splinterdb **txn_kvsb)
{
   return transactional_splinterdb_create_or_open(kvsb_cfg, txn_kvsb, FALSE);
}

int
transactional_splinterdb_open(const splinterdb_config   *kvsb_cfg,
                              transactional_splinterdb **txn_kvsb)
{
   return transactional_splinterdb_create_or_open(kvsb_cfg, txn_kvsb, TRUE);
}

void
transactional_splinterdb_close(transactional_splinterdb **txn_kvsb)
{
   transactional_splinterdb *_txn_kvsb = *txn_kvsb;
   splinterdb_close(&_txn_kvsb->kvsb);

   transaction_table_deinit(&_txn_kvsb->active_transactions);
   atomic_counter_deinit(&_txn_kvsb->ts_allocator);

   platform_free(0, _txn_kvsb->tcfg);
   platform_free(0, _txn_kvsb);

   *txn_kvsb = NULL;
}

void
transactional_splinterdb_register_thread(transactional_splinterdb *kvs)
{
   splinterdb_register_thread(kvs->kvsb);
}

void
transactional_splinterdb_deregister_thread(transactional_splinterdb *kvs)
{
   splinterdb_deregister_thread(kvs->kvsb);
}

int
transactional_splinterdb_begin(transactional_splinterdb *txn_kvsb,
                               transaction              *txn)
{
   // Initialize the given transaction
   transaction_internal *txn_internal;
   transaction_internal_create(&txn_internal);

   txn->internal = txn_internal;

   // Get a new timestamp from a global atomic counter
   txn_internal->start_ts = atomic_counter_get_next(&txn_kvsb->ts_allocator);

   // Insert the new transaction into the active transaction table
   transaction_table_insert(&txn_kvsb->active_transactions, txn_internal);

   return 0;
}

int
transactional_splinterdb_commit(transactional_splinterdb *txn_kvsb,
                                transaction              *txn)
{
   transaction_internal *txn_internal = txn->internal;
   platform_assert(txn_internal != NULL);

   txn_internal->val_ts = atomic_counter_get_next(&txn_kvsb->ts_allocator);

   bool is_conflict =
      transaction_check_for_conflict(&txn_kvsb->active_transactions,
                                     txn_internal,
                                     txn_kvsb->tcfg->kvsb_cfg.data_cfg);

   if (is_conflict) {
      transaction_table_delete(&txn_kvsb->active_transactions, txn_internal);
      transaction_internal_destroy((transaction_internal **)&txn->internal);
      return -1;
   }

   int rc = 0;

   // Write all elements in txn->ws
   for (int i = 0; i < txn_internal->ws_size; ++i) {
      switch (message_class(txn_internal->ws[i].msg)) {
         case MESSAGE_TYPE_INSERT:
            rc = splinterdb_insert(txn_kvsb->kvsb,
                                   txn_internal->ws[i].key,
                                   message_slice(txn_internal->ws[i].msg));
            platform_assert(rc == 0);
            break;
         case MESSAGE_TYPE_UPDATE:
            rc = splinterdb_update(txn_kvsb->kvsb,
                                   txn_internal->ws[i].key,
                                   message_slice(txn_internal->ws[i].msg));
            platform_assert(rc == 0);
            break;
         case MESSAGE_TYPE_DELETE:
            rc = splinterdb_delete(txn_kvsb->kvsb, txn_internal->ws[i].key);
            platform_assert(rc == 0);
            break;
         default:
            platform_assert(0, "invalid operation");
      }
   }

   // FIXME: when committed transactions can be deleted?
   // transaction_clean_up(txn);

   txn_internal->fin_ts = atomic_counter_get_next(&txn_kvsb->ts_allocator);

   return 0;
}

int
transactional_splinterdb_abort(transactional_splinterdb *txn_kvsb,
                               transaction              *txn)
{
   platform_assert(txn->internal != NULL);

   transaction_table_delete(&txn_kvsb->active_transactions, txn->internal);
   transaction_internal_destroy((transaction_internal **)&txn->internal);

   return 0;
}

int
transactional_splinterdb_insert(transactional_splinterdb *txn_kvsb,
                                transaction              *txn,
                                slice                     key,
                                slice                     value)
{
   transaction_internal *txn_internal = txn->internal;
   platform_assert(txn_internal != NULL);

   writable_buffer key_buf;
   writable_buffer_init_from_slice(&key_buf, 0, key);
   txn_internal->ws[txn_internal->ws_size].key =
      writable_buffer_to_slice(&key_buf);

   writable_buffer value_buf;
   writable_buffer_init_from_slice(&value_buf, 0, value);
   txn_internal->ws[txn_internal->ws_size].msg =
      message_create(MESSAGE_TYPE_INSERT, writable_buffer_to_slice(&value_buf));

   ++txn_internal->ws_size;

   return 0;
}

int
transactional_splinterdb_delete(transactional_splinterdb *txn_kvsb,
                                transaction              *txn,
                                slice                     key)
{
   transaction_internal *txn_internal = txn->internal;
   platform_assert(txn_internal != NULL);

   writable_buffer key_buf;
   writable_buffer_init_from_slice(&key_buf, 0, key);
   txn_internal->ws[txn_internal->ws_size].key =
      writable_buffer_to_slice(&key_buf);
   txn_internal->ws[txn_internal->ws_size].msg = DELETE_MESSAGE;

   ++txn_internal->ws_size;

   return 0;
}

int
transactional_splinterdb_update(transactional_splinterdb *txn_kvsb,
                                transaction              *txn,
                                slice                     key,
                                slice                     delta)
{
   transaction_internal *txn_internal = txn->internal;
   platform_assert(txn_internal != NULL);

   writable_buffer key_buf;
   writable_buffer_init_from_slice(&key_buf, 0, key);
   txn_internal->ws[txn_internal->ws_size].key =
      writable_buffer_to_slice(&key_buf);
   writable_buffer delta_buf;
   writable_buffer_init_from_slice(&delta_buf, 0, delta);
   txn_internal->ws[txn_internal->ws_size].msg =
      message_create(MESSAGE_TYPE_UPDATE, writable_buffer_to_slice(&delta_buf));

   ++txn_internal->ws_size;

   return 0;
}

int
transactional_splinterdb_lookup(transactional_splinterdb *txn_kvsb,
                                transaction              *txn,
                                slice                     key,
                                splinterdb_lookup_result *result)
{
   transaction_internal *txn_internal = txn->internal;
   platform_assert(txn_internal != NULL);

   // Support read a value within its write set, which may not be committed
   for (int i = 0; i < txn_internal->ws_size; ++i) {
      if (data_key_compare(
             txn_kvsb->tcfg->kvsb_cfg.data_cfg, key, txn_internal->ws[i].key)
          == 0)
      {
         _splinterdb_lookup_result *_result =
            (_splinterdb_lookup_result *)result;
         merge_accumulator_copy_message(&_result->value,
                                        txn_internal->ws[i].msg);

         writable_buffer key_buf;
         writable_buffer_init_from_slice(&key_buf, 0, key);
         txn_internal->rs[txn_internal->rs_size].key =
            writable_buffer_to_slice(&key_buf);
         ++txn_internal->rs_size;

         return 0;
      }
   }

   int rc = splinterdb_lookup(txn_kvsb->kvsb, key, result);

   if (splinterdb_lookup_found(result)) {
      writable_buffer key_buf;
      writable_buffer_init_from_slice(&key_buf, 0, key);
      txn_internal->rs[txn_internal->rs_size].key =
         writable_buffer_to_slice(&key_buf);
      ++txn_internal->rs_size;
   }

   return rc;
}

void
transactional_splinterdb_lookup_result_init(
   transactional_splinterdb *txn_kvsb,   // IN
   splinterdb_lookup_result *result,     // IN/OUT
   uint64                    buffer_len, // IN
   char                     *buffer      // IN
)
{
   return splinterdb_lookup_result_init(
      txn_kvsb->kvsb, result, buffer_len, buffer);
}

void
transactional_splinterdb_set_isolation_level(
   transactional_splinterdb   *txn_kvsb,
   transaction_isolation_level isol_level)
{
   // TODO: implement isolation_level. Current: serializable
}
