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
   txn->start_ts = txn->val_ts = txn->fin_ts = 0;
   txn->ws_size = txn->rs_size = 0;

   // Get a new timestamp from a global atomic counter
   txn->start_ts = atomic_counter_get_next(&txn_kvsb->ts_allocator);

   // Insert the new transaction into the active transaction table
   transaction_table_insert(&txn_kvsb->active_transactions, txn);

   return 0;
}

static void
transaction_clean_up(transaction *txn)
{
   for (uint64 i = 0; i < txn->ws_size; ++i) {
      writable_buffer key_buf;
      writable_buffer_init_with_buffer(&key_buf,
                                       0,
                                       slice_length(txn->ws[i].key),
                                       (void *)slice_data(txn->ws[i].key),
                                       slice_length(txn->ws[i].key));
      writable_buffer_resize(&key_buf, slice_length(txn->ws[i].key));
      writable_buffer_deinit(&key_buf);

      writable_buffer value_buf;
      writable_buffer_init_with_buffer(&value_buf,
                                       0,
                                       message_length(txn->ws[i].msg),
                                       (void *)message_data(txn->ws[i].msg),
                                       message_length(txn->ws[i].msg));
      writable_buffer_resize(&value_buf, message_length(txn->ws[i].msg));
      writable_buffer_deinit(&value_buf);
   }

   for (uint64 i = 0; i < txn->rs_size; ++i) {
      writable_buffer key_buf;
      writable_buffer_init_with_buffer(&key_buf,
                                       0,
                                       slice_length(txn->rs[i].key),
                                       (void *)slice_data(txn->rs[i].key),
                                       slice_length(txn->rs[i].key));
      writable_buffer_resize(&key_buf, slice_length(txn->rs[i].key));
      writable_buffer_deinit(&key_buf);

      writable_buffer value_buf;
      writable_buffer_init_with_buffer(&value_buf,
                                       0,
                                       message_length(txn->rs[i].msg),
                                       (void *)message_data(txn->rs[i].msg),
                                       message_length(txn->rs[i].msg));
      writable_buffer_resize(&value_buf, message_length(txn->rs[i].msg));
      writable_buffer_deinit(&value_buf);
   }

   txn->start_ts = txn->val_ts = txn->fin_ts = 0;
   txn->ws_size = txn->rs_size = 0;
}

int
transactional_splinterdb_commit(transactional_splinterdb *txn_kvsb,
                                transaction              *txn)
{
   txn->val_ts = atomic_counter_get_next(&txn_kvsb->ts_allocator);

   bool is_conflict = transaction_check_for_conflict(
      &txn_kvsb->active_transactions, txn, txn_kvsb->tcfg->kvsb_cfg.data_cfg);

   transaction_table_delete(&txn_kvsb->active_transactions, txn);

   if (is_conflict) {
      transaction_clean_up(txn);
      return 1;
   }

   int rc = 0;

   // Write all elements in txn->ws
   for (int i = 0; i < txn->ws_size; ++i) {
      switch (message_class(txn->ws[i].msg)) {
         case MESSAGE_TYPE_INSERT:
            rc = splinterdb_insert(
               txn_kvsb->kvsb, txn->ws[i].key, message_slice(txn->ws[i].msg));
            platform_assert(rc == 0);
            break;
         case MESSAGE_TYPE_UPDATE:
            rc = splinterdb_update(
               txn_kvsb->kvsb, txn->ws[i].key, message_slice(txn->ws[i].msg));
            platform_assert(rc == 0);
            break;
         case MESSAGE_TYPE_DELETE:
            rc = splinterdb_delete(txn_kvsb->kvsb, txn->ws[i].key);
            platform_assert(rc == 0);
            break;
         default:
            platform_assert(0, "invalid operation");
      }
   }

   transaction_clean_up(txn);

   txn->fin_ts = atomic_counter_get_next(&txn_kvsb->ts_allocator);
   return 0;
}

int
transactional_splinterdb_abort(transactional_splinterdb *txn_kvsb,
                               transaction              *txn)
{
   transaction_table_delete(&txn_kvsb->active_transactions, txn);
   transaction_clean_up(txn);

   return 0;
}

int
transactional_splinterdb_insert(transactional_splinterdb *txn_kvsb,
                                transaction              *txn,
                                slice                     key,
                                slice                     value)
{
   writable_buffer key_buf;
   writable_buffer_init_from_slice(&key_buf, 0, key);
   txn->ws[txn->ws_size].key = writable_buffer_to_slice(&key_buf);

   writable_buffer value_buf;
   writable_buffer_init_from_slice(&value_buf, 0, value);
   txn->ws[txn->ws_size].msg =
      message_create(MESSAGE_TYPE_INSERT, writable_buffer_to_slice(&value_buf));

   ++txn->ws_size;

   return 0;
}

int
transactional_splinterdb_delete(transactional_splinterdb *txn_kvsb,
                                transaction              *txn,
                                slice                     key)
{
   writable_buffer key_buf;
   writable_buffer_init_from_slice(&key_buf, 0, key);
   txn->ws[txn->ws_size].key = writable_buffer_to_slice(&key_buf);
   txn->ws[txn->ws_size].msg = DELETE_MESSAGE;

   ++txn->ws_size;

   return 0;
}

int
transactional_splinterdb_update(transactional_splinterdb *txn_kvsb,
                                transaction              *txn,
                                slice                     key,
                                slice                     delta)
{
   writable_buffer key_buf;
   writable_buffer_init_from_slice(&key_buf, 0, key);
   txn->ws[txn->ws_size].key = writable_buffer_to_slice(&key_buf);
   writable_buffer delta_buf;
   writable_buffer_init_from_slice(&delta_buf, 0, delta);
   txn->ws[txn->ws_size].msg =
      message_create(MESSAGE_TYPE_UPDATE, writable_buffer_to_slice(&delta_buf));

   ++txn->ws_size;

   return 0;
}

int
transactional_splinterdb_lookup(transactional_splinterdb *txn_kvsb,
                                transaction              *txn,
                                slice                     key,
                                splinterdb_lookup_result *result)
{
   // Support read a value within its write set, which may not be committed
   for (int i = 0; i < txn->ws_size; ++i) {
      if (data_key_compare(
             txn_kvsb->tcfg->kvsb_cfg.data_cfg, key, txn->ws[i].key)
          == 0)
      {
         _splinterdb_lookup_result *_result =
            (_splinterdb_lookup_result *)result;
         merge_accumulator_copy_message(&_result->value, txn->ws[i].msg);

         return 0;
      }
   }

   ++txn->ws_size;

   int rc = splinterdb_lookup(txn_kvsb->kvsb, key, result);

   if (splinterdb_lookup_found(result)) {
      writable_buffer key_buf;
      writable_buffer_init_from_slice(&key_buf, 0, key);
      txn->rs[txn->rs_size].key = writable_buffer_to_slice(&key_buf);
      ++txn->rs_size;
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