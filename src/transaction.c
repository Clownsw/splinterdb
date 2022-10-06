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
   transaction_table                all_transactions;
   platform_mutex                   lock;
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
   transaction_table_init(&_txn_kvsb->all_transactions);
   platform_mutex_init(&_txn_kvsb->lock, 0, 0);

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

   platform_mutex_destroy(&_txn_kvsb->lock);
   transaction_table_deinit(&_txn_kvsb->all_transactions);
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

   // Get a new timestamp from a global atomic counter
   txn_internal->start_tn = atomic_counter_get_current(&txn_kvsb->ts_allocator);


   txn->internal = txn_internal;

   return 0;
}

static void
write_into_splinterdb(transactional_splinterdb *txn_kvsb,
                      transaction_internal     *txn_internal)
{
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
}

int
transactional_splinterdb_commit(transactional_splinterdb *txn_kvsb,
                                transaction              *txn)
{
   transaction_internal *txn_internal = txn->internal;
   platform_assert(txn_internal != NULL);

   platform_mutex_lock(&txn_kvsb->lock);

   txn_internal->finish_tn =
      atomic_counter_get_current(&txn_kvsb->ts_allocator);
   bool valid =
      transaction_check_for_conflict(&txn_kvsb->all_transactions,
                                     txn_internal,
                                     txn_kvsb->tcfg->kvsb_cfg.data_cfg);

   if (valid) {
      // The new transaction finally becomes visible globally
      transaction_table_insert(&txn_kvsb->all_transactions, txn_internal);

      write_into_splinterdb(txn_kvsb, txn_internal);
      txn_internal->tn = atomic_counter_get_next(&txn_kvsb->ts_allocator);
   }

   platform_mutex_unlock(&txn_kvsb->lock);

   if (!valid) {
      transaction_internal_destroy((transaction_internal **)&txn->internal);
      return -1;
   }

   // TODO: Garbage collection

   return 0;
}

int
transactional_splinterdb_abort(transactional_splinterdb *txn_kvsb,
                               transaction              *txn)
{
   platform_assert(txn->internal != NULL);

   transaction_internal_destroy((transaction_internal **)&txn->internal);

   return 0;
}

static void
insert_into_write_set(transaction_internal *txn_internal,
                      slice                 key,
                      message_type          op,
                      slice                 value,
                      const data_config    *cfg)
{
   // check if there is the same key in its write set
   for (uint64 i = 0; i < txn_internal->ws_size; ++i) {
      if (data_key_compare(cfg, key, txn_internal->ws[i].key) == 0) {
         if (op == MESSAGE_TYPE_INSERT) {
            writable_buffer value_buf;
            writable_buffer_init_with_buffer(
               &value_buf,
               0,
               message_length(txn_internal->ws[i].msg),
               (void *)message_data(txn_internal->ws[i].msg),
               message_length(txn_internal->ws[i].msg));
            writable_buffer_copy_slice(&value_buf, value);

            txn_internal->ws[i].msg =
               message_create(op, writable_buffer_to_slice(&value_buf));
         } else if (op == MESSAGE_TYPE_DELETE) {
            txn_internal->ws[i].msg = DELETE_MESSAGE;
         } else if (op == MESSAGE_TYPE_UPDATE) {
            merge_accumulator new_msg;
            merge_accumulator_init_from_message(
               &new_msg, 0, message_create(op, value));

            data_merge_tuples(cfg, key, txn_internal->ws[i].msg, &new_msg);

            writable_buffer value_buf;
            writable_buffer_init_with_buffer(
               &value_buf,
               0,
               message_length(txn_internal->ws[i].msg),
               (void *)message_data(txn_internal->ws[i].msg),
               message_length(txn_internal->ws[i].msg));
            writable_buffer_copy_slice(&value_buf,
                                       merge_accumulator_to_value(&new_msg));

            txn_internal->ws[i].msg =
               message_create(merge_accumulator_message_class(&new_msg),
                              writable_buffer_to_slice(&value_buf));

            merge_accumulator_deinit(&new_msg);
         }

         return;
      }
   }

   writable_buffer key_buf;
   writable_buffer_init_from_slice(&key_buf, 0, key);
   txn_internal->ws[txn_internal->ws_size].key =
      writable_buffer_to_slice(&key_buf);

   if (op == MESSAGE_TYPE_DELETE) {
      txn_internal->ws[txn_internal->ws_size].msg = DELETE_MESSAGE;
   } else {
      writable_buffer value_buf;
      writable_buffer_init_from_slice(&value_buf, 0, value);
      txn_internal->ws[txn_internal->ws_size].msg =
         message_create(op, writable_buffer_to_slice(&value_buf));
   }

   ++txn_internal->ws_size;
}

static void
insert_into_read_set(transaction_internal *txn_internal, slice key)
{
   writable_buffer key_buf;
   writable_buffer_init_from_slice(&key_buf, 0, key);
   txn_internal->rs[txn_internal->rs_size].key =
      writable_buffer_to_slice(&key_buf);

   ++txn_internal->rs_size;
}

int
transactional_splinterdb_insert(transactional_splinterdb *txn_kvsb,
                                transaction              *txn,
                                slice                     key,
                                slice                     value)
{
   transaction_internal *txn_internal = txn->internal;
   platform_assert(txn_internal != NULL);

   insert_into_write_set(txn_internal,
                         key,
                         MESSAGE_TYPE_INSERT,
                         value,
                         txn_kvsb->tcfg->kvsb_cfg.data_cfg);

   return 0;
}

int
transactional_splinterdb_delete(transactional_splinterdb *txn_kvsb,
                                transaction              *txn,
                                slice                     key)
{
   transaction_internal *txn_internal = txn->internal;
   platform_assert(txn_internal != NULL);

   insert_into_write_set(txn_internal,
                         key,
                         MESSAGE_TYPE_DELETE,
                         NULL_SLICE,
                         txn_kvsb->tcfg->kvsb_cfg.data_cfg);

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

   insert_into_write_set(txn_internal,
                         key,
                         MESSAGE_TYPE_UPDATE,
                         delta,
                         txn_kvsb->tcfg->kvsb_cfg.data_cfg);

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

         insert_into_read_set(txn_internal, key);

         return 0;
      }
   }

   int rc = splinterdb_lookup(txn_kvsb->kvsb, key, result);

   if (splinterdb_lookup_found(result)) {
      insert_into_read_set(txn_internal, key);
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
