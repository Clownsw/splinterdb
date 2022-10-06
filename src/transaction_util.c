#include "transaction_util.h"
#include "data_internal.h"


void
transaction_internal_create(transaction_internal **new_internal)
{
   transaction_internal *txn_internal;
   txn_internal           = TYPED_ZALLOC(0, txn_internal);
   txn_internal->start_ts = txn_internal->val_ts = txn_internal->fin_ts = 0;
   txn_internal->ws_size = txn_internal->rs_size = 0;

   *new_internal = txn_internal;
}

void
transaction_internal_destroy(transaction_internal **internal_to_delete)
{
   transaction_internal *txn_internal = *internal_to_delete;

   for (uint64 i = 0; i < txn_internal->ws_size; ++i) {
      writable_buffer key_buf;
      writable_buffer_init_with_buffer(
         &key_buf,
         0,
         slice_length(txn_internal->ws[i].key),
         (void *)slice_data(txn_internal->ws[i].key),
         slice_length(txn_internal->ws[i].key));
      writable_buffer_resize(&key_buf, slice_length(txn_internal->ws[i].key));
      writable_buffer_deinit(&key_buf);

      writable_buffer value_buf;
      writable_buffer_init_with_buffer(
         &value_buf,
         0,
         message_length(txn_internal->ws[i].msg),
         (void *)message_data(txn_internal->ws[i].msg),
         message_length(txn_internal->ws[i].msg));
      writable_buffer_resize(&value_buf,
                             message_length(txn_internal->ws[i].msg));
      writable_buffer_deinit(&value_buf);
   }

   for (uint64 i = 0; i < txn_internal->rs_size; ++i) {
      writable_buffer key_buf;
      writable_buffer_init_with_buffer(
         &key_buf,
         0,
         slice_length(txn_internal->rs[i].key),
         (void *)slice_data(txn_internal->rs[i].key),
         slice_length(txn_internal->rs[i].key));
      writable_buffer_resize(&key_buf, slice_length(txn_internal->rs[i].key));
      writable_buffer_deinit(&key_buf);

      writable_buffer value_buf;
      writable_buffer_init_with_buffer(
         &value_buf,
         0,
         message_length(txn_internal->rs[i].msg),
         (void *)message_data(txn_internal->rs[i].msg),
         message_length(txn_internal->rs[i].msg));
      writable_buffer_resize(&value_buf,
                             message_length(txn_internal->rs[i].msg));
      writable_buffer_deinit(&value_buf);
   }

   platform_free(0, *internal_to_delete);
   internal_to_delete = NULL;
}

static int
transaction_compare(const void *a, const void *b, void *arg)
{
   const transaction_internal **ta = (const transaction_internal **)a;
   const transaction_internal **tb = (const transaction_internal **)b;
   return (*ta)->start_ts < (*tb)->start_ts;
}

uint64_t
transaction_hash(const void *item, uint64_t seed0, uint64_t seed1)
{
   const transaction_internal **txn = (const transaction_internal **)item;
   return hashmap_sip(*txn, sizeof(transaction_internal *), seed0, seed1);
}

void
transaction_table_init(transaction_table *active_transactions)
{
   active_transactions->table = hashmap_new(sizeof(transaction_internal *),
                                            0,
                                            0,
                                            0,
                                            transaction_hash,
                                            transaction_compare,
                                            NULL,
                                            NULL);

   platform_mutex_init(&active_transactions->lock, 0, 0);
}

void
transaction_table_deinit(transaction_table *active_transactions)
{
   platform_mutex_destroy(&active_transactions->lock);

   hashmap_free(active_transactions->table);
}

void
transaction_table_insert(transaction_table    *active_transactions,
                         transaction_internal *txn)
{
   platform_mutex_lock(&active_transactions->lock);
   hashmap_set(active_transactions->table, &txn);
   platform_mutex_unlock(&active_transactions->lock);
}

void
transaction_table_delete(transaction_table    *active_transactions,
                         transaction_internal *txn)
{
   platform_mutex_lock(&active_transactions->lock);
   hashmap_delete(active_transactions->table, &txn);
   platform_mutex_unlock(&active_transactions->lock);
}

/* For a reference, this logic is in this link:
 * https://courses.cs.washington.edu/courses/cse444/22wi/lectures/lecture16-18-transactions-optimistic-cc.pdf
 */
bool
transaction_check_for_conflict(transaction_table    *active_transactions,
                               transaction_internal *txn,
                               const data_config    *cfg)
{
   platform_mutex_lock(&active_transactions->lock);
   uint64 iter = 0;
   void  *item = NULL;

   while (hashmap_iter(active_transactions->table, &iter, &item)) {
      const transaction_internal *txn2 = *((const transaction_internal **)item);
      if (txn == txn2) {
         continue;
      }

      if (txn2->fin_ts > 0 && txn->start_ts > txn2->fin_ts) {
         continue;
      }

      for (uint64 i = 0; i < txn->rs_size; ++i) {
         for (uint64 j = 0; j < txn2->ws_size; ++j) {
            if (data_key_compare(cfg, txn->rs[i].key, txn2->ws[j].key) == 0) {
               // platform_default_log("overlap between rs and ws\n");
               // platform_default_log("txn->start_ts: %lu, txn2->fin_ts:
               // %lu\n", txn->start_ts, txn2->fin_ts);
               if (txn->start_ts < txn2->fin_ts) {
                  platform_mutex_unlock(&active_transactions->lock);
                  return TRUE;
               }
            }
         }
      }

      for (uint64 i = 0; i < txn->ws_size; ++i) {
         for (uint64 j = 0; j < txn2->ws_size; ++j) {
            if (data_key_compare(cfg, txn->ws[i].key, txn2->ws[j].key) == 0) {
               // platform_default_log("overlap between ws and ws\n");
               // platform_default_log("txn->val_ts: %lu, txn2->fin_ts: %lu\n",
               // txn->val_ts, txn2->fin_ts);
               if (txn->val_ts < txn2->fin_ts) {
                  platform_mutex_unlock(&active_transactions->lock);
                  return TRUE;
               }
            }
         }
      }
   }

   platform_mutex_unlock(&active_transactions->lock);
   return FALSE;
}
