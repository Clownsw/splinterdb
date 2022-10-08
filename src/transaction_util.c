#include "transaction_util.h"
#include "platform_linux/poison.h"

void
transaction_internal_create(transaction_internal **new_internal)
{
   transaction_internal *txn_internal;
   txn_internal           = TYPED_ZALLOC(0, txn_internal);
   txn_internal->start_tn = txn_internal->finish_tn = txn_internal->tn = 0;
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
   *internal_to_delete = NULL;
}

static int
transaction_compare(const void *a, const void *b, void *arg)
{
   return *(uint64 *)a - *(uint64 *)b;
}

uint64_t
transaction_hash(const void *item, uint64_t seed0, uint64_t seed1)
{
   return hashmap_sip(item, sizeof(transaction_internal *), seed0, seed1);
}

void
transaction_table_init(transaction_table *transactions)
{
   transactions->table = hashmap_new(sizeof(transaction_internal *),
                                     0,
                                     0,
                                     0,
                                     transaction_hash,
                                     transaction_compare,
                                     NULL,
                                     NULL);
}

void
transaction_table_init_from_table(transaction_table       *transactions,
                                  const transaction_table *other)
{
   transaction_table_init(transactions);
   transaction_table_insert_table(transactions, other);
}

void
transaction_table_deinit(transaction_table *transactions)
{
   hashmap_free(transactions->table);
   transactions->table = NULL;
}

void
transaction_table_insert(transaction_table    *transactions,
                         transaction_internal *txn)
{
   if (hashmap_set(transactions->table, &txn) == NULL) {
      platform_assert(!hashmap_oom(transactions->table));
   }
}

void
transaction_table_insert_table(transaction_table       *transactions,
                               const transaction_table *other)
{
   uint64 iter = 0;
   void  *item = NULL;

   while (hashmap_iter(other->table, &iter, &item)) {
      transaction_table_insert(transactions, *(transaction_internal **)item);
   }
}

void
transaction_table_delete(transaction_table    *transactions,
                         transaction_internal *txn)
{
   hashmap_delete(transactions->table, &txn);
}

bool
transaction_check_for_conflict(transaction_table    *transactions,
                               transaction_internal *txn,
                               const data_config    *cfg)
{
   uint64 iter = 0;
   void  *item = NULL;

   timestamp earliest_start_tn_in_use = UINT64_MAX;

   while (hashmap_iter(transactions->table, &iter, &item)) {
      transaction_internal *txn_i = *((transaction_internal **)item);
      if (txn->start_tn >= txn_i->tn || txn->finish_tn < txn_i->tn) {
         continue;
      }

      for (uint64 i = 0; i < txn_i->rs_size; ++i) {
         for (uint64 j = 0; j < txn->ws_size; ++j) {
            if (data_key_compare(cfg, txn_i->rs[i].key, txn->ws[j].key) == 0) {
               return FALSE;
            }
         }
      }

      bool is_active_txn = txn_i->start_tn > 0 && txn_i->tn == 0;
      if (is_active_txn) {
         earliest_start_tn_in_use =
            MIN(earliest_start_tn_in_use, txn_i->start_tn);
      }
   }

   // GC

   iter = 0;
   item = NULL;

   while (hashmap_iter(transactions->table, &iter, &item)) {
      transaction_internal *txn_i      = *((transaction_internal **)item);
      bool                  need_to_gc = txn_i->tn < earliest_start_tn_in_use;
      if (need_to_gc) {
         hashmap_delete(transactions->table, &txn_i);
      }
   }

   return TRUE;
}

#ifdef PARALLEL_VALIDATION
// TODO: rename to the general
bool
transaction_check_for_conflict_with_active_transactions(
   transaction_internal *txn,
   const data_config    *cfg)
{
   uint64 iter = 0;
   void  *item = NULL;

   while (hashmap_iter(txn->finish_active_transactions.table, &iter, &item)) {
      transaction_internal *txn_i = *((transaction_internal **)item);

      for (uint64 i = 0; i < txn_i->ws_size; ++i) {
         for (uint64 j = 0; j < txn->rs_size; ++j) {
            if (data_key_compare(cfg, txn_i->ws[i].key, txn->rs[j].key) == 0) {
               return FALSE;
            }
         }

         for (uint64 j = 0; j < txn->ws_size; ++j) {
            if (data_key_compare(cfg, txn_i->ws[i].key, txn->ws[j].key) == 0) {
               return FALSE;
            }
         }
      }
   }

   return TRUE;
}
#endif