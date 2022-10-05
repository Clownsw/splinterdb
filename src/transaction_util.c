#include "transaction_util.h"
#include "data_internal.h"

static int
transaction_compare(const void *a, const void *b, void *arg)
{
   const transaction **ta = (const transaction **)a;
   const transaction **tb = (const transaction **)b;
   return (*ta)->start_ts < (*tb)->start_ts;
}

uint64_t
transaction_hash(const void *item, uint64_t seed0, uint64_t seed1)
{
   const transaction **txn = (const transaction **)item;
   return hashmap_sip(*txn, sizeof(transaction *), seed0, seed1);
}

void
transaction_table_init(transaction_table *active_transactions)
{
   active_transactions->table = hashmap_new(sizeof(transaction *),
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
transaction_table_insert(transaction_table *active_transactions,
                         transaction       *txn)
{
   platform_mutex_lock(&active_transactions->lock);
   hashmap_set(active_transactions->table, &txn);
   platform_mutex_unlock(&active_transactions->lock);
}

void
transaction_table_delete(transaction_table *active_transactions,
                         transaction       *txn)
{
   platform_mutex_lock(&active_transactions->lock);
   hashmap_delete(active_transactions->table, &txn);
   platform_mutex_unlock(&active_transactions->lock);
}

bool
transaction_check_for_conflict(transaction_table *active_transactions,
                               transaction       *txn,
                               const data_config *cfg)
{
   platform_mutex_lock(&active_transactions->lock);
   uint64 iter = 0;
   void  *item = NULL;

   while (hashmap_iter(active_transactions->table, &iter, &item)) {
      const transaction *txn2 = (const transaction *)item;
      if (txn == txn2) {
         continue;
      }

      for (uint64 i = 0; i < txn->rs_size; ++i) {
         for (uint64 j = 0; j < txn2->ws_size; ++j) {
            if (data_key_compare(cfg, txn->rs[i].key, txn2->ws[j].key) == 0) {
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