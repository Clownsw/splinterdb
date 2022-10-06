// Copyright 2022 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

#ifndef _TRANSACTION_UTIL_H_
#define _TRANSACTION_UTIL_H_

#include "splinterdb/public_platform.h"
#include "splinterdb/data.h"
#include "splinterdb/transaction.h"
#include "hashmap.h"
#include "platform.h"

#define TRANSACTION_RW_SET_MAX 16

typedef struct transaction_rw_set_entry {
   slice   key;
   message msg;
} transaction_rw_set_entry;

typedef struct transaction_internal {
   timestamp start_tn;
   timestamp finish_tn;
   timestamp tn;

   transaction_rw_set_entry rs[TRANSACTION_RW_SET_MAX];
   transaction_rw_set_entry ws[TRANSACTION_RW_SET_MAX];

   uint64 rs_size;
   uint64 ws_size;
} transaction_internal;

void
transaction_internal_create(transaction_internal **new_internal);
void
transaction_internal_destroy(transaction_internal **internal_to_delete);

typedef struct transaction_table {
   struct hashmap *table;
   platform_mutex  lock;
} transaction_table;

void
transaction_table_init(transaction_table *active_transactions);

void
transaction_table_deinit(transaction_table *active_transactions);

void
transaction_table_insert(transaction_table    *active_transactions,
                         transaction_internal *txn);

void
transaction_table_delete(transaction_table    *active_transactions,
                         transaction_internal *txn);

bool
transaction_check_for_conflict(transaction_table    *active_transactions,
                               transaction_internal *txn,
                               const data_config    *cfg);


#endif // _TRANSACTION_UTIL_H_
