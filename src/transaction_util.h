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

typedef struct transaction_table {
   struct hashmap *table;
} transaction_table;

typedef struct transaction_internal {
   timestamp start_tn;
   timestamp finish_tn;
   timestamp tn;

   transaction_rw_set_entry rs[TRANSACTION_RW_SET_MAX];
   transaction_rw_set_entry ws[TRANSACTION_RW_SET_MAX];

   uint64 rs_size;
   uint64 ws_size;

#ifdef PARALLEL_VALIDATION
   transaction_table finish_active_transactions;
#endif
} transaction_internal;

void
transaction_internal_create(transaction_internal **new_internal);
void
transaction_internal_destroy(transaction_internal **internal_to_delete);

void
transaction_table_init(transaction_table *transactions);

void
transaction_table_init_from_table(transaction_table       *transactions,
                                  const transaction_table *other);

void
transaction_table_deinit(transaction_table *transactions);

void
transaction_table_insert(transaction_table    *transactions,
                         transaction_internal *txn);

void
transaction_table_insert_table(transaction_table       *transactions,
                               const transaction_table *other);

void
transaction_table_delete(transaction_table    *transactions,
                         transaction_internal *txn);

bool
transaction_check_for_conflict(transaction_table    *transactions,
                               transaction_internal *txn,
                               const data_config    *cfg);

#ifdef PARALLEL_VALIDATION
bool
transaction_check_for_conflict_with_active_transactions(
   transaction_internal *txn,
   const data_config    *cfg);
#endif

#endif // _TRANSACTION_UTIL_H_
