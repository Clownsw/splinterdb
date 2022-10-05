// Copyright 2022 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

#ifndef _TRANSACTION_UTIL_H_
#define _TRANSACTION_UTIL_H_

#include "splinterdb/public_platform.h"
#include "splinterdb/data.h"
#include "splinterdb/transaction.h"
#include "hashmap.h"
#include "platform.h"

typedef struct transaction_table {
   struct hashmap *table;
   platform_mutex  lock;
} transaction_table;

void
transaction_table_init(transaction_table *active_transactions);

void
transaction_table_deinit(transaction_table *active_transactions);

void
transaction_table_insert(transaction_table *active_transactions,
                         transaction       *txn);

void
transaction_table_delete(transaction_table *active_transactions,
                         transaction       *txn);

bool
transaction_check_for_conflict(transaction_table *active_transactions,
                               transaction       *txn,
                               const data_config *cfg);


#endif // _TRANSACTION_UTIL_H_
