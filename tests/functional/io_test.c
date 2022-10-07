// Copyright 2018-2022 VMware, Inc.
// SPDX-License-Identifier: Apache-2.0

/*
 * io_test.c --
 *
 *     This file tests the IO sub-system interfaces used by SplinterDB.
 */

#include "platform.h"
#include "config.h"
#include "io.h"

// Function prototypes
static platform_status
test_sync_writes(platform_heap_id    hid,
                 io_config          *io_cfgp,
                 platform_io_handle *io_hdlp,
                 uint64              start_addr,
                 uint64              end_addr,
                 char                stamp_char);

static platform_status
test_sync_reads(platform_heap_id    hid,
                io_config          *io_cfgp,
                platform_io_handle *io_hdlp,
                uint64              start_addr,
                uint64              end_addr,
                char                stamp_char);

/*
 * ----------------------------------------------------------------------------
 * splinter_io_test() - Entry point 'main' for SplinterDB IO sub-system testing.
 *
 * This test exercises core IO interfaces that are used by SplinterDB
 * to verify that these interfaces work as expected when executed:
 *
 * - using threads, with Splinter using default malloc()-memory allocation
 * - using threads, with Splinter setup to use shared-memory
 * - using forked processes, with Splinter setup to use shared-memory
 *
 * The test design is quite simple:
 *
 * - Starting with a default IO configuration, create a device of some size
 *   and using Sync IO interfaces driven by the main program, fill the
 *   device out with some known data. Read back each page worth of data from
 *   the file using sync-read and verify correctness. This case covers the
 *   correctness check for basic sync read/write APIs driven by the main thread.
 *
 * - Using the data generated above, fire-up n-threads. Assign contiguous
 *   sections of the file to each thread. Each thread verifies previously
 *   written data using sync-read. Then, each thread writes new data to its
 *   section using sync-writes, and verifies using sync-reads.
 * ----------------------------------------------------------------------------
 */
int
splinter_io_test(int argc, char *argv[])
{

   uint64 heap_capacity = (256 * MiB); // small heap is sufficient.

   bool use_shmem = FALSE;

   // Create a heap for io system's memory allocation.
   platform_heap_handle hh  = NULL;
   platform_heap_id     hid = NULL;
   platform_status      rc  = platform_heap_create(
      platform_get_module_id(), heap_capacity, use_shmem, &hh, &hid);
   platform_assert_status_ok(rc);

   // Do minimal IO config setup, using default IO values.
   master_config master_cfg;
   io_config     io_cfg;

   // Initialize the IO sub-system configuration.
   config_set_defaults(&master_cfg);
   io_config_init(&io_cfg,
                  master_cfg.page_size,
                  master_cfg.extent_size,
                  master_cfg.io_flags,
                  master_cfg.io_perms,
                  master_cfg.io_async_queue_depth,
                  "splinter_io_apis_test.db");

   platform_default_log("Exercise IO sub-system test on device '%s'"
                        ", page_size=%lu, extent_size=%lu, async_queue_size=%lu"
                        ", kernel_queue_size=%lu, async_max_pages=%lu ...\n",
                        io_cfg.filename,
                        io_cfg.page_size,
                        io_cfg.extent_size,
                        io_cfg.async_queue_size,
                        io_cfg.kernel_queue_size,
                        io_cfg.async_max_pages);

   platform_io_handle *io_handle = TYPED_MALLOC(hid, io_handle);

   // Initialize the handle to the IO sub-system. A device with a small initial
   // size gets created here.
   rc = io_handle_init(io_handle, &io_cfg, hh, hid);
   if (!SUCCESS(rc)) {
      platform_error_log("Failed to initialize IO handle: %s\n",
                         platform_status_to_string(rc));
      goto io_free;
   }

   uint64 disk_size_MB = 256;
   uint64 disk_size    = (disk_size_MB * MiB); // bytes

   uint64 start_addr = 0;
   uint64 end_addr   = disk_size;

   // Basic exercise of sync write / read APIs, from main thread.
   test_sync_writes(hid, &io_cfg, io_handle, start_addr, end_addr, 'a');
   test_sync_reads(hid,  &io_cfg, io_handle, start_addr, end_addr, 'a');

   test_sync_write_reads_across_threads();

io_free:
   platform_free(hid, io_handle);
   platform_heap_destroy(&hh);

   return (SUCCESS(rc) ? 0 : -1);
}

/*
 * -----------------------------------------------------------------------------
 * test_sync_writes() - Write out a swath of disk using page-sized sync-write
 * IO.
 *
 * This routine is used to verify that basic sync-write API works as expected.
 * We just test that the IO succeeded; not the resulting contents. That
 *verification will be done by its sibling routine test_sync_reads().
 *
 * Parameters:
 *	io_cfg		- Ptr to IO config struct to use
 *  io_hdlp		- Platform-specific IO handle
 *	start_addr	- Start address to write from.
 *	end_addr	- End address to write to (< end_addr)
 *  stamp_char	- Character to write out, on each page
 * -----------------------------------------------------------------------------
 */
static platform_status
test_sync_writes(platform_heap_id    hid,
                 io_config          *io_cfgp,
                 platform_io_handle *io_hdlp,
                 uint64              start_addr,
                 uint64              end_addr,
                 char                stamp_char)
{
   platform_thread this_thread = platform_get_tid();

   int page_size = (int)io_cfgp->page_size;

   // Allocate a buffer to do page I/O
   char *buf = TYPED_ARRAY_ZALLOC(hid, buf, page_size);

   memset(buf, stamp_char, page_size);

   platform_status rc = STATUS_OK;

   io_handle *io_hdl = (io_handle *)io_hdlp;

   uint64 num_IOs = 0;
   // Iterate thru all pages and do the writes
   for (uint64 curr = start_addr; curr < end_addr; curr += page_size, num_IOs++)
   {
      rc = io_write(io_hdl, buf, page_size, curr);
      if (!SUCCESS(rc)) {
         platform_error_log("Write IO at addr %lu wrote %d bytes"
                            ", expected to write out %d bytes.\n",
                            curr,
                            io_hdl->nbytes_rw,
                            page_size);
         goto free_buf;
      }
   }

   platform_default_log("  %s(): Thread %lu performed %lu %dK page write IOs "
                        "from start addr=%lu through end addr=%lu\n",
                        __FUNCTION__,
                        this_thread,
                        num_IOs,
                        (int)(page_size / KiB),
                        start_addr,
                        end_addr);

free_buf:
   platform_free(hid, buf);
   return rc;
}

/*
 * -----------------------------------------------------------------------------
 * test_sync_reads() - Read a swath of disk using page-sized sync-read IO.
 *
 * This routine is used to verify that basic sync-read API works as expected.
 * test_sync_writes() has minted out a known character. Verify that every
 * read reads back the same contents in each page.
 *
 * Parameters:
 *	io_cfg		- Ptr to IO config struct to use
 *  io_hdlp		- Platform-specific IO handle
 *	start_addr	- Start address to write from.
 *	end_addr	- End address to write to (< end_addr)
 *  stamp_char	- Character that was written out, on each page
 * -----------------------------------------------------------------------------
 */
static platform_status
test_sync_reads(platform_heap_id    hid,
                io_config          *io_cfgp,
                platform_io_handle *io_hdlp,
                uint64              start_addr,
                uint64              end_addr,
                char                stamp_char)
{
   platform_thread this_thread = platform_get_tid();

   int page_size = (int)io_cfgp->page_size;

   // Allocate a buffer to do page I/O, and an expected results buffer
   char *buf = TYPED_ARRAY_ZALLOC(hid, buf, page_size);
   char *exp = TYPED_ARRAY_ZALLOC(hid, exp, page_size);
   memset(exp, stamp_char, page_size);

   platform_status rc = STATUS_OK;

   io_handle *io_hdl = (io_handle *)io_hdlp;

   uint64 num_IOs = 0;
   // Iterate thru all pages and do the writes
   for (uint64 curr = start_addr; curr < end_addr; curr += page_size, num_IOs++)
   {
      rc = io_read(io_hdl, buf, page_size, curr);
      if (!SUCCESS(rc)) {
         platform_error_log("Read IO at addr %lu read %d bytes"
                            ", expected to read %d bytes.\n",
                            curr,
                            io_hdl->nbytes_rw,
                            page_size);
         goto free_buf;
      }

      int rv = memcmp(exp, buf, page_size);
      if (rv != 0) {
         rc = STATUS_IO_ERROR;
         platform_error_log("Page IO at address=%lu is incorrect.\n", curr);
         goto free_buf;
      }
      // Clear out buffer for next page read.
      memset(buf, 'X', page_size);
   }

   platform_default_log("  %s():  Thread %lu performed %lu %dK page read  IOs "
                        "from start addr=%lu through end addr=%lu\n",
                        __FUNCTION__,
                        this_thread,
                        num_IOs,
                        (int)(page_size / KiB),
                        start_addr,
                        end_addr);

free_buf:
   platform_free(hid, buf);
   platform_free(hid, exp);
   return rc;
}
