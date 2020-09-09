// SPDX-License-Identifier: BSD-3-Clause
/* Copyright 2019-2020, Intel Corporation */

/*
 * pmemkv_basic.c -- example usage of pmemkv.
 */

#include <assert.h>
#include <libpmemkv.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define LOG(msg) puts(msg)
#define MAX_VAL_LEN 64

static const uint64_t SIZE = 1024UL * 1024UL * 1024UL;

int get_kv_callback(const char *k, size_t kb, const char *value, size_t value_bytes,
		    void *arg)
{
	printf("   visited: %s\n", k);

	return 0;
}

int update_callback(char *value, size_t valuebytes, void *arg)
{
	memset(value, 'D', valuebytes);
	return 0;
}

int main(int argc, char *argv[])
{
	if (argc < 2) {
		fprintf(stderr, "Usage: %s file\n", argv[0]);
		exit(1);
	}

	/* See libpmemkv_config(3) for more detailed example of config creation */
	LOG("Creating config");
	pmemkv_config *cfg = pmemkv_config_new();
	assert(cfg != NULL);

	int s = pmemkv_config_put_string(cfg, "path", argv[1]);
	assert(s == PMEMKV_STATUS_OK);
	s = pmemkv_config_put_uint64(cfg, "size", SIZE);
	assert(s == PMEMKV_STATUS_OK);
	s = pmemkv_config_put_uint64(cfg, "force_create", 1);
	assert(s == PMEMKV_STATUS_OK);

	LOG("Opening pmemkv database with 'cmap' engine");
	pmemkv_db *db = NULL;
	s = pmemkv_open("cmap", cfg, &db);
	assert(s == PMEMKV_STATUS_OK);
	assert(db != NULL);

	LOG("Putting new key");
	const char *key1 = "key1";
	const char *value1 = "value1";
	s = pmemkv_put(db, key1, strlen(key1), value1, strlen(value1));
	assert(s == PMEMKV_STATUS_OK);

	size_t cnt;
	s = pmemkv_count_all(db, &cnt);
	assert(s == PMEMKV_STATUS_OK);
	assert(cnt == 1);

	LOG("Reading key back");
	char val[MAX_VAL_LEN];
	s = pmemkv_get_copy(db, key1, strlen(key1), val, MAX_VAL_LEN, NULL);
	assert(s == PMEMKV_STATUS_OK);
	assert(!strcmp(val, "value1"));

	LOG("Iterating existing keys");
	const char *key2 = "key2";
	const char *value2 = "value2";
	const char *key3 = "key3";
	const char *value3 = "value3";
	pmemkv_put(db, key2, strlen(key2), value2, strlen(value2));
	pmemkv_put(db, key3, strlen(key3), value3, strlen(value3));
	pmemkv_get_all(db, &get_kv_callback, NULL);

	LOG("Updating key");
	s = pmemkv_update(db, key1, strlen(key1), 0, 2, update_callback, NULL);
	assert(s == PMEMKV_STATUS_OK);

	LOG("Reading changed key back");
	s = pmemkv_get_copy(db, key1, strlen(key1), val, MAX_VAL_LEN, NULL);
	assert(s == PMEMKV_STATUS_OK);
	assert(!strcmp(val, "DDlue1"));

	LOG("Removing existing key");
	s = pmemkv_remove(db, key1, strlen(key1));
	assert(s == PMEMKV_STATUS_OK);
	assert(pmemkv_exists(db, key1, strlen(key1)) == PMEMKV_STATUS_NOT_FOUND);

	LOG("Defragmenting the database");
	s = pmemkv_defrag(db, 0, 100);
	assert(s == PMEMKV_STATUS_OK);

	LOG("Closing database");
	pmemkv_close(db);

	return 0;
}
