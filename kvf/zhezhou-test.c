#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <sched.h>
#include <pthread.h>
#include <string.h>
#include <unistd.h>

#include "kvf-test.h"
#include "kvf.h"
#include "kvf_api.h"

#include "zhezhou-test.h"

#define KeySizeLimit 256

/* #define POOL_SIZE 1048576
string_t POOL[POOL_SIZE];
string_t* POOL_GC[POOL_SIZE];
int POOL_GC_TOP = 0;

void string_t_allocator_init() {
	int i;
	for (i = 0; i < POOL_SIZE; i++) {
		POOL_GC[i] = &POOL[i];
	}
	POOL_GC_TOP = POOL_SIZE;
}

string_t* alloc_string_t() {
	return POOL_GC[__sync_add_and_fetch(&POOL_GC_TOP, -1)];
}

void free_string_t(string_t* p) {
	POOL_GC[__sync_fetch_and_add(&POOL_GC_TOP, 1)] = p;
} */

string_t* alloc_string_t() {
	return (string_t *)malloc(sizeof(string_t));
}

void free_string_t(string_t* p) {
	free((void *)p);
}

static string_t* HDFS_gen_str(char* c_str, int len, unsigned alloc_len) {
	string_t* s_str = alloc_string_t();
	s_str->len = len;
	posix_memalign((void**) &(s_str->data), NVMKV_KVF_SECTOR_SIZE, alloc_len);
	memset(s_str->data, 0, len);

	if (c_str != NULL){
		memcpy(s_str->data, c_str, s_str->len);
	}
	return s_str;
}

#define HDR_LEN 4

static string_t* HDFS_gen_str_with_len(char* c_str, unsigned int len) {
	string_t* s_str = alloc_string_t();
	int real_len = len + HDR_LEN;
	s_str->len = real_len;
	posix_memalign((void**) &(s_str->data), NVMKV_KVF_SECTOR_SIZE, NVMKV_KVF_MAX_VAL_LEN);
	memset(s_str->data, 0, real_len);

	// write length header without endian problem
	s_str->data[0] = (char)((len & 0x000000FFu) >> 0);
	s_str->data[1] = (char)((len & 0x0000FF00u) >> 8);
	s_str->data[2] = (char)((len & 0x00FF0000u) >> 16);
	s_str->data[3] = (char)((len & 0xFF000000u) >> 24);

	if (c_str != NULL){
		memcpy(s_str->data + HDR_LEN, c_str, s_str->len - HDR_LEN);
	}
	return s_str;
}

unsigned int HDFS_get_length(string_t* s) {
	unsigned int res = 0;
	if (s == NULL || s->len < HDR_LEN) {
		return 0;
	}
	res = res | (((unsigned int)(s->data[0]) & 0xFFu) << 0);
	res = res | (((unsigned int)(s->data[1]) & 0xFFu) << 8);
	res = res | (((unsigned int)(s->data[2]) & 0xFFu) << 16);
	res = res | (((unsigned int)(s->data[3]) & 0xFFu) << 24);
	return res;
	// return strlen(s->data + 4);
}

void FreeBlkData(char *blk) {
	free(blk - HDR_LEN);
}

static void HDFS_del_str(string_t* s_str) {
	if (s_str == NULL)
		return;
	free(s_str->data);
	free_string_t(s_str);
}

static void i_to_char(uint64_t int_in, char* char_in) {
	sprintf(char_in, "exblk_%d",int_in);
	return;
}

// New and init of the KVFBlockManager. 
// a pointer of new KVFBlockManager.
// Return error number(default 0).
int NewKVFBlockManager(char* name, struct KVFBlockManager* _ret) {
	_ret->currentID = 1;
	_ret->usage = 0;
    _ret->pool_name = 	malloc(strlen(name)+1);
    _ret->kvf = 		malloc(sizeof(kvf_type_t));
	_ret->pool = 		malloc(sizeof(pool_t));
	_ret->props = 		malloc(sizeof(kv_props_t));

	memcpy(_ret->pool_name, name, strlen(name)+1);
	memcpy(_ret->kvf, &nvmkv_kvlib_std, sizeof(kvf_type_t));
	memcpy(_ret->pool, &nvmkv_pool_std, sizeof(pool_t));

	kvf_load("");
	kvf_register(_ret->kvf);
	kvf_init(_ret->kvf, "");

	pool_create(_ret->kvf, _ret->pool_name, "kvfinitconf.txt", _ret->pool);

	return 0;
}

// Destroy KVFBlockManager.
// Return a error number(default is 0).
int DestroyKVFBlockManager(struct KVFBlockManager* kbm) {
	pool_destroy(kbm->pool);

	kvf_shutdown(kbm->kvf);
	kvf_unregister(kbm->kvf);
	kvf_unload();

	free(kbm->pool_name);
	free(kbm->kvf);
	free(kbm->pool);
	free(kbm->props);

	return 0;
}

// Get a value(char*) with a key(uint64_t)
// return a error number(default is 0, block not found is 1).
int GetBlock(struct KVFBlockManager* kbm, uint64_t id, char** blk, uint64_t *blkLen) {
	int ret = 0; 
	char id_char[KeySizeLimit];
	i_to_char(id, id_char);
	string_t*	key_id = HDFS_gen_str((char*)id_char, strlen(id_char), NVMKV_KVF_MAX_KEY_LEN);
	string_t*	val_emp = HDFS_gen_str(NULL, NVMKV_KVF_MAX_VAL_LEN, NVMKV_KVF_MAX_VAL_LEN);

	get(kbm->pool, key_id, val_emp, kbm->props, NULL);
	if(val_emp->data == NULL){
		ret = 1;
		HDFS_del_str(key_id);
		HDFS_del_str(val_emp);
	
		return ret;
	}

	HDFS_del_str(key_id);
	if (val_emp != NULL) {
		*blk = (char *)(val_emp->data + HDR_LEN);
		*blkLen = HDFS_get_length(val_emp);
	} else {
		*blkLen = 0;
	}
	free_string_t(val_emp);

	return ret;
}

// Set a value(char*) with a key(uint64_t), the old value of this key would lose.
// a error number(default is 0, block not found is 1, could not set block is 2).
int SetBlock(struct KVFBlockManager* kbm, uint64_t id, char* blk, uint64_t blkLen) {
	int error = 0;
	char id_char[KeySizeLimit];
	i_to_char(id, id_char);
	
	string_t*	key_id = HDFS_gen_str((char*)id_char, strlen(id_char), NVMKV_KVF_MAX_KEY_LEN);
	string_t*	val_input = HDFS_gen_str_with_len(blk, blkLen);;
	// string_t*	val_emp = HDFS_gen_str(NULL, NVMKV_KVF_MAX_VAL_LEN, NVMKV_KVF_MAX_VAL_LEN);

	// TODO: Getting before putting leads to a performance pitfall
	/* get(kbm->pool, key_id, val_emp, kbm->props, NULL);
	if(val_emp->data == NULL){
		HDFS_del_str(key_id);
		HDFS_del_str(val_input);
		HDFS_del_str(val_emp);
		return 1;
	} */

	put(kbm->pool, key_id, val_input, kbm->props, NULL);
	put(kbm->pool, key_id, val_input, kbm->props, NULL);

	HDFS_del_str(key_id);
	HDFS_del_str(val_input);
	// HDFS_del_str(val_emp);

	return 0;
}

// Remove a key-value-pair with a key(uint64_t).
// a error number(default is 0, block not found is 1, could not remove block is 2).
int RemoveBlock(struct KVFBlockManager* kbm, uint64_t id) {
	int error = 0;
	char id_char[KeySizeLimit];
	i_to_char(id, id_char);

	string_t*	key_id = HDFS_gen_str((char*)id_char, strlen(id_char), NVMKV_KVF_MAX_KEY_LEN);
	// string_t*	val_emp = HDFS_gen_str(NULL, NVMKV_KVF_MAX_VAL_LEN, NVMKV_KVF_MAX_VAL_LEN);

	// TODO: Getting before removing leads to a performance pitfall
	/* get(kbm->pool, key_id, val_emp, kbm->props, NULL);
	if(val_emp->data == NULL){
		HDFS_del_str(key_id);
		HDFS_del_str(val_emp);
	
		return 1;
	} */

	del(kbm->pool, key_id, kbm->props, NULL);

	__sync_add_and_fetch(&kbm->usage,-1);
	HDFS_del_str(key_id);
	// HDFS_del_str(val_emp);

	return 0;

}

// insert a key-value-pair. return the key.
// if fail, return a error number( 1 ).
int AllocBlock(struct KVFBlockManager* kbm, uint64_t* key) {
	int error = 0;
	uint64_t id = __sync_add_and_fetch(&kbm->currentID,1);
	__sync_add_and_fetch(&kbm->usage,1);
	*key = id;

	/* char* blk;
	char* id_buffer = (char*)malloc(KeySizeLimit);
	i_to_char(id, id_buffer);

	string_t*	key_id = HDFS_gen_str((char*)id_buffer, strlen(id_buffer), NVMKV_KVF_MAX_KEY_LEN);
	string_t*	val_input = HDFS_gen_str_with_len("", 0);

	put(kbm->pool, key_id, val_input, kbm->props, NULL);
	put(kbm->pool, key_id, val_input, kbm->props, NULL); // ?

	HDFS_del_str(key_id);
	HDFS_del_str(val_input); */
	return 0;

}

uint64_t Blocksize(struct KVFBlockManager* kbm) {
	uint64_t _ret = MLBMSizeLimit;
	return _ret;
}

int Blockstat(struct KVFBlockManager* kbm, uint64_t* total, uint64_t* used, uint64_t* free, uint64_t* avail) {
	*total = MLBMBlocks;
	*used = *(&kbm->usage);
	*free = MLBMBlocks - *used;
	*avail = *free;
	return 0;
}
