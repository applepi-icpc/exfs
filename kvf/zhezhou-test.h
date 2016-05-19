#ifndef _ZHEZHOU_TEST_H_
#define _ZHEZHOU_TEST_H_
#define _GNU_SOURCE
#include "stdio.h"
#include "stdlib.h"
#include "sched.h"
#include "pthread.h"

#include "kvf-test.h"
#include "kvf.h"
#include "kvf_api.h"

#define MLBMSizeLimit  524288
#define MLBMBlocks     4194304


struct KVFBlockManager{
	char*		pool_name;
	kvf_type_t* 	kvf;
	pool_t*         pool;
	kv_props_t*	props;
	uint64_t  	currentID;	
	uint64_t	usage;
	uint64_t	key_num;	
};

int NewKVFBlockManager(char* name, struct KVFBlockManager* _ret);

int DestroyKVFBlockManager(struct KVFBlockManager* kbm);

// It's caller's responsibility to FreeBlkData(*blk)
int GetBlock(struct KVFBlockManager* kbm, uint64_t id, char** blk, uint64_t *blkLen);

int SetBlock(struct KVFBlockManager* kbm, uint64_t id, char* blk, uint64_t blkLen);

void FreeBlkData(char *blk);

int RemoveBlock(struct KVFBlockManager* kbm, uint64_t id);

int AllocBlock(struct KVFBlockManager* kbm, uint64_t* key);

uint64_t Blocksize(struct KVFBlockManager* kbm);

int Blockstat(struct KVFBlockManager* kbm, uint64_t* total, uint64_t* used, uint64_t* free, uint64_t* avail);

#endif

