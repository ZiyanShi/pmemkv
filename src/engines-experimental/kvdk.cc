// SPDX-License-Identifier: BSD-3-Clause
/* Copyright 2017-2021, Intel Corporation */

#include "kvdk.h"

#include <../out.h>

namespace pmem
{
namespace kv
{

status kvdk::status_mapper(storage_engine::Status s)
{
	switch(s)
	{
		case storage_engine::Status::Ok: return status::OK;
		case  storage_engine::Status::NotFound: return status::NOT_FOUND;
		default: return status::UNKNOWN_ERROR;
//		case  MemoryOverflow,
//		case  PmemOverflow,
//		case  NotSupported: return status::,
//		case  MapError,
//		case  BatchOverflow,
//		case  TooManyWriteThreads,
//		case  InvalidDataSize,
//		case  IOError,
//		case  InvalidConfiguration,
	}
}

kvdk::kvdk(std::unique_ptr<internal::config> cfg)
{
	LOG("Started ok");
	storage_engine::Configs engine_configs;
	engine_configs.pmem_file_size = cfg->get_size();
	engine_configs.pmem_segment_blocks = (1ull << 8);
	engine_configs.hash_bucket_num = (1ull << 10);

	auto status = storage_engine::Engine::Open(cfg->get_path(), &engine, engine_configs, stdout);
	(void)status;
	assert(status == storage_engine::Status::Ok);

}

kvdk::~kvdk()
{
	LOG("Stopped ok");
	delete engine;
}

std::string kvdk::name()
{
	return "kvdk";
}

status kvdk::count_all(std::size_t &cnt)
{
	LOG("count_all");

	cnt = 0;

	auto iter = engine->NewSortedIterator(collection);
	if(iter) {
		iter->SeekToFirst();
		if (iter->Valid()) {
			std::string tmp;
			if(engine->SGet(collection, iter->Key(), &tmp) == storage_engine::Status::NotFound)
				return status::OK;
		}
		for(;iter->Valid(); iter->Next()){
			cnt++;
		}
	}
	return status::OK;
}

status kvdk::count_above(string_view key, std::size_t &cnt)
{
	LOG("count_above for key=" << std::string(key.data(), key.size()));

	cnt = 0;

	return status::OK;
}

status kvdk::count_equal_above(string_view key, std::size_t &cnt)
{
	LOG("count_equal_above for key=" << std::string(key.data(), key.size()));

	cnt = 0;

	return status::OK;
}

status kvdk::count_equal_below(string_view key, std::size_t &cnt)
{
	LOG("count_equal_below for key=" << std::string(key.data(), key.size()));

	cnt = 0;

	return status::OK;
}

status kvdk::count_below(string_view key, std::size_t &cnt)
{
	LOG("count_below for key=" << std::string(key.data(), key.size()));

	cnt = 0;

	return status::OK;
}

status kvdk::count_between(string_view key1, string_view key2, std::size_t &cnt)
{
	LOG("count_between for key1=" << key1.data() << ", key2=" << key2.data());

	cnt = 0;

	return status::OK;
}

status kvdk::get_all(get_kv_callback *callback, void *arg)
{
	LOG("get_all");

	auto iter = engine->NewSortedIterator(collection);
	if(iter) {
		iter->SeekToFirst();
		for(;iter->Valid(); iter->Next()) {
			auto key = iter->Key();
			auto value = iter->Value();
			auto ret = callback(key.data(), key.size(), value.data(), value.size(), arg);
			if (ret != 0 )
				return status::STOPPED_BY_CB;
		}
		return status::OK;
	}
	return status::NOT_FOUND;
}

status kvdk::get_above(string_view key, get_kv_callback *callback, void *arg)
{
	LOG("get_above for key=" << std::string(key.data(), key.size()));

	return status::NOT_FOUND;
}

status kvdk::get_equal_above(string_view key, get_kv_callback *callback, void *arg)
{
	LOG("get_equal_above for key=" << std::string(key.data(), key.size()));

	return status::NOT_FOUND;
}

status kvdk::get_equal_below(string_view key, get_kv_callback *callback, void *arg)
{
	LOG("get_equal_below for key=" << std::string(key.data(), key.size()));

	return status::NOT_FOUND;
}

status kvdk::get_below(string_view key, get_kv_callback *callback, void *arg)
{
	LOG("get_below for key=" << std::string(key.data(), key.size()));

	return status::NOT_FOUND;
}

status kvdk::get_between(string_view key1, string_view key2,
			      get_kv_callback *callback, void *arg)
{
	LOG("get_between for key1=" << key1.data() << ", key2=" << key2.data());

	return status::NOT_FOUND;
}

status kvdk::exists(string_view key)
{
	LOG("exists for key=" << std::string(key.data(), key.size()));
	std::string value;
	return status_mapper(engine->SGet(collection, key, &value));
}

status kvdk::get(string_view key, get_v_callback *callback, void *arg)
{
	LOG("get key=" << std::string(key.data(), key.size()));

	std::string value;
	auto status = engine->SGet(collection, key, &value);
	if (status == storage_engine::Status::Ok){
		callback(value.data(), value.size(), arg);
	}
	return status_mapper(status);
}

status kvdk::put(string_view key, string_view value)
{
	LOG("put key=" << std::string(key.data(), key.size())
		       << ", value.size=" << std::to_string(value.size()));
	auto status = engine->SSet(collection ,key, value);

	return status_mapper(status);
}

status kvdk::remove(string_view key)
{
	LOG("remove key=" << std::string(key.data(), key.size()));
	auto status = exists(key);
	if (status == status::OK) {
		return status_mapper(engine->SDelete(collection, key));
	}
	return status;
}

internal::iterator_base *kvdk::new_iterator()
{
	LOG("create write iterator");

	return new kvdk_iterator{};
}

internal::iterator_base *kvdk::new_const_iterator()
{
	LOG("create read iterator");

	return new kvdk_iterator{};
}

/* required for logging */
std::string kvdk::kvdk_iterator::name()
{
	return "kvdk iterator";
}

status kvdk::kvdk_iterator::seek(string_view key)
{
	LOG("seek to key=" << std::string(key.data(), key.size()));

	return status::OK;
}

result<string_view> kvdk::kvdk_iterator::key()
{
	LOG("key");

	return status::NOT_FOUND;
}

result<pmem::obj::slice<const char *>>
kvdk::kvdk_iterator::read_range(size_t pos, size_t n)
{
	LOG("read_range, pos=" << pos << " n=" << n);

	return status::NOT_FOUND;
}

static factory_registerer register_kvdk(
	std::unique_ptr<engine_base::factory_base>(new kvdk_factory));

} // namespace kv
} // namespace pmem
