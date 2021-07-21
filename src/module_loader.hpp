#include <iostream>
#include <string>

#include <dlfcn.h>
#include <memory>

template <typename BaseModule>
class module_loader {
private:
	void *handle;
	std::string path_to_lib_;
	std::string ctor_symbol;

	void open()
	{
		if (!(handle = dlopen(path_to_lib_.c_str(), RTLD_NOW | RTLD_LAZY))) {
			std::cerr << dlerror() << std::endl;
		}
	}

	void close()
	{
		if (dlclose(handle) != 0) {
			std::cerr << dlerror() << std::endl;
		}
	}

public:
	module_loader(std::string path_to_lib) : path_to_lib_(path_to_lib)
	{
		ctor_symbol = "module_ctor";

		open();
	}

	~module_loader()
	{
		close();
	}

	std::unique_ptr<BaseModule> get_instance()

	{

		using base_module_ctor_ptr = BaseModule *(*)();

		auto module_ctor = reinterpret_cast<base_module_ctor_ptr>(dlsym(handle, ctor_symbol.c_str()));

		if (!module_ctor) {
			close();
			throw std::runtime_error(dlerror());
		}

		return std::unique_ptr<BaseModule>(module_ctor());
	}
};
