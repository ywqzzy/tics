#include <Functions/FunctionLeast.h>

namespace DB
{
void registerFunctionTiDBLeast(FunctionFactory & factory)
{
    factory.registerFunction<FunctionTiDBLeast>();
}

} // namespace DB