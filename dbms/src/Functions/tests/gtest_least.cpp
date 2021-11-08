#include <Interpreters/Context.h>
#include <TestUtils/FunctionTestUtils.h>
#include <TestUtils/TiFlashTestBasic.h>

#include <string>
#include <vector>

namespace DB::tests
{
class LeastTest : public DB::tests::FunctionTest
{
};

TEST_F(LeastTest, testOnlyNull)
try
{
const String & func_name = "tidbLeast";

    ASSERT_COLUMN_EQ(
        createColumn<Nullable<int32>>({1}),
        executeFunction(
            func_name,
            createColumn<Nullable<int32>>({1}),
            createColumn<Nullable<int32>>({2}),
            createColumn<Nullable<int32>>({3}),
            ));
}
CATCH
} // namespace DB::tests
