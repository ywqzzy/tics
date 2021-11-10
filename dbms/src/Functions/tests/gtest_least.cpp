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

TEST_F(LeastTest, testTiDBLeast)
try
{
    const String & func_name = "tidbLeast";

    ASSERT_COLUMN_EQ(
        createColumn<Nullable<Int32>>({1}),
        executeFunction(
            func_name,
            createColumn<Nullable<Int32>>({1}),
            createColumn<Nullable<Int32>>({2}),
            createColumn<Nullable<Int32>>({3})
            ));
}
CATCH
} // namespace DB::tests
