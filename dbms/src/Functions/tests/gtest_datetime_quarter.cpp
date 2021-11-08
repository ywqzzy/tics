#include <Columns/ColumnConst.h>
#include <Columns/ColumnString.h>
#include <Common/Exception.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsDateTime.h>
#include <Interpreters/Context.h>
#include <TestUtils/FunctionTestUtils.h>
#include <TestUtils/TiFlashTestBasic.h>

#include <string>
#include <vector>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wsign-compare"
#include <Poco/Types.h>

#pragma GCC diagnostic pop

namespace DB
{
namespace tests
{
class TestDateTimeQuarter : public DB::tests::FunctionTest
{
};


TEST_F(TestDateTimeQuarter, Quarter)
try
{ 
    const String func_name = "toQuarter";


    MyDateTime datetime_value1(2021, 1, 29, 12, 34, 56, 123456);

    auto col_datetime1 = ColumnUInt64::create();
    {
        col_datetime1->insert(Field(datetime_value1.toPackedUInt()));
    }
    ColumnWithTypeAndName datetime_ctn1
            = ColumnWithTypeAndName(std::move(col_datetime1), std::make_shared<DataTypeMyDateTime>(), "datetime_value");
        

    ASSERT_COLUMN_EQ(
        createColumn<UInt8>({1}),
        executeFunction(func_name, datetime_ctn1));

    MyDateTime datetime_value2(2021, 5, 19, 12, 34, 56, 123456);

    auto col_datetime2 = ColumnUInt64::create();
    {
        col_datetime2->insert(Field(datetime_value2.toPackedUInt()));
    }
    ColumnWithTypeAndName datetime_ctn2
            = ColumnWithTypeAndName(std::move(col_datetime2), std::make_shared<DataTypeMyDateTime>(), "datetime_value");
        

    ASSERT_COLUMN_EQ(
        createColumn<UInt8>({2}),
        executeFunction(func_name, datetime_ctn2));    
}
CATCH

} // namespace test
} // namespace DB
