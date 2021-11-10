#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeNullable.h>
#include <Functions/FunctionHelpers.h>
#include <Poco/String.h>
#include <ext/range.h>
#include <type_traits>

namespace DB
{
namespace ErrorCodes
{
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}


template <typename FromDataType, typename ToDataType>
struct LeastImpl
{
    // using FromFieldType = typename FromDataType::FieldType;
    // using ToFieldType = typename ToDataType::FieldType;


    static void execute(Block & block, const ColumnNumbers & arguments, size_t result)
    {
        if (arguments.size() == 2)
            executeBinary(block, arguments, result);
        else
            executeBinary(block, arguments, result);
    }

private:
    static void executeBinary(Block & block [[maybe_unused]], const ColumnNumbers & arguments [[maybe_unused]], const size_t result [[maybe_unused]])
    {
        // const ColumnVector<FromDataType> * col = checkAndGetColumn<ColumnVector<FromDataType>>(block.getByPosition(arguments[0]).column.get());
        // const ColumnVector<FromDataType> * col1 = checkAndGetColumn<ColumnVector<FromDataType>>(block.getByPosition(arguments[1]).column.get());
        {
            // using ResultType = FromDataType;

            // auto col_res = ColumnVector<ResultType>::create();

            // typename ColumnVector<ResultType>::Container & vec_res = col_res->getData();
            // vec_res.resize(col->getData().size());
            // // // todo change it..
            // // auto vec_a = col->getData();
            // // auto vec_b = col1->getData();
            // // size_t size = col->getData().size();
            // // for (size_t i = 0 ; i < size; ++i)
            // // {
            // //     vec_res[i] = static_cast<ResultType>(vec_a[i]) < static_cast<ResultType>(vec_b[i]) ? static_cast<ResultType>(vec_a[i]) : static_cast<ResultType>(vec_b[i]);
            // // }
            // block.getByPosition(result).column = std::move(col_res);
        }
    }

    // ywq todo implement NAry
    // void executeNAry(Block & block, const ColumnNumbers & arguments, const size_t result) const
    // {
    //     size_t num_sources = arguments.size();

    //     for (size_t i = 0; i < num_sources; ++i)
    //         sources[i] = createDynamicStringSource(*block.getByPosition(arguments[i]).column);

    //     auto c_res = ColumnString::create();
    //     concat(sources, StringSink(*c_res, block.rows()));
    //     block.getByPosition(result).column = std::move(c_res);
    // }
};

class FunctionTiDBLeast : public IFunction
{
private:
    const Context & context;

    struct NameTiDBLeast
    {
        static constexpr auto name = "tidbLeast";
    };

public:
    static constexpr auto name = NameTiDBLeast::name;
    FunctionTiDBLeast(const Context & context)
        : context(context)
    {}
    static FunctionPtr create(const Context & context)
    {
        return std::make_shared<FunctionTiDBLeast>(context);
    }

    String getName() const override { return name; }

    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }

    bool useDefaultImplementationForNulls() const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.size() < 1)
            throw Exception("Number of arguments for function " + getName() + " doesn't match: passed " + toString(arguments.size())
                                + ", should be at least 1.",
                            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
        DataTypePtr type_res;
        for (const auto arg_idx : ext::range(0, arguments.size()))
        {
            const auto & arg = arguments[arg_idx];
            if (!(checkType<DataTypeUInt8>(arg, type_res)
              || checkType<DataTypeUInt16>(arg, type_res)
              || checkType<DataTypeUInt32>(arg, type_res)
              || checkType<DataTypeUInt64>(arg, type_res)
              || checkType<DataTypeInt8>(arg, type_res)
              || checkType<DataTypeInt16>(arg, type_res)
              || checkType<DataTypeInt32>(arg, type_res)
              || checkType<DataTypeInt64>(arg, type_res)
              || checkType<DataTypeDecimal<Decimal32>>(arg, type_res)
              || checkType<DataTypeDecimal<Decimal64>>(arg, type_res)
              || checkType<DataTypeDecimal<Decimal128>>(arg, type_res)
              || checkType<DataTypeDecimal<Decimal256>>(arg, type_res)
              || checkType<DataTypeFloat32>(arg, type_res)
              || checkType<DataTypeFloat64>(arg, type_res)))
              // todo know the difference between types....
            throw Exception(
                "Illegal types " + arg->getName() + " of arguments of function " + getName(),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        }
        // Every column should be nullable
        return makeNullable(type_res);
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, const size_t result) const override
    {
        if (arguments.size() == 1)
        {
            const IColumn * c0 = block.getByPosition(arguments[0]).column.get();
            block.getByPosition(result).column = c0->cloneResized(c0->size());
        }
        else
        {
            DataTypes types;
            for (const auto i : ext::range(0, arguments.size()))
            {
                auto cur_arg = block.getByPosition(arguments[i]).type;
                types.push_back(cur_arg);
            }
            DataTypePtr result_type = getReturnTypeImpl(types);
            using ResultDataType = std::decay_t<decltype(result_type)>;
            const IDataType * from_type = block.getByPosition(arguments[0]).type.get();

            if (checkDataType<DataTypeUInt8>(from_type))
                return LeastImpl<DataTypeUInt8, ResultDataType>::execute(block, arguments, result);
            else if (checkDataType<DataTypeUInt16>(from_type))
                return LeastImpl<DataTypeUInt16, ResultDataType>::execute(block, arguments, result);
            else if (checkDataType<DataTypeUInt32>(from_type))
                return LeastImpl<DataTypeUInt32, ResultDataType>::execute(block, arguments, result);
            else if (checkDataType<DataTypeUInt64>(from_type))
                return LeastImpl<DataTypeUInt64, ResultDataType>::execute(block, arguments, result);
            else if (checkDataType<DataTypeInt8>(from_type))
                return LeastImpl<DataTypeInt8, ResultDataType>::execute(block, arguments, result);
            else if (checkDataType<DataTypeInt16>(from_type))
                return LeastImpl<DataTypeInt16, ResultDataType>::execute(block, arguments, result);
            else if (checkDataType<DataTypeInt32>(from_type))
                return LeastImpl<DataTypeInt32, ResultDataType>::execute(block, arguments, result);
            else if (checkDataType<DataTypeInt64>(from_type))
                return LeastImpl<DataTypeInt64, ResultDataType>::execute(block, arguments, result);
            else if (checkDataType<DataTypeFloat32>(from_type))
                return LeastImpl<DataTypeFloat32, ResultDataType>::execute(block, arguments, result);
            else if (checkDataType<DataTypeFloat64>(from_type))
                return LeastImpl<DataTypeFloat64, ResultDataType>::execute(block, arguments, result);
            else
            {
                throw Exception("Illegal type " + block.getByPosition(arguments[0]).type->getName() + " of argument of function " + getName(),
                            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
            }
        }
    }
private:
    template <typename T0>
    bool checkType(const DataTypePtr & arg, DataTypePtr & type_res) const 
    {
        if (typeid_cast<const T0 *>(arg.get())) {
            type_res = std::make_shared<T0>();
            return true;
        }
        return false;
    }
};

void registerFunctionIntLeast(FunctionFactory & factory)
{
    factory.registerFunction<FunctionTiDBLeast>();
}

} // namespace DB