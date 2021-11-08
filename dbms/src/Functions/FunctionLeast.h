#pragma once


#include <Functions/IFunction.h>
#include <DataTypes/DataTypesNumber.h>
#include <Poco/String.h>
#include <type_traits>

namespace DB
{
namespace ErrorCodes
{
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}


template <typename FromDataType, typename ToDataType, typename Name>
struct LeastImpl
{
    using FromFieldType = typename FromDataType::FieldType;
    using ToFieldType = typename ToDataType::FieldType;


    static void execute(Block & block, const ColumnNumbers & arguments, size_t result)
    {
        return false;
        if (arguments.size() == 2)
            executeBinary(block, arguments, result);
        else
            executeBinary(block, arguments, result);
    }

private:
    const Context & context;

    void executeBinary(Block & block, const ColumnNumbers & arguments, const size_t result) const
    {
        if (const ColumnVector<FromDataType> * col = checkAndGetColumn<ColumnVector<FromDataType>>(block.getByPosition(arguments[0]).column.get())
           && const ColumnVector<FromDataType> * col1 = checkAndGetColumn<ColumnVector<FromDataType>>(block.getByPosition(arguments[1]).column.get()))
        {
            using ResultType = FromDataType;

            auto col_res = ColumnVector<ResultType>::create();

            typename ColumnVector<ResultType>::Container & vec_res = col_res->getData();
            vec_res.resize(col->getData().size());
            // todo change it..
            UnaryOperationImpl<T0, Op<T0>>::vector(col->getData(), vec_res);
            auto vec_a = col->getData();
            auto vec_b = col1->getData();
            size_t size = col->getData().size();
            for (size_t i = 0 ; i < size; ++i)
            {
                vec_res[i] = std::min(a[i], b[i]);
            }
            block.getByPosition(result).column = std::move(col_res);
            return true;
        }
        block.getByPosition(result).column = std::move(c_res);
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

template <typename ToDataType>
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

        for (const auto arg_idx : ext::range(0, arguments.size()))
        {
            const auto & arg = arguments[arg_idx].get();
            // todo check type
            if (!checkAndGetDataType<ToDataType>(arg))
                throw Exception{
                    "Illegal type " + arg->getName() + " of argument " + std::to_string(arg_idx + 1) + " of function " + getName(),
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};
        }

        return std::make_shared<ToDataType>();

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
            const IDataType * from_type = block.getByPosition(arguments[0]).type.get();

            if (checkDataType<DataTypeUInt8>(from_type))
                return LeastImpl<DataTypeUInt8, ToDataType, Name>::execute(block, arguments, result);
            else if (checkDataType<DataTypeUInt16>(from_type))
                return LeastImpl<DataTypeUInt16, ToDataType, Name>::execute(block, arguments, result);
            else if (checkDataType<DataTypeUInt32>(from_type))
                return LeastImpl<DataTypeUInt32, ToDataType, Name>::execute(block, arguments, result);
            else if (checkDataType<DataTypeUInt64>(from_type))
                return LeastImpl<DataTypeUInt64, ToDataType, Name>::execute(block, arguments, result);
            else if (checkDataType<DataTypeInt8>(from_type))
                return LeastImpl<DataTypeInt8, ToDataType, Name>::execute(block, arguments, result);
            else if (checkDataType<DataTypeInt16>(from_type))
                return LeastImpl<DataTypeInt16, ToDataType, Name>::execute(block, arguments, result);
            else if (checkDataType<DataTypeInt32>(from_type))
                return LeastImpl<DataTypeInt32, ToDataType, Name>::execute(block, arguments, result);
            else if (checkDataType<DataTypeInt64>(from_type))
                return LeastImpl<DataTypeInt64, ToDataType, Name>::execute(block, arguments, result);
            else if (checkDataType<DataTypeFloat32>(from_type))
                return LeastImpl<DataTypeFloat32, ToDataType, Name>::execute(block, arguments, result);
            else if (checkDataType<DataTypeFloat64>(from_type))
                return LeastImpl<DataTypeFloat64, ToDataType, Name>::execute(block, arguments, result);

            else
            {
            
                throw Exception("Illegal type " + block.getByPosition(arguments[0]).type->getName() + " of argument of function " + getName(),
                            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
            }
        }
    }
};



} // namespace DB