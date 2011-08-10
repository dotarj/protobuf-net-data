using System;
using System.Collections.Generic;
using System.Linq;

namespace ProtoBuf.Data.Internal
{
    public class ProtoDataRowFactory
    {
        readonly IDictionary<Type, int> numberOfColumnsPerType;
        readonly int totalNumberOfColumns;

        public ProtoDataRowFactory(ProtoDataHeader header)
        {
            if (header == null) throw new ArgumentNullException("header");

            numberOfColumnsPerType = GetNumberOfColumnsPerClrType(header);

            totalNumberOfColumns = header.Columns.Count;
        }

        static IDictionary<Type, int> GetNumberOfColumnsPerClrType(ProtoDataHeader header)
        {
            var numberOfColumnsPerType =
                from protoType in ProtoDataTypes.AllTypes
                let columns = header.Columns.Where(c => c.ProtoDataType == protoType)
                let clrType = ConvertProtoDataType.ToClrType(protoType)
                select new {Type = clrType, Count = columns.Count()};
                
            return numberOfColumnsPerType.ToDictionary(k => k.Type, v => v.Count);
        }

        public ProtoDataRow CreateRow()
        {
            return new ProtoDataRow
            {
                NullColumns = Enumerable.Repeat(default(bool), totalNumberOfColumns).ToList(),
                BoolValues = CreateNumberOfFields<bool>(numberOfColumnsPerType),
                ByteValues = CreateNumberOfFields<byte>(numberOfColumnsPerType),
                DateTimeValues = CreateNumberOfFields<DateTime>(numberOfColumnsPerType),
                FloatValues = CreateNumberOfFields<float>(numberOfColumnsPerType),
                GuidValues = CreateNumberOfFields<Guid>(numberOfColumnsPerType),
                Int16Values = CreateNumberOfFields<Int16>(numberOfColumnsPerType),
                Int32Values = CreateNumberOfFields<Int32>(numberOfColumnsPerType),
                Int64Values = CreateNumberOfFields<Int64>(numberOfColumnsPerType),
                CharValues = CreateNumberOfFields<char>(numberOfColumnsPerType),
                StringValues = CreateNumberOfStringFields(numberOfColumnsPerType),
                DecimalValues = CreateNumberOfFields<decimal>(numberOfColumnsPerType),
                DoubleValues = CreateNumberOfFields<double>(numberOfColumnsPerType)
            };
        }

        static List<string> CreateNumberOfStringFields(IDictionary<Type, int> numberOfColumnsPerType)
        {
            var count = numberOfColumnsPerType[typeof(string)];
            return Enumerable.Repeat("", count).ToList();
        }

        static List<T> CreateNumberOfFields<T>(IDictionary<Type, int> numberOfColumnsPerType) where T : struct
        {
            var count = numberOfColumnsPerType[typeof(T)];

            return Enumerable.Repeat(default(T), count).ToList();
        }
    }
}