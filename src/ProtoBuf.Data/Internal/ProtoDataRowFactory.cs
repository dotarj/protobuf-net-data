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
                NullColumns = new bool[totalNumberOfColumns].ToList(),
                BoolValues = CreateNumberOfFields<bool>(),
                ByteValues = CreateNumberOfFields<byte>(),
                DateTimeValues = CreateNumberOfFields<DateTime>(),
                FloatValues = CreateNumberOfFields<float>(),
                GuidValues = CreateNumberOfFields<Guid>(),
                Int16Values = CreateNumberOfFields<Int16>(),
                Int32Values = CreateNumberOfFields<Int32>(),
                Int64Values = CreateNumberOfFields<Int64>(),
                CharValues = CreateNumberOfFields<char>(),
                StringValues = CreateNumberOfStringFields(),
                DecimalValues = CreateNumberOfFields<decimal>(),
                DoubleValues = CreateNumberOfFields<double>()
            };
        }

        List<string> CreateNumberOfStringFields()
        {
            var count = numberOfColumnsPerType[typeof(string)];
            return Enumerable.Repeat("", count).ToList();
        }

        List<T> CreateNumberOfFields<T>() where T : struct
        {
            var count = numberOfColumnsPerType[typeof(T)];

            return new T[count].ToList();
        }
    }
}