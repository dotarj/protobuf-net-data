// Copyright 2011 Richard Dingwall - http://richarddingwall.name
// 
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// 
//     http://www.apache.org/licenses/LICENSE-2.0
// 
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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