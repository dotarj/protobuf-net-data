// Copyright (c) Richard Dingwall, Arjen Post. See LICENSE in the project root for license information.

using System.Collections.Generic;
using System.IO;

namespace ProtoBuf.Data.Internal
{
    internal static class ColumnsReader
    {
        private const int NoneFieldHeader = 0;
        private const int ColumnFieldHeader = 2;
        private const int ColumnNameFieldHeader = 1;
        private const int ColumnTypeFieldHeader = 2;

        public static void ReadColumns(ProtoReaderContext context)
        {
            if (context.CurrentFieldHeader != ColumnFieldHeader)
            {
                throw new InvalidDataException($"Field header '{ColumnFieldHeader}' expected, actual '{context.CurrentFieldHeader}'.");
            }

            context.Columns = new List<ProtoBufDataColumn>(ReadColumnsImpl(context));
        }

        private static IEnumerable<ProtoBufDataColumn> ReadColumnsImpl(ProtoReaderContext context)
        {
            do
            {
                context.StartSubItem();

                var name = ReadColumnName(context);
                var protoDataType = ReadColumnType(context);

                // Backwards compatibility or unnecessary?
                while (context.ReadFieldHeader() != NoneFieldHeader)
                {
                    context.Reader.SkipField();
                }

                context.EndSubItem();

                yield return new ProtoBufDataColumn(name: name, dataType: TypeHelper.GetType(protoDataType), protoBufDataType: protoDataType);
            }
            while (context.ReadFieldHeader() == ColumnFieldHeader);
        }

        private static string ReadColumnName(ProtoReaderContext context)
        {
            context.ReadExpectedFieldHeader(ColumnNameFieldHeader);

            return context.Reader.ReadString();
        }

        private static ProtoDataType ReadColumnType(ProtoReaderContext context)
        {
            context.ReadExpectedFieldHeader(ColumnTypeFieldHeader);

            return (ProtoDataType)context.Reader.ReadInt32();
        }
    }
}
