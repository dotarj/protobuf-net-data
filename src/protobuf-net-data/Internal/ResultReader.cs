// Copyright (c) Richard Dingwall, Arjen Post. See LICENSE in the project root for license information.

using System.IO;

namespace ProtoBuf.Data.Internal
{
    internal static class ResultReader
    {
        private const int NoneFieldHeader = 0;
        private const int ResultFieldHeader = 1;

        public static bool ReadResult(ProtoReaderContext context)
        {
            if (context.ReadFieldHeader() == NoneFieldHeader)
            {
                return false;
            }

            if (context.CurrentFieldHeader != ResultFieldHeader)
            {
                throw new InvalidDataException($"Field header '{ResultFieldHeader}' expected, actual '{context.CurrentFieldHeader}'.");
            }

            context.StartSubItem();

            if (context.ReadFieldHeader() == NoneFieldHeader)
            {
                // TODO: Clear buffer and reset columns.
                context.ReachedEndOfCurrentResult = true;
                context.EndSubItem();

                return true;
            }

            ColumnsReader.ReadColumns(context);

            context.ReachedEndOfCurrentResult = false;

            return true;
        }
    }
}
