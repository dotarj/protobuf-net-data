// Copyright (c) Richard Dingwall, Arjen Post. See LICENSE in the project root for license information.

namespace ProtoBuf.Data.Internal
{
    internal static class RecordReader
    {
        private const int NoneFieldHeader = 0;

        public static bool ReadRecord(ProtoReaderContext context)
        {
            if (context.ReachedEndOfCurrentResult)
            {
                return false;
            }

            if (context.CurrentFieldHeader == 0)
            {
                context.EndSubItem();

                context.ReachedEndOfCurrentResult = true;

                return false;
            }

            if (context.Buffers == null)
            {
                context.Buffers = new ProtoBufDataBuffer[context.Columns.Count];

                ProtoBufDataBuffer.Initialize(context.Buffers);
            }
            else
            {
                ProtoBufDataBuffer.Clear(context.Buffers);
            }

            context.StartSubItem();

            ReadRecordValues(context);

            context.EndSubItem();

            context.ReadFieldHeader();

            return true;
        }

        private static void ReadRecordValues(ProtoReaderContext context)
        {
            while (context.ReadFieldHeader() != NoneFieldHeader)
            {
                // Backwards compatibility or unnecessary?
                if (context.CurrentFieldHeader > context.Buffers.Length)
                {
                    context.Reader.SkipField();

                    continue;
                }

                var columnIndex = context.CurrentFieldHeader - 1;

                switch (context.Columns[columnIndex].ProtoDataType)
                {
                    case ProtoDataType.String:
                        context.Buffers[columnIndex].String = context.Reader.ReadString();
                        context.Buffers[columnIndex].IsNull = false;
                        break;
                    case ProtoDataType.DateTime:
                        context.Buffers[columnIndex].DateTime = BclHelpers.ReadDateTime(context.Reader);
                        context.Buffers[columnIndex].IsNull = false;
                        break;
                    case ProtoDataType.Int:
                        context.Buffers[columnIndex].Int = context.Reader.ReadInt32();
                        context.Buffers[columnIndex].IsNull = false;
                        break;
                    case ProtoDataType.Long:
                        context.Buffers[columnIndex].Long = context.Reader.ReadInt64();
                        context.Buffers[columnIndex].IsNull = false;
                        break;
                    case ProtoDataType.Short:
                        context.Buffers[columnIndex].Short = context.Reader.ReadInt16();
                        context.Buffers[columnIndex].IsNull = false;
                        break;
                    case ProtoDataType.Bool:
                        context.Buffers[columnIndex].Bool = context.Reader.ReadBoolean();
                        context.Buffers[columnIndex].IsNull = false;
                        break;
                    case ProtoDataType.Byte:
                        context.Buffers[columnIndex].Byte = context.Reader.ReadByte();
                        context.Buffers[columnIndex].IsNull = false;
                        break;
                    case ProtoDataType.Float:
                        context.Buffers[columnIndex].Float = context.Reader.ReadSingle();
                        context.Buffers[columnIndex].IsNull = false;
                        break;
                    case ProtoDataType.Double:
                        context.Buffers[columnIndex].Double = context.Reader.ReadDouble();
                        context.Buffers[columnIndex].IsNull = false;
                        break;
                    case ProtoDataType.Guid:
                        context.Buffers[columnIndex].Guid = BclHelpers.ReadGuid(context.Reader);
                        context.Buffers[columnIndex].IsNull = false;
                        break;
                    case ProtoDataType.Char:
                        context.Buffers[columnIndex].Char = (char)context.Reader.ReadInt16();
                        context.Buffers[columnIndex].IsNull = false;
                        break;
                    case ProtoDataType.Decimal:
                        context.Buffers[columnIndex].Decimal = BclHelpers.ReadDecimal(context.Reader);
                        context.Buffers[columnIndex].IsNull = false;
                        break;
                    case ProtoDataType.ByteArray:
                        context.Buffers[columnIndex].ByteArray = ProtoReader.AppendBytes(null, context.Reader);
                        context.Buffers[columnIndex].IsNull = false;
                        break;
                    case ProtoDataType.CharArray:
                        context.Buffers[columnIndex].CharArray = context.Reader.ReadString().ToCharArray();
                        context.Buffers[columnIndex].IsNull = false;
                        break;
                    case ProtoDataType.TimeSpan:
                        context.Buffers[columnIndex].TimeSpan = BclHelpers.ReadTimeSpan(context.Reader);
                        context.Buffers[columnIndex].IsNull = false;
                        break;
                }
            }
        }
    }
}
