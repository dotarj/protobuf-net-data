// Copyright (c) Richard Dingwall, Arjen Post. See LICENSE in the project root for license information.

using System;
using System.Collections.Generic;
using System.IO;

namespace ProtoBuf.Data.Internal
{
    internal static class TypeHelper
    {
        private static readonly Type[] SupportedDataTypes = new[]
        {
            typeof(bool),
            typeof(byte),
            typeof(byte[]),
            typeof(char),
            typeof(char[]),
            typeof(DateTime),
            typeof(decimal),
            typeof(double),
            typeof(float),
            typeof(Guid),
            typeof(int),
            typeof(long),
            typeof(short),
            typeof(string),
            typeof(TimeSpan)
        };

        private static readonly IDictionary<Type, ProtoDataType> ProtoDataTypeByType = new Dictionary<Type, ProtoDataType>
        {
            { typeof(bool), ProtoDataType.Bool },
            { typeof(byte), ProtoDataType.Byte },
            { typeof(DateTime), ProtoDataType.DateTime },
            { typeof(double), ProtoDataType.Double },
            { typeof(float), ProtoDataType.Float },
            { typeof(Guid), ProtoDataType.Guid },
            { typeof(int), ProtoDataType.Int },
            { typeof(long), ProtoDataType.Long },
            { typeof(short), ProtoDataType.Short },
            { typeof(string), ProtoDataType.String },
            { typeof(char), ProtoDataType.Char },
            { typeof(decimal), ProtoDataType.Decimal },
            { typeof(byte[]), ProtoDataType.ByteArray },
            { typeof(char[]), ProtoDataType.CharArray },
            { typeof(TimeSpan), ProtoDataType.TimeSpan },
        };

        private static readonly IDictionary<ProtoDataType, Type> TypeByProtoDataType = new Dictionary<ProtoDataType, Type>
        {
            { ProtoDataType.Bool, typeof(bool) },
            { ProtoDataType.Byte, typeof(byte) },
            { ProtoDataType.DateTime, typeof(DateTime) },
            { ProtoDataType.Double, typeof(double) },
            { ProtoDataType.Float, typeof(float) },
            { ProtoDataType.Guid, typeof(Guid) },
            { ProtoDataType.Int, typeof(int) },
            { ProtoDataType.Long, typeof(long) },
            { ProtoDataType.Short, typeof(short) },
            { ProtoDataType.String, typeof(string) },
            { ProtoDataType.Char, typeof(char) },
            { ProtoDataType.Decimal, typeof(decimal) },
            { ProtoDataType.ByteArray, typeof(byte[]) },
            { ProtoDataType.CharArray, typeof(char[]) },
            { ProtoDataType.TimeSpan, typeof(TimeSpan) },
        };

        private static string supportedDataTypes;

        public static ProtoDataType GetProtoDataType(Type type)
        {
            ProtoDataType value;

            if (ProtoDataTypeByType.TryGetValue(type, out value))
            {
                return value;
            }

            throw new NotSupportedException($"The data type '{type.Name}' is not supported. The supported data types are: {GetSupportedDataTypes()}.");
        }

        public static Type GetType(ProtoDataType type)
        {
            Type value;

            if (TypeByProtoDataType.TryGetValue(type, out value))
            {
                return value;
            }

            throw new InvalidDataException($"Undefined ProtoDataType '{(int)type}'.");
        }

        public static string GetSupportedDataTypes()
        {
            var dataTypeNames = new string[TypeHelper.SupportedDataTypes.Length];

            for (var i = 0; i < TypeHelper.SupportedDataTypes.Length; i++)
            {
                dataTypeNames[i] = TypeHelper.SupportedDataTypes[i].Name;
            }

            return supportedDataTypes = string.Join(", ", dataTypeNames);
        }
    }
}
