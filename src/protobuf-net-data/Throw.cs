// Copyright (c) Richard Dingwall, Arjen Post. See LICENSE in the project root for license information.

using System;

namespace ProtoBuf.Data
{
    internal static class Throw
    {
        public static void IfNull<T>(T value, string parameterName)
        {
            if (value == null)
            {
                throw new ArgumentNullException(parameterName);
            }
        }
    }
}
