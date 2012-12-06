// Copyright 2012 Richard Dingwall - http://richarddingwall.name
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

namespace ProtoBuf.Data.Internal
{
    internal class HeaderWriter
    {
        private readonly ProtoWriter writer;

        public HeaderWriter(ProtoWriter writer)
        {
            if (writer == null) throw new ArgumentNullException("writer");
            this.writer = writer;
        }

        public void WriteHeader(IEnumerable<ProtoDataColumn> columns)
        {
            if (columns == null) throw new ArgumentNullException("columns");

            foreach (var column in columns)
            {
                // for each, write the name and data type
                ProtoWriter.WriteFieldHeader(2, WireType.StartGroup, writer);
                var columnToken = ProtoWriter.StartSubItem(column, writer);
                ProtoWriter.WriteFieldHeader(1, WireType.String, writer);
                ProtoWriter.WriteString(column.ColumnName, writer);
                ProtoWriter.WriteFieldHeader(2, WireType.Variant, writer);
                ProtoWriter.WriteInt32((int)column.ProtoDataType, writer);
                ProtoWriter.EndSubItem(columnToken, writer);
            }
        }
    }
}