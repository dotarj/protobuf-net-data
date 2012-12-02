using System.Data;
using System.IO;
using NUnit.Framework;
using SharpTestsEx;

namespace ProtoBuf.Data.Tests
{
    public class ProtoDataStreamTests
    {
        [TestFixture]
        public class When_reading_from_a_stream
        {
            private byte[] expectedBytes;
            private byte[] actualBytes;

            [TestFixtureSetUp]
            public void SetUp()
            {
                using (var reader = GetData())
                using (var memoryStream = new MemoryStream())
                {
                    DataSerializer.Serialize(memoryStream, reader);
                    expectedBytes = memoryStream.GetBuffer();
                }

                using (var reader = GetData())
                using (var stream = new ProtoDataStream(reader))
                using (var memoryStream = new MemoryStream())
                {
                    stream.CopyTo(memoryStream);
                    actualBytes = memoryStream.GetBuffer();
                }
            }

            private static IDataReader GetData()
            {
                return TestData.SmallDataTable().CreateDataReader();
            }

            [Test]
            public void It_should_be_binary_equal_to_the_data_serializer_version()
            {
                actualBytes.Should().Have.SameSequenceAs(expectedBytes);
            }
        }
    }
}