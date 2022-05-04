/*
 * QUANTCONNECT.COM - Democratizing Finance, Empowering Individuals.
 * Lean Algorithmic Trading Engine v2.0. Copyright 2014 QuantConnect Corporation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
*/

using System;
using ProtoBuf;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using ProtoBuf.Meta;
using Newtonsoft.Json;
using NUnit.Framework;
using QuantConnect;
using QuantConnect.Data;
using QuantConnect.DataSource;

namespace QuantConnect.DataLibrary.Tests
{
    [TestFixture]
    public class BrainDataTests
    {
        [Test]
        public void Thing()
        {
            var factory = new BrainCompanyFilingLanguageMetrics10K();

            var lines = new List<string>{
                "20210101,2021-01-01,20210101,869,516,0.1196,-1.6457,-1.5827,869,516,0.1196,-1.6457,-1.5827,869,516,0.1196,-1.6457,-1.5827,869,516,0.1196,-1.6457,-1.5827,869,516,0.1196,-1.6457,-1.5827,869,516,0.1196,-1.6457,-1.5827,869,516,0.1196,-1.6457,2021-01-01,869,516,0.1196,-1.6457,-1.5827,869,516,0.1196,-1.6457,-1.5827,869,516,0.1196,-1.6457,-1.5827,869,516,0.1196,-1.6457,-1.5827,869,516,0.1196,-1.6457,-1.5827,869,516,0.1196,-1.6457,-1.5827",
                "20210104,2021-01-04,20210104,702,432,0.1279,-2.3979,-2.1557,702,432,0.1279,-2.3979,-2.1557,702,432,0.1279,-2.3979,-2.1557,702,432,0.1279,-2.3979,-2.1557,702,432,0.1279,-2.3979,-2.1557,702,432,0.1279,-2.3979,-2.1557,702,432,0.1279,-2.3979,2021-01-04,702,432,0.1279,-2.3979,-2.1557,702,432,0.1279,-2.3979,-2.1557,702,432,0.1279,-2.3979,-2.1557,702,432,0.1279,-2.3979,-2.1557,702,432,0.1279,-2.3979,-2.1557,702,432,0.1279,-2.3979,-2.1557",
                "20210105,2021-01-05,20210105,691,414,0.0959,-2.4157,-2.2633,691,414,0.0959,-2.4157,-2.2633,691,414,0.0959,-2.4157,-2.2633,691,414,0.0959,-2.4157,-2.2633,691,414,0.0959,-2.4157,-2.2633,691,414,0.0959,-2.4157,-2.2633,691,414,0.0959,-2.4157,2021-01-05,691,414,0.0959,-2.4157,-2.2633,691,414,0.0959,-2.4157,-2.2633,691,414,0.0959,-2.4157,-2.2633,691,414,0.0959,-2.4157,-2.2633,691,414,0.0959,-2.4157,-2.2633,691,414,0.0959,-2.4157,-2.2633"
                };

            var config = new SubscriptionDataConfig(
                typeof(BrainCompanyFilingLanguageMetricsAll),
                Symbol.Create("AAPL", SecurityType.Base, Market.USA, baseDataType: typeof(BrainCompanyFilingLanguageMetricsAll)),
                Resolution.Daily,
                TimeZones.Utc,
                TimeZones.Utc,
                false,
                false,
                false,
                true,
                null
            );

            foreach (var line in lines)
            {
                var date = DateTime.ParseExact(line.Split(",").First(), "yyyyMMdd", CultureInfo.InvariantCulture);
                var a = factory.Reader(config, line, date, false);
                Console.WriteLine(a.ToString());
            }
        }
        
        [Test]
        public void JsonRoundTrip()
        {
            var expected = CreateNewInstance();
            var type = expected.GetType();
            var serialized = JsonConvert.SerializeObject(expected);
            var result = JsonConvert.DeserializeObject(serialized, type);

            AssertAreEqual(expected, result);
        }

        [Test]
        public void ProtobufRoundTrip()
        {
            var expected = CreateNewInstance();
            var type = expected.GetType();

            RuntimeTypeModel.Default[typeof(BaseData)].AddSubType(2000, type);

            using (var stream = new MemoryStream())
            {
                Serializer.Serialize(stream, expected);

                stream.Position = 0;

                var result = Serializer.Deserialize(type, stream);

                AssertAreEqual(expected, result, filterByCustomAttributes: true);
            }
        }

        [Test]
        public void Clone()
        {
            var expected = CreateNewInstance();
            var result = expected.Clone();

            AssertAreEqual(expected, result);
        }

        private void AssertAreEqual(object expected, object result, bool filterByCustomAttributes = false)
        {
            foreach (var propertyInfo in expected.GetType().GetProperties())
            {
                // we skip Symbol which isn't protobuffed
                if (filterByCustomAttributes && propertyInfo.CustomAttributes.Count() != 0)
                {
                    Assert.AreEqual(propertyInfo.GetValue(expected), propertyInfo.GetValue(result));
                }
            }
            foreach (var fieldInfo in expected.GetType().GetFields())
            {
                Assert.AreEqual(fieldInfo.GetValue(expected), fieldInfo.GetValue(result));
            }
        }

        private BaseData CreateNewInstance()
        {
            return new BrainStockRanking30Day
            {
                Symbol = Symbol.Empty,
                Time = DateTime.Today,
                Rank = 10m
            };
        }
    }
}