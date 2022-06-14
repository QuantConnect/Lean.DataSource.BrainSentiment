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
using System.Linq;
using Newtonsoft.Json;
using NUnit.Framework;
using QuantConnect.Data;
using QuantConnect.DataSource;
using System.Collections.Generic;

namespace QuantConnect.DataLibrary.Tests
{
    [TestFixture]
    public class BrainStockRankingUniverseTests
    {
        [TestCase(true)]
        [TestCase(false)]
        public void ReaderTest(bool liveMode)
        {
            var factory = new BrainStockRankingUniverse();
            var line = "AAPL R735QTJ8XC9X,AAPL,1,2,,,20";

            var now = new DateTime(2022, 04, 21);
            var data = (BrainStockRankingUniverse)factory.Reader(null, line, now, liveMode);
            Assert.AreEqual(now, data.EndTime);
            Assert.AreEqual(1, data.Rank2Days);
            Assert.AreEqual(2, data.Rank3Days);
            Assert.AreEqual(null, data.Rank5Days);
            Assert.AreEqual(null, data.Rank10Days);
            Assert.AreEqual(20, data.Rank21Days);
            Assert.AreEqual(1, data.Price);
            Assert.AreEqual("AAPL", data.Symbol.Value);
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
        public void Selection()
        {
            var datum = CreateNewSelection();

            var expected = from d in datum
                            where d.Rank2Days < 0m && d.Rank10Days < 0m
                            select d.Symbol;
            var result = new List<Symbol> {Symbol.Create("HWM", SecurityType.Equity, Market.USA)};

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
            return new BrainStockRankingUniverse
                {
                    Rank2Days = null,
                    Rank3Days = 5m,
                    Rank5Days = 100m,
                    Rank10Days = 10000m,
                    Rank21Days = 10000m,

                    Symbol = new Symbol(SecurityIdentifier.Parse("A RPTMYV3VC57P"), "A"),
                    Time = new DateTime(2022, 04, 21)
                };
        }

        private IEnumerable<BrainStockRankingUniverse> CreateNewSelection()
        {
            return new []
            {
                new BrainStockRankingUniverse
                {
                    Rank2Days = 5m,
                    Rank3Days = 5m,
                    Rank5Days = 100m,
                    Rank10Days = 10000m,
                    Rank21Days = 10000m,

                    Symbol = new Symbol(SecurityIdentifier.Parse("A RPTMYV3VC57P"), "A"),
                    Time = new DateTime(2022, 04, 21)
                },
                new BrainStockRankingUniverse
                {
                    Rank2Days = -5m,
                    Rank3Days = -5m,
                    Rank5Days = -100m,
                    Rank10Days = -10000m,
                    Rank21Days = -10000m,

                    Symbol = new Symbol(SecurityIdentifier.Parse("AA R735QTJ8XC9X"), "HWM"),
                    Time = new DateTime(2022, 04, 21)
                }
            };
        }
    }
}