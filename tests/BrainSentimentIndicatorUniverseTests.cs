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
using System.IO;
using System.Linq;
using ProtoBuf.Meta;
using Newtonsoft.Json;
using NodaTime;
using NUnit.Framework;
using QuantConnect.Data;
using QuantConnect.DataSource;
using QuantConnect.Data.Market;

namespace QuantConnect.DataLibrary.Tests
{
    [TestFixture]
    public class BrainSentimentIndicatorUniverseTests
    {
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
                            where d.Sentiment7Days < 0.1m && d.TotalBuzzVolume7Days < 0.1m
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
            return new BrainSentimentIndicatorUniverse
                {
                    TotalArticleMentions7Days = 10,
                    SentimentalArticleMentions7Days = 5m,
                    Sentiment7Days = 0.5m,
                    TotalBuzzVolume7Days = 200m,
                    SentimentalBuzzVolume7Days = 100m,
                    TotalArticleMentions30Days = 100,
                    SentimentalArticleMentions30Days = 50m,
                    Sentiment30Days = 0.25m,
                    TotalBuzzVolume30Days = 2000m,
                    SentimentalBuzzVolume30Days = 1000m,

                    Symbol = new Symbol(SecurityIdentifier.Parse("A RPTMYV3VC57P"), "A"),
                    Time = DateTime.Today
                };
        }

        private IEnumerable<BrainSentimentIndicatorUniverse> CreateNewSelection()
        {
            return new []
            {
                new BrainSentimentIndicatorUniverse
                {
                    TotalArticleMentions7Days = 10,
                    SentimentalArticleMentions7Days = 5m,
                    Sentiment7Days = 0.5m,
                    TotalBuzzVolume7Days = 200m,
                    SentimentalBuzzVolume7Days = 100m,
                    TotalArticleMentions30Days = 100,
                    SentimentalArticleMentions30Days = 50m,
                    Sentiment30Days = 0.25m,
                    TotalBuzzVolume30Days = 2000m,
                    SentimentalBuzzVolume30Days = 1000m,

                    Symbol = new Symbol(SecurityIdentifier.Parse("A RPTMYV3VC57P"), "A"),
                    Time = DateTime.Today
                },
                new BrainSentimentIndicatorUniverse
                {
                    TotalArticleMentions7Days = 0,
                    SentimentalArticleMentions7Days = 0m,
                    Sentiment7Days = -0.2m,
                    TotalBuzzVolume7Days = 0m,
                    SentimentalBuzzVolume7Days = 0m,
                    TotalArticleMentions30Days = 100,
                    SentimentalArticleMentions30Days = 50m,
                    Sentiment30Days = 0.25m,
                    TotalBuzzVolume30Days = 2000m,
                    SentimentalBuzzVolume30Days = 1000m,

                    Symbol = new Symbol(SecurityIdentifier.Parse("AA R735QTJ8XC9X"), "HWM"),
                    Time = DateTime.Today
                }
            };
        }
    }
}