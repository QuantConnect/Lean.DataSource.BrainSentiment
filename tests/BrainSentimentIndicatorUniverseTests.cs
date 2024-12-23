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
using QuantConnect.Data.Auxiliary;
using QuantConnect.Interfaces;
using QuantConnect.Util;

namespace QuantConnect.DataLibrary.Tests
{
    [TestFixture]
    public class BrainSentimentIndicatorUniverseTests
    {
        [OneTimeSetUp]
        public void Setup()
        {
            Composer.Instance.GetExportedValueByTypeName<IMapFileProvider>(Configuration.Config.Get("map-file-provider", typeof(LocalDiskMapFileProvider).Name));
        }
        [Test]
        public void ReaderTest()
        {
            var factory = new BrainSentimentIndicatorUniverse();
            var line = "AAPL R735QTJ8XC9X,AAPL,869,516,0.1196,,,5101,3176,0.0976,-0.169,0.0888";

            var config = CreateSubscriptionDataConfig();
            var date = new DateTime(2022, 04, 21);
            var data = (BrainSentimentIndicatorUniverse)factory.Reader(config, line, date, false);
            Assert.AreEqual(date.ConvertFromUtc(config.ExchangeTimeZone) + TimeSpan.FromHours(12), data.EndTime);
            Assert.AreEqual(0.1196, data.Sentiment7Days);
            Assert.AreEqual(0.0976, data.Sentiment30Days);
            Assert.AreEqual(516, data.SentimentalArticleMentions7Days);
            Assert.AreEqual(3176, data.SentimentalArticleMentions30Days);
            Assert.AreEqual(null, data.SentimentalBuzzVolume7Days);
            Assert.AreEqual(0.0888, data.SentimentalBuzzVolume30Days);
            Assert.AreEqual(869, data.TotalArticleMentions7Days);
            Assert.AreEqual(5101, data.TotalArticleMentions30Days);
            Assert.AreEqual(null, data.TotalBuzzVolume7Days);
            Assert.AreEqual(-0.169, data.TotalBuzzVolume30Days);
            Assert.AreEqual(0.1196, data.Price);
            Assert.AreEqual("AAPL", data.Symbol.Value);
        }

        [Test]
        public void ReaderNullSentiment7DaysTest()
        {
            var factory = new BrainSentimentIndicatorUniverse();
            var line = "CNCE VO2R14MRA2XX,CNCE,,,,,,22,14,0.323100,-0.945800,-0.745700";

            var config = CreateSubscriptionDataConfig();
            var date = new DateTime(2022, 04, 21);
            var data = (BrainSentimentIndicatorUniverse)factory.Reader(config, line, date, false);
            Assert.AreEqual(date.ConvertFromUtc(config.ExchangeTimeZone) + TimeSpan.FromHours(12), data.EndTime);
            Assert.AreEqual("CNCE", data.Symbol.Value);

            // Value is 0 because 7-day Sentiment is null
            Assert.AreEqual(0, data.Value);
            Assert.IsNull(data.Sentiment7Days);
            Assert.IsNull(data.SentimentalArticleMentions7Days);
            Assert.IsNull(data.SentimentalBuzzVolume7Days);
            Assert.IsNull(data.TotalArticleMentions7Days);
            Assert.IsNull(data.TotalBuzzVolume7Days);

            Assert.AreEqual(22, data.TotalArticleMentions30Days);
            Assert.AreEqual(14, data.SentimentalArticleMentions30Days);
            Assert.AreEqual(0.323100, data.Sentiment30Days);
            Assert.AreEqual(-0.945800, data.TotalBuzzVolume30Days);
            Assert.AreEqual(-0.745700, data.SentimentalBuzzVolume30Days);
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
                    TotalArticleMentions7Days = null,
                    SentimentalArticleMentions7Days = null,
                    Sentiment7Days = null,
                    TotalBuzzVolume7Days = null,
                    SentimentalBuzzVolume7Days = null,
                    TotalArticleMentions30Days = 100,
                    SentimentalArticleMentions30Days = 50m,
                    Sentiment30Days = 0.25m,
                    TotalBuzzVolume30Days = 2000m,
                    SentimentalBuzzVolume30Days = 1000m,

                    Symbol = new Symbol(SecurityIdentifier.Parse("A RPTMYV3VC57P"), "A"),
                    Time = new DateTime(2022, 04, 21)
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
                    Time = new DateTime(2022, 04, 21)
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
                    Time = new DateTime(2022, 04, 21)
                }
            };
        }

        private SubscriptionDataConfig CreateSubscriptionDataConfig() => new(
            typeof(BrainSentimentIndicatorUniverse),
            Symbol.None,
            Resolution.Daily,
            TimeZones.NewYork,
            TimeZones.NewYork,
            true,
            true,
            false);
    }
}