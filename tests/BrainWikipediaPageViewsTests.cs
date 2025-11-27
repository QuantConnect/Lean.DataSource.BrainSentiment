using System;
using NUnit.Framework;
using QuantConnect;
using QuantConnect.Data;
using QuantConnect.DataSource;

namespace QuantConnect.DataLibrary.Tests
{
    [TestFixture]
    public class BrainWikipediaPageViewsTests
    {
        private SubscriptionDataConfig _config;

        [OneTimeSetUp]
        public void Setup()
        {
            _config = new SubscriptionDataConfig(
                typeof(BrainWikipediaPageViews),
                Symbol.Create("AAPL", SecurityType.Equity, Market.USA),
                Resolution.Daily,
                TimeZones.Utc,
                TimeZones.Utc,
                true,
                false,
                false
            );
        }

        [Test]
        public void Reader_Parses_All_Fields_Correctly()
        {
            // FULL CSV reflecting production-format BWPV output
            string csv =
            "20250910, 14220, 4.2139, 58460, 0.5318, 218379, -0.6219";

            var data = new BrainWikipediaPageViews()
                .Reader(_config, csv, DateTime.UtcNow, false) as BrainWikipediaPageViews;

            Assert.NotNull(data, "Reader returned null");

            // === METADATA ===
            Assert.AreEqual(14220m, data.NumberViews1);
            Assert.AreEqual(4.2139m, data.Buzz1);
            Assert.AreEqual(58460m, data.NumberViews7);
            Assert.AreEqual(0.5318m, data.Buzz7);
            Assert.AreEqual(218379m, data.NumberViews30);
            Assert.AreEqual(-0.6219m, data.Buzz30);
        }
    }
}
