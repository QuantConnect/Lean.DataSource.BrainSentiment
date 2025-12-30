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
            // 7 columns required
            var csv = "20250910,14220,4.2139,58460,0.5318,218379,-0.6219";

            var data = new BrainWikipediaPageViews()
                .Reader(_config, csv, DateTime.UtcNow, false) as BrainWikipediaPageViews;

            Assert.NotNull(data, "Reader returned null");

            // Time may be midnight or midday in some datasets; validate date only
            Assert.AreEqual(new DateTime(2025, 9, 10), data.Time.Date);

            Assert.AreEqual(14220m, data.NumberViews1);
            Assert.AreEqual(4.2139m, data.Buzz1);
            Assert.AreEqual(58460m, data.NumberViews7);
            Assert.AreEqual(0.5318m, data.Buzz7);
            Assert.AreEqual(218379m, data.NumberViews30);
            Assert.AreEqual(-0.6219m, data.Buzz30);
        }

        [Test]
        public void Reader_Allows_Empty_Numeric_Fields_As_Null()
        {
            // Keep 7 columns; last value empty -> Buzz30 should be null
            var csv = "20250910,14220,4.2139,58460,0.5318,218379,";

            var data = new BrainWikipediaPageViews()
                .Reader(_config, csv, DateTime.UtcNow, false) as BrainWikipediaPageViews;

            Assert.NotNull(data);

            Assert.AreEqual(new DateTime(2025, 9, 10), data.Time.Date);
            Assert.AreEqual(14220m, data.NumberViews1);
            Assert.IsNull(data.Buzz30);
        }

        [Test]
        public void Reader_ReturnsNull_On_Whitespace_Line()
        {
            var data = new BrainWikipediaPageViews()
                .Reader(_config, "   ", DateTime.UtcNow, false);

            Assert.IsNull(data);
        }

        [Test]
        public void Reader_ReturnsNull_When_TooFew_Columns()
        {
            // Less than 7 columns -> null
            var csv = "20250910,14220,4.2139,58460,0.5318,218379"; // only 6 columns

            var data = new BrainWikipediaPageViews()
                .Reader(_config, csv, DateTime.UtcNow, false);

            Assert.IsNull(data);
        }

        [Test]
        public void Reader_Throws_On_Invalid_RowDate()
        {
            var csv = "bad-date,14220,4.2139,58460,0.5318,218379,-0.6219";

            Assert.Throws<FormatException>(() =>
                new BrainWikipediaPageViews().Reader(_config, csv, DateTime.UtcNow, false));
        }

        [Test]
        public void Reader_Throws_On_Invalid_Numeric_Field()
        {
            var csv = "20250910,14220,abc,58460,0.5318,218379,-0.6219";

            Assert.Throws<FormatException>(() =>
                new BrainWikipediaPageViews().Reader(_config, csv, DateTime.UtcNow, false));
        }

        [Test]
        public void Reader_Allows_Empty_Middle_Field_As_Null()
        {
            // Buzz7 empty (still 7 columns) => null
            var csv = "20250910,14220,4.2139,58460,,218379,-0.6219";

            var data = new BrainWikipediaPageViews()
                .Reader(_config, csv, DateTime.UtcNow, false) as BrainWikipediaPageViews;

            Assert.NotNull(data);

            Assert.AreEqual(14220m, data.NumberViews1);
            Assert.AreEqual(4.2139m, data.Buzz1);
            Assert.AreEqual(58460m, data.NumberViews7);
            Assert.IsNull(data.Buzz7);
            Assert.AreEqual(218379m, data.NumberViews30);
            Assert.AreEqual(-0.6219m, data.Buzz30);
        }
    }
}
