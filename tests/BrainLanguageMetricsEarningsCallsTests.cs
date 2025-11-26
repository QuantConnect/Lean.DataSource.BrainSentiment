using System;
using NUnit.Framework;
using QuantConnect;
using QuantConnect.Data;
using QuantConnect.DataSource;

namespace QuantConnect.DataLibrary.Tests
{
    [TestFixture]
    public class BrainLanguageMetricsEarningsCallsTests
    {
        private SubscriptionDataConfig _config;

        [OneTimeSetUp]
        public void Setup()
        {
            _config = new SubscriptionDataConfig(
                typeof(BrainLanguageMetricsEarningsCalls),
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
            // FULL CSV reflecting production-format BLMECT output
            string csv =
                "20250910,20250731,3,2025," +
                "20851,0.9062,0.1141,0.0537,0.0201,11.178,0.3696,0.5447,0.0717," +
                "9032,0.6471,0.4058,0.029,0.0145," +
                "14849,0.9636,0.2152,0.1013,0.0506,8.481,0.4344,0.4326,0.0619," +
                "20250501,2,2025," +
                "0.0831,0.0124,-0.0298,0.0234,-0.0026,-0.0935,-0.0597,0.0122,0.0026," +
                "0.659,0.7342,0.7638,0.8332,0.9562,0.2," +
                "-0.0662,0.6471,-0.0386,-0.0266,0.0145," +
                "0.7541,0.3921,0.7232," +
                "0.0267,0.3549,-0.0495,0.0621,0.0212,-0.4425,-0.0319,-0.0182,-0.0206," +
                "0.6139,0.4824,0.9113,0.482,0.75,0";

            var data = new BrainLanguageMetricsEarningsCalls()
                .Reader(_config, csv, DateTime.UtcNow, false) as BrainLanguageMetricsEarningsCalls;

            Assert.NotNull(data, "Reader returned null");

            // === CORE METADATA ===
            Assert.AreEqual(new DateTime(2025, 7, 31), data.LastTranscriptDate);
            Assert.AreEqual(3, data.LastTranscriptQuarter);
            Assert.AreEqual(2025, data.LastTranscriptYear);

            // === MD METRICS ===
            Assert.AreEqual(20851m, data.MdNCharacters);
            Assert.AreEqual(0.9062m, data.MdSentiment);
            Assert.AreEqual(0.1141m, data.MdScoreUncertainty);
            Assert.AreEqual(0.0537m, data.MdScoreLitigious);
            Assert.AreEqual(0.0201m, data.MdScoreConstraining);
            Assert.AreEqual(11.178m, data.MdReadability);
            Assert.AreEqual(0.3696m, data.MdLexicalRichness);
            Assert.AreEqual(0.5447m, data.MdLexicalDensity);
            Assert.AreEqual(0.0717m, data.MdSpecificDensity);

            // === AQ METRICS ===
            Assert.AreEqual(9032m, data.AqNCharacters);
            Assert.AreEqual(0.6471m, data.AqSentiment);
            Assert.AreEqual(0.4058m, data.AqScoreUncertainty);
            Assert.AreEqual(0.029m,  data.AqScoreLitigious);
            Assert.AreEqual(0.0145m, data.AqScoreConstraining);

            // === MA METRICS ===
            Assert.AreEqual(14849m, data.MaNCharacters);
            Assert.AreEqual(0.9636m, data.MaSentiment);
            Assert.AreEqual(0.2152m, data.MaScoreUncertainty);
            Assert.AreEqual(0.1013m, data.MaScoreLitigious);
            Assert.AreEqual(0.0506m, data.MaScoreConstraining);
            Assert.AreEqual(8.481m,  data.MaReadability);
            Assert.AreEqual(0.4344m, data.MaLexicalRichness);
            Assert.AreEqual(0.4326m, data.MaLexicalDensity);
            Assert.AreEqual(0.0619m, data.MaSpecificDensity);

            // === PREVIOUS TRANSCRIPT METADATA ===
            Assert.AreEqual(new DateTime(2025, 5, 1), data.PrevTranscriptDate);
            Assert.AreEqual(2, data.PrevTranscriptQuarter);
            Assert.AreEqual(2025, data.PrevTranscriptYear);

            // === MD DELTAS (9 fields) ===
            Assert.AreEqual(0.0831m,  data.MdDeltaPercNCharacters);
            Assert.AreEqual(0.0124m,  data.MdDeltaSentiment);
            Assert.AreEqual(-0.0298m, data.MdDeltaScoreUncertainty);
            Assert.AreEqual(0.0234m,  data.MdDeltaScoreLitigious);
            Assert.AreEqual(-0.0026m, data.MdDeltaScoreConstraining);
            Assert.AreEqual(-0.0935m, data.MdDeltaReadability);
            Assert.AreEqual(-0.0597m, data.MdDeltaLexicalRichness);
            Assert.AreEqual(0.0122m,  data.MdDeltaLexicalDensity);
            Assert.AreEqual(0.0026m,  data.MdDeltaSpecificDensity);

            // === MD SIMILARITIES (6 fields) ===
            Assert.AreEqual(0.659m,  data.MdSimilarityAll);
            Assert.AreEqual(0.7342m, data.MdSimilarityPositive);
            Assert.AreEqual(0.7638m, data.MdSimilarityNegative);
            Assert.AreEqual(0.8332m, data.MdSimilarityUncertainty);
            Assert.AreEqual(0.9562m, data.MdSimilarityLitigious);
            Assert.AreEqual(0.2m,    data.MdSimilarityConstraining);

            // === AQ DELTAS (5 fields) ===
            Assert.AreEqual(-0.0662m, data.AqDeltaPercNCharacters);
            Assert.AreEqual(0.6471m,  data.AqDeltaSentimentDelta);
            Assert.AreEqual(-0.0386m, data.AqDeltaScoreUncertainty);
            Assert.AreEqual(-0.0266m, data.AqDeltaScoreLitigious);
            Assert.AreEqual(0.0145m,  data.AqDeltaScoreConstraining);

            // === AQ SIMILARITIES (3 fields) ===
            Assert.AreEqual(0.7541m, data.AqSimilarityAll);
            Assert.AreEqual(0.3921m, data.AqSimilarityPositive);
            Assert.AreEqual(0.7232m, data.AqSimilarityNegative);

            // === MA DELTAS (10 fields) ===
            Assert.AreEqual(0.0267m,  data.MaDeltaPercNCharacters);
            Assert.AreEqual(0.3549m,  data.MaDeltaSentimentDelta);
            Assert.AreEqual(-0.0495m, data.MaDeltaScoreUncertainty);
            Assert.AreEqual(0.0621m,  data.MaDeltaScoreLitigious);
            Assert.AreEqual(0.0212m,  data.MaDeltaScoreConstraining);
            Assert.AreEqual(-0.4425m, data.MaDeltaReadability);
            Assert.AreEqual(-0.0319m, data.MaDeltaLexicalRichness);
            Assert.AreEqual(-0.0182m, data.MaDeltaLexicalDensity);
            Assert.AreEqual(-0.0206m, data.MaDeltaSpecificDensity);

            // === MA SIMILARITIES (6 fields) ===
            Assert.AreEqual(0.6139m, data.MaSimilarityAll);
            Assert.AreEqual(0.4824m, data.MaSimilarityPositive);
            Assert.AreEqual(0.9113m, data.MaSimilarityNegative);
            Assert.AreEqual(0.482m,  data.MaSimilarityUncertainty);
            Assert.AreEqual(0.75m,   data.MaSimilarityLitigious);
            Assert.AreEqual(0m,      data.MaSimilarityConstraining);
        }
    }
}
