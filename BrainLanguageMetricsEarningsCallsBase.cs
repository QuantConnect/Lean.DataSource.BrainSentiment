using System;
using System.IO;
using NodaTime;
using QuantConnect;
using QuantConnect.Data;
using System.Globalization;
using System.Linq;

namespace QuantConnect.DataSource
{
    /// <summary>
    /// Base class for Brain Language Metrics on Earnings Calls (metrics-only version).
    /// This class parses daily rows of MD/AQ/MA metrics for a given symbol.
    /// </summary>
    
    
    public abstract class BrainLanguageMetricsEarningsCallsBase<T> : BaseData
        where T : BrainLanguageMetricsEarningsCallsBase<T>, new()
    {
        private static DateTime? ParseNullableDate(string v)
        {
            if (string.IsNullOrWhiteSpace(v))
                return null;
            v = v.Trim();
            if (DateTime.TryParseExact(v, "yyyyMMdd", CultureInfo.InvariantCulture,
                DateTimeStyles.None, out var d1))
                return d1;
            if (DateTime.TryParseExact(v, "yyyy-MM-dd", CultureInfo.InvariantCulture,
                DateTimeStyles.None, out var d2))
                return d2;
            if (DateTime.TryParse(v, CultureInfo.InvariantCulture, DateTimeStyles.None, out var d3))
                return d3;
            return null;
        }
        
        private static int? ParseNullableIntSafe(string v)
        {
            if (string.IsNullOrWhiteSpace(v))
                return null;

            return int.TryParse(v, NumberStyles.Any, CultureInfo.InvariantCulture, out var x)
                ? x
                : null;
        }

        private static decimal? ParseNullableDecimalSafe(string v)
        {
            if (string.IsNullOrWhiteSpace(v))
                return null;

            return decimal.TryParse(v, NumberStyles.Any, CultureInfo.InvariantCulture, out var x)
                ? x
                : null;
        }
        
        // === Transcript metadata ===
        public DateTime? LastTranscriptDate { get; set; }
        public int? LastTranscriptQuarter { get; set; }
        public int? LastTranscriptYear { get; set; }

        // === MD Metrics ===
        public decimal? MdNCharacters { get; set; }
        public decimal? MdSentiment { get; set; }
        public decimal? MdScoreUncertainty { get; set; }
        public decimal? MdScoreLitigious { get; set; }
        public decimal? MdScoreConstraining { get; set; }
        public decimal? MdReadability { get; set; }
        public decimal? MdLexicalRichness { get; set; }
        public decimal? MdLexicalDensity { get; set; }
        public decimal? MdSpecificDensity { get; set; }

        // === AQ Metrics ===
        public decimal? AqNCharacters { get; set; }
        public decimal? AqSentiment { get; set; }
        public decimal? AqScoreUncertainty { get; set; }
        public decimal? AqScoreLitigious { get; set; }
        public decimal? AqScoreConstraining { get; set; }

        // === MA Metrics ===
        public decimal? MaNCharacters { get; set; }
        public decimal? MaSentiment { get; set; }
        public decimal? MaScoreUncertainty { get; set; }
        public decimal? MaScoreLitigious { get; set; }
        public decimal? MaScoreConstraining { get; set; }
        public decimal? MaReadability { get; set; }
        public decimal? MaLexicalRichness { get; set; }
        public decimal? MaLexicalDensity { get; set; }
        public decimal? MaSpecificDensity { get; set; }

        // === Previous transcript metadata ===
        public DateTime? PrevTranscriptDate { get; set; }
        public int? PrevTranscriptQuarter { get; set; }
        public int? PrevTranscriptYear { get; set; }

        // === MD deltas ===
        public decimal? MdDeltaPercNCharacters { get; set; }
        public decimal? MdDeltaSentiment { get; set; }
        public decimal? MdDeltaScoreUncertainty { get; set; }
        public decimal? MdDeltaScoreLitigious { get; set; }
        public decimal? MdDeltaScoreConstraining { get; set; }
        public decimal? MdDeltaReadability { get; set; }
        public decimal? MdDeltaLexicalRichness { get; set; }
        public decimal? MdDeltaLexicalDensity { get; set; }
        public decimal? MdDeltaSpecificDensity { get; set; }

        // === MD similarities ===
        public decimal? MdSimilarityAll { get; set; }
        public decimal? MdSimilarityPositive { get; set; }
        public decimal? MdSimilarityNegative { get; set; }
        public decimal? MdSimilarityUncertainty { get; set; }
        public decimal? MdSimilarityLitigious { get; set; }
        public decimal? MdSimilarityConstraining { get; set; }

        // === AQ deltas & similarities ===
        public decimal? AqDeltaPercNCharacters { get; set; }
        public decimal? AqDeltaSentimentDelta { get; set; }
        public decimal? AqDeltaScoreUncertainty { get; set; }
        public decimal? AqDeltaScoreLitigious { get; set; }
        public decimal? AqDeltaScoreConstraining { get; set; }
        public decimal? AqSimilarityAll { get; set; }
        public decimal? AqSimilarityPositive { get; set; }
        public decimal? AqSimilarityNegative { get; set; }

        // === MA deltas & similarities ===
        public decimal? MaDeltaPercNCharacters { get; set; }
        public decimal? MaDeltaSentimentDelta { get; set; }
        public decimal? MaDeltaScoreUncertainty { get; set; }
        public decimal? MaDeltaScoreLitigious { get; set; }
        public decimal? MaDeltaScoreConstraining { get; set; }
        public decimal? MaDeltaReadability { get; set; }
        public decimal? MaDeltaLexicalRichness { get; set; }
        public decimal? MaDeltaLexicalDensity { get; set; }
        public decimal? MaDeltaSpecificDensity { get; set; }
        public decimal? MaSimilarityAll { get; set; }
        public decimal? MaSimilarityPositive { get; set; }
        public decimal? MaSimilarityNegative { get; set; }
        public decimal? MaSimilarityUncertainty { get; set; }
        public decimal? MaSimilarityLitigious { get; set; }
        public decimal? MaSimilarityConstraining { get; set; }


        /// <summary>
        /// Path structure:
        /// alternative/brain/blmect/{symbol}.csv
        /// </summary>
        public override SubscriptionDataSource GetSource(SubscriptionDataConfig config, DateTime date, bool isLiveMode)
        {
            var path = Path.Combine(
                Globals.DataFolder,
                "alternative",
                "brain",
                "blmect",
                $"{config.Symbol.Value.ToLowerInvariant()}.csv"
            );

            return new SubscriptionDataSource(
                path,
                SubscriptionTransportMedium.LocalFile);
        }

        /// <summary>
        /// Parse one metrics row.
        /// CSV Layout:
        /// 0: date (yyyyMMdd)
        /// 1: last transcript date (yyyy-MM-dd)
        /// 2: last transcript quarter
        /// 3: last transcript year
        /// 4..12: MD metrics (9 columns)
        /// 13..17: AQ metrics (5 columns)
        /// 18..26: MA metrics (9 columns)
        /// 27: prev transcript date (yyyy-MM-dd)
        /// 28: prev transcript quarter
        /// 29: prev transcript year
        /// 30..38: MD deltas (9 columns)
        /// 39..44: MD similarities (6 columns)
        /// 45..49: AQ deltas (5 columns)
        /// 50..52: AQ similarities (3 columns)
        /// 53..62: MA deltas (10 columns)
        /// 63..68: MA similarities (6 columns)
        /// </summary>
        public override BaseData Reader(SubscriptionDataConfig config, string line, DateTime date, bool isLiveMode)
        {
            if (string.IsNullOrWhiteSpace(line))
                return null;

            var csv = line.Split(',');
            if (csv.Length < 4)
                return null;

            //var dataDate = Parse.DateTimeExact(csv[0], "yyyyMMdd");
            if (!DateTime.TryParseExact(csv[0], "yyyyMMdd", CultureInfo.InvariantCulture,
                DateTimeStyles.None, out var dataDate))
                return null;

            var data = new T
            {
                Symbol = config.Symbol,
                Time = dataDate,
                EndTime = dataDate.AddHours(12)
            };

            int i = 1;

            // === Transcript metadata ===
            data.LastTranscriptDate    = ParseNullableDate(csv.ElementAtOrDefault(i++));
            data.LastTranscriptQuarter = ParseNullableIntSafe(csv.ElementAtOrDefault(i++));
            data.LastTranscriptYear    = ParseNullableIntSafe(csv.ElementAtOrDefault(i++));

            // === MD ===
            data.MdNCharacters       = ParseNullableDecimalSafe(csv.ElementAtOrDefault(i++));
            data.MdSentiment         = ParseNullableDecimalSafe(csv.ElementAtOrDefault(i++));
            data.MdScoreUncertainty  = ParseNullableDecimalSafe(csv.ElementAtOrDefault(i++));
            data.MdScoreLitigious    = ParseNullableDecimalSafe(csv.ElementAtOrDefault(i++));
            data.MdScoreConstraining = ParseNullableDecimalSafe(csv.ElementAtOrDefault(i++));
            data.MdReadability       = ParseNullableDecimalSafe(csv.ElementAtOrDefault(i++));
            data.MdLexicalRichness   = ParseNullableDecimalSafe(csv.ElementAtOrDefault(i++));
            data.MdLexicalDensity    = ParseNullableDecimalSafe(csv.ElementAtOrDefault(i++));
            data.MdSpecificDensity   = ParseNullableDecimalSafe(csv.ElementAtOrDefault(i++));

            // === AQ ===
            data.AqNCharacters       = ParseNullableDecimalSafe(csv.ElementAtOrDefault(i++));
            data.AqSentiment         = ParseNullableDecimalSafe(csv.ElementAtOrDefault(i++));
            data.AqScoreUncertainty  = ParseNullableDecimalSafe(csv.ElementAtOrDefault(i++));
            data.AqScoreLitigious    = ParseNullableDecimalSafe(csv.ElementAtOrDefault(i++));
            data.AqScoreConstraining = ParseNullableDecimalSafe(csv.ElementAtOrDefault(i++));

            // === MA ===
            data.MaNCharacters       = ParseNullableDecimalSafe(csv.ElementAtOrDefault(i++));
            data.MaSentiment         = ParseNullableDecimalSafe(csv.ElementAtOrDefault(i++));
            data.MaScoreUncertainty  = ParseNullableDecimalSafe(csv.ElementAtOrDefault(i++));
            data.MaScoreLitigious    = ParseNullableDecimalSafe(csv.ElementAtOrDefault(i++));
            data.MaScoreConstraining = ParseNullableDecimalSafe(csv.ElementAtOrDefault(i++));
            data.MaReadability       = ParseNullableDecimalSafe(csv.ElementAtOrDefault(i++));
            data.MaLexicalRichness   = ParseNullableDecimalSafe(csv.ElementAtOrDefault(i++));
            data.MaLexicalDensity    = ParseNullableDecimalSafe(csv.ElementAtOrDefault(i++));
            data.MaSpecificDensity   = ParseNullableDecimalSafe(csv.ElementAtOrDefault(i++));

            if (csv.Length <= i)
                return data;

            // === Prev transcript ===
            data.PrevTranscriptDate    = ParseNullableDate(csv.ElementAtOrDefault(i++));
            data.PrevTranscriptQuarter = ParseNullableIntSafe(csv.ElementAtOrDefault(i++));
            data.PrevTranscriptYear    = ParseNullableIntSafe(csv.ElementAtOrDefault(i++));

            // === MD deltas & similarities ===
            data.MdDeltaPercNCharacters   = ParseNullableDecimalSafe(csv.ElementAtOrDefault(i++));
            data.MdDeltaSentiment         = ParseNullableDecimalSafe(csv.ElementAtOrDefault(i++));
            data.MdDeltaScoreUncertainty  = ParseNullableDecimalSafe(csv.ElementAtOrDefault(i++));
            data.MdDeltaScoreLitigious    = ParseNullableDecimalSafe(csv.ElementAtOrDefault(i++));
            data.MdDeltaScoreConstraining = ParseNullableDecimalSafe(csv.ElementAtOrDefault(i++));
            data.MdDeltaReadability       = ParseNullableDecimalSafe(csv.ElementAtOrDefault(i++));
            data.MdDeltaLexicalRichness   = ParseNullableDecimalSafe(csv.ElementAtOrDefault(i++));
            data.MdDeltaLexicalDensity    = ParseNullableDecimalSafe(csv.ElementAtOrDefault(i++));
            data.MdDeltaSpecificDensity   = ParseNullableDecimalSafe(csv.ElementAtOrDefault(i++));

            data.MdSimilarityAll          = ParseNullableDecimalSafe(csv.ElementAtOrDefault(i++));
            data.MdSimilarityPositive     = ParseNullableDecimalSafe(csv.ElementAtOrDefault(i++));
            data.MdSimilarityNegative     = ParseNullableDecimalSafe(csv.ElementAtOrDefault(i++));
            data.MdSimilarityUncertainty  = ParseNullableDecimalSafe(csv.ElementAtOrDefault(i++));
            data.MdSimilarityLitigious    = ParseNullableDecimalSafe(csv.ElementAtOrDefault(i++));
            data.MdSimilarityConstraining = ParseNullableDecimalSafe(csv.ElementAtOrDefault(i++));

            // === AQ deltas & similarities ===
            data.AqDeltaPercNCharacters   = ParseNullableDecimalSafe(csv.ElementAtOrDefault(i++));
            data.AqDeltaSentimentDelta    = ParseNullableDecimalSafe(csv.ElementAtOrDefault(i++));
            data.AqDeltaScoreUncertainty  = ParseNullableDecimalSafe(csv.ElementAtOrDefault(i++));
            data.AqDeltaScoreLitigious    = ParseNullableDecimalSafe(csv.ElementAtOrDefault(i++));
            data.AqDeltaScoreConstraining = ParseNullableDecimalSafe(csv.ElementAtOrDefault(i++));
            data.AqSimilarityAll          = ParseNullableDecimalSafe(csv.ElementAtOrDefault(i++));
            data.AqSimilarityPositive     = ParseNullableDecimalSafe(csv.ElementAtOrDefault(i++));
            data.AqSimilarityNegative     = ParseNullableDecimalSafe(csv.ElementAtOrDefault(i++));

            // === MA deltas & similarities ===
            data.MaDeltaPercNCharacters   = ParseNullableDecimalSafe(csv.ElementAtOrDefault(i++));
            data.MaDeltaSentimentDelta    = ParseNullableDecimalSafe(csv.ElementAtOrDefault(i++));
            data.MaDeltaScoreUncertainty  = ParseNullableDecimalSafe(csv.ElementAtOrDefault(i++));
            data.MaDeltaScoreLitigious    = ParseNullableDecimalSafe(csv.ElementAtOrDefault(i++));
            data.MaDeltaScoreConstraining = ParseNullableDecimalSafe(csv.ElementAtOrDefault(i++));
            data.MaDeltaReadability       = ParseNullableDecimalSafe(csv.ElementAtOrDefault(i++));
            data.MaDeltaLexicalRichness   = ParseNullableDecimalSafe(csv.ElementAtOrDefault(i++));
            data.MaDeltaLexicalDensity    = ParseNullableDecimalSafe(csv.ElementAtOrDefault(i++));
            data.MaDeltaSpecificDensity   = ParseNullableDecimalSafe(csv.ElementAtOrDefault(i++));
            
            data.MaSimilarityAll          = ParseNullableDecimalSafe(csv.ElementAtOrDefault(i++));
            data.MaSimilarityPositive     = ParseNullableDecimalSafe(csv.ElementAtOrDefault(i++));
            data.MaSimilarityNegative     = ParseNullableDecimalSafe(csv.ElementAtOrDefault(i++));
            data.MaSimilarityUncertainty  = ParseNullableDecimalSafe(csv.ElementAtOrDefault(i++));
            data.MaSimilarityLitigious    = ParseNullableDecimalSafe(csv.ElementAtOrDefault(i++));
            data.MaSimilarityConstraining = ParseNullableDecimalSafe(csv.ElementAtOrDefault(i++));

            return data;
        }

        private static int? ParseNullableInt(string v) =>
            string.IsNullOrWhiteSpace(v) ? null : (int)Parse.Decimal(v);

        private static decimal? ParseNullableDecimal(string v) =>
            string.IsNullOrWhiteSpace(v) ? null : Parse.Decimal(v);

        public override bool RequiresMapping() => true;
        public override bool IsSparseData() => true;
        public override Resolution DefaultResolution() => Resolution.Daily;
        public override System.Collections.Generic.List<Resolution> SupportedResolutions() => DailyResolution;
        public override DateTimeZone DataTimeZone() => DateTimeZone.Utc;
    }
}
