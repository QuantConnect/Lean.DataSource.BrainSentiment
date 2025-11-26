using System;
using System.IO;
using NodaTime;
using QuantConnect;
using QuantConnect.Data;

namespace QuantConnect.DataSource
{
    /// <summary>
    /// Base class for Brain Language Metrics on Earnings Calls (metrics-only version).
    /// This class parses daily rows of MD/AQ/MA metrics for a given symbol.
    /// </summary>
    
    
    public abstract class BrainLanguageMetricsEarningsCallsBase<T> : BaseData
        where T : BrainLanguageMetricsEarningsCallsBase<T>, new()
    {
        // === Transcript metadata ===
        public DateTime LastTranscriptDate { get; set; }
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

            var dataDate = Parse.DateTimeExact(csv[0], "yyyyMMdd");

            var data = new T
            {
                Symbol = config.Symbol,
                Time = dataDate,
                EndTime = dataDate.AddHours(12)
            };

            data.LastTranscriptDate = Parse.DateTimeExact(csv[1], "yyyyMMdd");
            data.LastTranscriptQuarter = ParseNullableInt(csv[2]);
            data.LastTranscriptYear = ParseNullableInt(csv[3]);

            int i = 4;

            // === MD ===
            data.MdNCharacters = ParseNullableDecimal(csv[i++]);
            data.MdSentiment = ParseNullableDecimal(csv[i++]);
            data.MdScoreUncertainty = ParseNullableDecimal(csv[i++]);
            data.MdScoreLitigious = ParseNullableDecimal(csv[i++]);
            data.MdScoreConstraining = ParseNullableDecimal(csv[i++]);
            data.MdReadability = ParseNullableDecimal(csv[i++]);
            data.MdLexicalRichness = ParseNullableDecimal(csv[i++]);
            data.MdLexicalDensity = ParseNullableDecimal(csv[i++]);
            data.MdSpecificDensity = ParseNullableDecimal(csv[i++]);

            // === AQ ===
            data.AqNCharacters = ParseNullableDecimal(csv[i++]);
            data.AqSentiment = ParseNullableDecimal(csv[i++]);
            data.AqScoreUncertainty = ParseNullableDecimal(csv[i++]);
            data.AqScoreLitigious = ParseNullableDecimal(csv[i++]);
            data.AqScoreConstraining = ParseNullableDecimal(csv[i++]);

            // === MA ===
            data.MaNCharacters = ParseNullableDecimal(csv[i++]);
            data.MaSentiment = ParseNullableDecimal(csv[i++]);  
            data.MaScoreUncertainty = ParseNullableDecimal(csv[i++]);
            data.MaScoreLitigious = ParseNullableDecimal(csv[i++]);
            data.MaScoreConstraining = ParseNullableDecimal(csv[i++]);
            data.MaReadability = ParseNullableDecimal(csv[i++]);
            data.MaLexicalRichness = ParseNullableDecimal(csv[i++]);
            data.MaLexicalDensity = ParseNullableDecimal(csv[i++]);
            data.MaSpecificDensity = ParseNullableDecimal(csv[i++]);

            if (csv.Length <= i)
                return data;

            // === Prev transcript ===
            data.PrevTranscriptDate    = Parse.DateTimeExact(csv[i++], "yyyyMMdd");
            data.PrevTranscriptQuarter = ParseNullableInt(csv[i++]);
            data.PrevTranscriptYear    = ParseNullableInt(csv[i++]);

            // === MD deltas & similarities ===
            data.MdDeltaPercNCharacters   = ParseNullableDecimal(csv[i++]);
            data.MdDeltaSentiment         = ParseNullableDecimal(csv[i++]);
            data.MdDeltaScoreUncertainty  = ParseNullableDecimal(csv[i++]);
            data.MdDeltaScoreLitigious    = ParseNullableDecimal(csv[i++]);
            data.MdDeltaScoreConstraining = ParseNullableDecimal(csv[i++]);
            data.MdDeltaReadability       = ParseNullableDecimal(csv[i++]);
            data.MdDeltaLexicalRichness   = ParseNullableDecimal(csv[i++]);
            data.MdDeltaLexicalDensity    = ParseNullableDecimal(csv[i++]);
            data.MdDeltaSpecificDensity   = ParseNullableDecimal(csv[i++]);

            data.MdSimilarityAll          = ParseNullableDecimal(csv[i++]);
            data.MdSimilarityPositive     = ParseNullableDecimal(csv[i++]);
            data.MdSimilarityNegative     = ParseNullableDecimal(csv[i++]);
            data.MdSimilarityUncertainty  = ParseNullableDecimal(csv[i++]);
            data.MdSimilarityLitigious    = ParseNullableDecimal(csv[i++]);
            data.MdSimilarityConstraining = ParseNullableDecimal(csv[i++]);

            // === AQ deltas & similarities ===
            data.AqDeltaPercNCharacters   = ParseNullableDecimal(csv[i++]);
            data.AqDeltaSentimentDelta    = ParseNullableDecimal(csv[i++]);
            data.AqDeltaScoreUncertainty  = ParseNullableDecimal(csv[i++]);
            data.AqDeltaScoreLitigious    = ParseNullableDecimal(csv[i++]);
            data.AqDeltaScoreConstraining = ParseNullableDecimal(csv[i++]);
            data.AqSimilarityAll          = ParseNullableDecimal(csv[i++]);
            data.AqSimilarityPositive     = ParseNullableDecimal(csv[i++]);
            data.AqSimilarityNegative     = ParseNullableDecimal(csv[i++]);

            // === MA deltas & similarities ===
            data.MaDeltaPercNCharacters   = ParseNullableDecimal(csv[i++]);
            data.MaDeltaSentimentDelta    = ParseNullableDecimal(csv[i++]);
            data.MaDeltaScoreUncertainty  = ParseNullableDecimal(csv[i++]);
            data.MaDeltaScoreLitigious    = ParseNullableDecimal(csv[i++]);
            data.MaDeltaScoreConstraining = ParseNullableDecimal(csv[i++]);
            data.MaDeltaReadability       = ParseNullableDecimal(csv[i++]);
            data.MaDeltaLexicalRichness   = ParseNullableDecimal(csv[i++]);
            data.MaDeltaLexicalDensity    = ParseNullableDecimal(csv[i++]);
            data.MaDeltaSpecificDensity   = ParseNullableDecimal(csv[i++]);
            data.MaSimilarityAll          = ParseNullableDecimal(csv[i++]);
            data.MaSimilarityPositive     = ParseNullableDecimal(csv[i++]);
            data.MaSimilarityNegative     = ParseNullableDecimal(csv[i++]);
            data.MaSimilarityUncertainty  = ParseNullableDecimal(csv[i++]);
            data.MaSimilarityLitigious    = ParseNullableDecimal(csv[i++]);
            data.MaSimilarityConstraining = ParseNullableDecimal(csv[i++]);

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
