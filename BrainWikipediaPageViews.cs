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
using System.Collections.Generic;
using System.IO;
using NodaTime;
using QuantConnect;
using QuantConnect.Data;

namespace QuantConnect.DataSource
{
    /// <summary>
    /// Brain Wikipedia Page Views (BWPV)
    ///
    /// The dataset monitors Wikipedia page views and "buzz" metrics for
    /// the top ~1000 US companies. It provides the raw views and buzz
    /// scores over different horizons (1, 7, and 30 days).
    /// </summary>
    public class BrainWikipediaPageViews : BaseData
    {
        /// <summary>
        /// Raw number of views for the past day
        /// </summary>
        public decimal? NumberViews1 { get; set; }

        /// <summary>
        /// "Buzz" metric over the past day
        /// </summary>
        public decimal? Buzz1 { get; set; }

        /// <summary>
        /// Raw number of views over the past 7 days
        /// </summary>
        public decimal? NumberViews7 { get; set; }

        /// <summary>
        /// "Buzz" metric over the past 7 days
        /// </summary>
        public decimal? Buzz7 { get; set; }

        /// <summary>
        /// Raw number of views over the past 30 days
        /// </summary>
        public decimal? NumberViews30 { get; set; }

        /// <summary>
        /// "Buzz" metric over the past 30 days
        /// </summary>
        public decimal? Buzz30 { get; set; }

        /// <summary>
        /// Returns the path to the daily data file for a given symbol and month.
        ///
        /// Folder structure (Option A):
        ///     alternative/brain/bwpv/{yyyyMM}/{symbol}.csv
        ///
        /// Each file contains multiple daily rows for the given symbol and month.
        /// </summary>
        public override SubscriptionDataSource GetSource(SubscriptionDataConfig config, DateTime date, bool isLiveMode)
        {
            return new SubscriptionDataSource(
                Path.Combine(
                    Globals.DataFolder,
                    "alternative",
                    "brain",
                    "bwpv",
                    $"{config.Symbol.Value.ToLowerInvariant()}.csv"
                ),
                SubscriptionTransportMedium.LocalFile
            );
        }

        /// <summary>
        /// Parses a line from the BWPV data file into a <see cref="BrainWikipediaPageViews"/> instance.
        ///
        /// File format (per line):
        ///   0: yyyyMMdd (data date)
        ///   1: NumberViews1
        ///   2: Buzz1
        ///   3: NumberViews7
        ///   4: Buzz7
        ///   5: NumberViews30
        ///   6: Buzz30
        /// </summary>
        public override BaseData Reader(SubscriptionDataConfig config, string line, DateTime date, bool isLiveMode)
        {
            if (string.IsNullOrWhiteSpace(line))
            {
                return null;
            }

            var csv = line.Split(',');

            if (csv.Length < 7)
            {
                return null;
            }

            var dataDate = Parse.DateTimeExact(csv[0], "yyyyMMdd");

            var data = new BrainWikipediaPageViews
            {
                Symbol = config.Symbol,
                Time = dataDate,
                EndTime = dataDate.AddHours(12),
                NumberViews1 = ParseNullableDecimal(csv[1]),
                Buzz1 = ParseNullableDecimal(csv[2]),
                NumberViews7 = ParseNullableDecimal(csv[3]),
                Buzz7 = ParseNullableDecimal(csv[4]),
                NumberViews30 = ParseNullableDecimal(csv[5]),
                Buzz30 = ParseNullableDecimal(csv[6])
            };

            return data;
        }

        private static decimal? ParseNullableDecimal(string value)
        {
            if (string.IsNullOrWhiteSpace(value))
            {
                return null;
            }
            return Parse.Decimal(value);
        }

        /// <summary>
        /// Clones this instance
        /// </summary>
        public override BaseData Clone()
        {
            return new BrainWikipediaPageViews
            {
                Symbol = Symbol,
                Time = Time,
                EndTime = EndTime,
                NumberViews1 = NumberViews1,
                Buzz1 = Buzz1,
                NumberViews7 = NumberViews7,
                Buzz7 = Buzz7,
                NumberViews30 = NumberViews30,
                Buzz30 = Buzz30
            };
        }

        /// <summary>
        /// Indicates whether the data source requires symbol mapping
        /// </summary>
        public override bool RequiresMapping()
        {
            return true;
        }

        /// <summary>
        /// Indicates whether the data is sparse
        /// </summary>
        public override bool IsSparseData()
        {
            return true;
        }

        /// <summary>
        /// Gets the default resolution (daily)
        /// </summary>
        public override Resolution DefaultResolution()
        {
            return Resolution.Daily;
        }

        /// <summary>
        /// Gets the supported resolutions (daily only)
        /// </summary>
        public override List<Resolution> SupportedResolutions()
        {
            return DailyResolution;
        }

        /// <summary>
        /// Gets the data time zone (UTC)
        /// </summary>
        public override DateTimeZone DataTimeZone()
        {
            return DateTimeZone.Utc;
        }

        /// <summary>
        /// Converts this instance to a string
        /// </summary>
        public override string ToString()
        {
            return $"{Symbol} - BWPV (Buzz30: {Buzz30?.ToStringInvariant() ?? "N/A"})";
        }
    }
}
