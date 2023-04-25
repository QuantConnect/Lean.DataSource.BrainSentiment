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
using System.Globalization;
using System.IO;
using NodaTime;
using QuantConnect.Data;

namespace QuantConnect.DataSource
{
    /// <summary>
    /// Universe Selection helper class for Brain Sentiment dataset
    /// </summary>
    public class BrainSentimentIndicatorUniverse : BaseData
    {
        private static readonly TimeSpan _period = TimeSpan.FromDays(1);
        
        /// <summary>
        /// Total Article Mentions in 7 days
        /// </summary>
        public int? TotalArticleMentions7Days { get; set; }

        /// <summary>
        /// Sentimental Article Mentions in 7 days
        /// </summary>
        public decimal? SentimentalArticleMentions7Days { get; set; }

        /// <summary>
        /// Setiment Score in 7 days
        /// </summary>
        public decimal? Sentiment7Days { get; set; }

        /// <summary>
        /// Total Buzz Volume in 7 days
        /// </summary>
        public decimal? TotalBuzzVolume7Days { get; set; }

        /// <summary>
        /// Sentimental Buzz Volume in 7 days
        /// </summary>
        public decimal? SentimentalBuzzVolume7Days { get; set; }

        /// <summary>
        /// Total Article Mentions in 30 days
        /// </summary>
        public int? TotalArticleMentions30Days { get; set; }

        /// <summary>
        /// Sentimental Article Mentions in 30 days
        /// </summary>
        public decimal? SentimentalArticleMentions30Days { get; set; }

        /// <summary>
        /// Setiment Score in 30 days
        /// </summary>
        public decimal? Sentiment30Days { get; set; }

        /// <summary>
        /// Total Buzz Volume in 30 days
        /// </summary>
        public decimal? TotalBuzzVolume30Days { get; set; }

        /// <summary>
        /// Sentimental Buzz Volume in 30 days
        /// </summary>
        public decimal? SentimentalBuzzVolume30Days { get; set; }

        /// <summary>
        /// Time the data became available
        /// </summary>
        public override DateTime EndTime => Time + _period;

        /// <summary>
        /// Return the URL string source of the file. This will be converted to a stream
        /// </summary>
        /// <param name="config">Configuration object</param>
        /// <param name="date">Date of this source file</param>
        /// <param name="isLiveMode">true if we're in live mode, false for backtesting mode</param>
        /// <returns>String URL of source file.</returns>
        public override SubscriptionDataSource GetSource(SubscriptionDataConfig config, DateTime date, bool isLiveMode)
        {
            return new SubscriptionDataSource(
                Path.Combine(
                    Globals.DataFolder,
                    "alternative",
                    "brain",
                    "sentiment",
                    "universe",
                    $"{date:yyyyMMdd}.csv"
                ),
                SubscriptionTransportMedium.LocalFile
            );
        }

        /// <summary>
        /// Parses the data from the line provided and loads it into LEAN
        /// </summary>
        /// <param name="config">Subscription configuration</param>
        /// <param name="line">Line of data</param>
        /// <param name="date">Date</param>
        /// <param name="isLiveMode">Is live mode</param>
        /// <returns>New instance</returns>
        public override BaseData Reader(SubscriptionDataConfig config, string line, DateTime date, bool isLiveMode)
        {
            var csv = line.Split(',');
            var sentiment7Days = csv[4].IfNotNullOrEmpty<decimal?>(s => decimal.Parse(s, NumberStyles.Any, CultureInfo.InvariantCulture));

            return new BrainSentimentIndicatorUniverse
            {
                TotalArticleMentions7Days = csv[2].IfNotNullOrEmpty<int?>(s => int.Parse(s)),
                SentimentalArticleMentions7Days = csv[3].IfNotNullOrEmpty<decimal?>(s => decimal.Parse(s, NumberStyles.Any, CultureInfo.InvariantCulture)),
                Sentiment7Days = sentiment7Days,
                TotalBuzzVolume7Days = csv[5].IfNotNullOrEmpty<decimal?>(s => decimal.Parse(s, NumberStyles.Any, CultureInfo.InvariantCulture)),
                SentimentalBuzzVolume7Days = csv[6].IfNotNullOrEmpty<decimal?>(s => decimal.Parse(s, NumberStyles.Any, CultureInfo.InvariantCulture)),

                TotalArticleMentions30Days = csv[7].IfNotNullOrEmpty<int?>(s => int.Parse(s)),
                SentimentalArticleMentions30Days = csv[8].IfNotNullOrEmpty<decimal?>(s => decimal.Parse(s, NumberStyles.Any, CultureInfo.InvariantCulture)),
                Sentiment30Days = csv[9].IfNotNullOrEmpty<decimal?>(s => decimal.Parse(s, NumberStyles.Any, CultureInfo.InvariantCulture)),
                TotalBuzzVolume30Days = csv[10].IfNotNullOrEmpty<decimal?>(s => decimal.Parse(s, NumberStyles.Any, CultureInfo.InvariantCulture)),
                SentimentalBuzzVolume30Days = csv[11].IfNotNullOrEmpty<decimal?>(s => decimal.Parse(s, NumberStyles.Any, CultureInfo.InvariantCulture)),

                Symbol = new Symbol(SecurityIdentifier.Parse(csv[0]), csv[1]),
                // subtract 12 hours to match the base type data time
                Time = date.ConvertFromUtc(DataTimeZone()).AddHours(-12),
                Value = sentiment7Days ?? 0m
            };
        }

        /// <summary>
        /// Converts the instance to string
        /// </summary>
        public override string ToString()
        {
            return $@"{Symbol},
                    {TotalArticleMentions7Days},
                    {SentimentalArticleMentions7Days},
                    {Sentiment7Days},
                    {TotalBuzzVolume7Days},
                    {SentimentalBuzzVolume7Days},
                    {TotalArticleMentions30Days},
                    {SentimentalArticleMentions30Days},
                    {Sentiment30Days},
                    {TotalBuzzVolume30Days},
                    {SentimentalBuzzVolume30Days}";
        }

        /// <summary>
        /// Gets the default resolution for this data and security type
        /// </summary>
        public override Resolution DefaultResolution()
        {
            return Resolution.Daily;
        }

        /// <summary>
        /// Gets the supported resolution for this data and security type
        /// </summary>
        public override List<Resolution> SupportedResolutions()
        {
            return DailyResolution;
        }

        /// <summary>
        /// Specifies the data time zone for this data type. This is useful for custom data types
        /// </summary>
        /// <returns>The <see cref="T:NodaTime.DateTimeZone" /> of this data type</returns>
        public override DateTimeZone DataTimeZone()
        {
            return TimeZones.NewYork;
        }
    }
}