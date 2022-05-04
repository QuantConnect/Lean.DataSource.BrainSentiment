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
using System.Linq;
using NodaTime;
using ProtoBuf;
using QuantConnect;
using QuantConnect.Data;

namespace QuantConnect.DataSource
{
    /// <summary>
    /// Brain sentiment on 10-K/10-Q SEC reports
    /// </summary>
    [ProtoContract(SkipConstructor = true)]
    public class BrainCompanyFilingLanguageMetricsBase<T> : BaseData
        where T : BrainCompanyFilingLanguageMetricsBase<T>, new()
    {
        public DateTime ReportDate { get; set; }

        public string ReportCategory { get; set; }

        public int? ReportPeriod { get; set; }

        public DateTime? PreviousReportDate { get; set; }

        public string PreviousReportCategory { get; set; }

        public int? PreviousReportPeriod { get; set; }

        public BrainCompanyFilingLanguageMetrics ReportSentiment { get; set; }

        public BrainCompanyFilingLanguageMetrics RiskFactorsStatementSentiment { get; set; }

        public BrainCompanyFilingLanguageMetrics ManagementDiscussionAnalyasisOfFinancialConditionAndResultsOfOperations { get; set; }
        
        protected virtual string ReportType { get; set; }

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
                    $"report_{ReportType.ToLowerInvariant()}",
                    $"{date:yyyyMM}",
                    $"{config.Symbol.Value.ToLowerInvariant()}.csv"
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
            if (string.IsNullOrWhiteSpace(line))
            {
                return null;
            }

            var csv = line.Split(',').ToList();
            var dataDate = csv[0];
            csv = csv.Skip(1).ToList();

            var baseInfo = csv.Take(2).ToList();
            var diffBaseInfo = csv.Skip(36).Take(3).ToList();
            baseInfo.Add(csv[35]);

            var reportMetrics = csv.Skip(2).Take(11).ToList();
            var riskFactorMetrics = csv.Skip(13).Take(11).ToList();
            var mdMetrics = csv.Skip(24).Take(11).ToList();

            var baseSimilarity = csv.Skip(39).Take(7).ToList();
            var riskFactorSimilarity = csv.Skip(46).Take(3).ToList();
            var mdSimilarity = csv.Skip(49).ToList();

            var data = (BrainCompanyFilingLanguageMetricsBase<T>)((object)new T());

            data.ReportDate = Parse.DateTimeExact(baseInfo[0], "yyyy-MM-dd");
            data.ReportCategory = baseInfo[1];
            //Console.WriteLine($"{config.Symbol.Value}, {baseInfo[0]}, {baseInfo[1]}, {baseInfo[2]}, {line}");
            data.ReportPeriod = !string.IsNullOrWhiteSpace(baseInfo[2])
                ? (int)Parse.Decimal(baseInfo[2])
                : null;
            data.PreviousReportDate = !string.IsNullOrWhiteSpace(diffBaseInfo[0])
                ? Parse.DateTimeExact(diffBaseInfo[0], "yyyy-MM-dd")
                : null;
            data.PreviousReportCategory = !string.IsNullOrWhiteSpace(diffBaseInfo[1])
                ? diffBaseInfo[1]
                : null;
            data.PreviousReportPeriod = !string.IsNullOrWhiteSpace(diffBaseInfo[2])
                ? (int)Parse.Decimal(diffBaseInfo[2])
                : null;

            data.ReportSentiment = BrainCompanyFilingLanguageMetrics.Parse(reportMetrics, baseSimilarity);
            data.RiskFactorsStatementSentiment = BrainCompanyFilingLanguageMetrics.Parse(riskFactorMetrics, riskFactorSimilarity);
            data.ManagementDiscussionAnalyasisOfFinancialConditionAndResultsOfOperations = BrainCompanyFilingLanguageMetrics.Parse(mdMetrics, mdSimilarity);

            data.Symbol = config.Symbol;
            data.EndTime = QuantConnect.Parse.DateTimeExact(dataDate, "yyyyMMdd").AddHours(12);

            return data;
        }

        /// <summary>
        /// Clones the data
        /// </summary>
        /// <returns>A clone of the object</returns>
        protected T CloneData()
        {
            var data = (BrainCompanyFilingLanguageMetricsBase<T>)((object)new T());

            data.ReportDate = ReportDate;
            data.ReportCategory = ReportCategory;
            data.ReportPeriod = ReportPeriod;
            data.PreviousReportDate = PreviousReportDate;
            data.PreviousReportCategory = PreviousReportCategory;
            data.PreviousReportPeriod = PreviousReportPeriod;

            data.ReportSentiment = ReportSentiment;
            data.RiskFactorsStatementSentiment = RiskFactorsStatementSentiment;
            data.ManagementDiscussionAnalyasisOfFinancialConditionAndResultsOfOperations = ManagementDiscussionAnalyasisOfFinancialConditionAndResultsOfOperations;

            data.Symbol = Symbol;
            data.EndTime = EndTime;

            return (T)data;
        }

        public override BaseData Clone()
        {
            return CloneData();
        }

        /// <summary>
        /// Indicates whether the data source is tied to an underlying symbol and requires that corporate events be applied to it as well, such as renames and delistings
        /// </summary>
        /// <returns>false</returns>
        public override bool RequiresMapping()
        {
            return true;
        }

        /// <summary>
        /// Indicates whether the data is sparse.
        /// If true, we disable logging for missing files
        /// </summary>
        /// <returns>true</returns>
        public override bool IsSparseData()
        {
            return true;
        }

        /// <summary>
        /// Converts the instance to string
        /// </summary>
        public override string ToString()
        {
            return $"{Symbol} - Category: {ReportCategory}, Sentiment: {(ReportSentiment.Sentiment?.ToStringInvariant() ?? "N/A")}";
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
            return DateTimeZone.Utc;
        }
    }
}
