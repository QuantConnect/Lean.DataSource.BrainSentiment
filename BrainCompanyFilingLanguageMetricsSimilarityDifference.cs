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
using QuantConnect;
using QuantConnect.Data;

namespace QuantConnect.DataSource
{
    public class BrainCompanyFilingLanguageMetricsSimilarityDifference
    {
        public decimal? All;

        public decimal? Positive { get; set; }

        public decimal? Negative { get; set; }

        public decimal? Uncertainty { get; set; }

        public decimal? Litigious { get; set; }

        public decimal? Constraining { get; set; }

        public decimal? Interesting { get; set; }

        public static BrainCompanyFilingLanguageMetricsSimilarityDifference Parse(List<string> similarityValues)
        {
            var limited = similarityValues.Count <= 3;
            return new BrainCompanyFilingLanguageMetricsSimilarityDifference
            {
                All = !string.IsNullOrWhiteSpace(similarityValues[0]) ? QuantConnect.Parse.Decimal(similarityValues[0]) : null,
                Positive = !string.IsNullOrWhiteSpace(similarityValues[1]) ? QuantConnect.Parse.Decimal(similarityValues[1]) : null,
                Negative = !string.IsNullOrWhiteSpace(similarityValues[2]) ? QuantConnect.Parse.Decimal(similarityValues[2]) : null,
                Uncertainty = !limited && !string.IsNullOrWhiteSpace(similarityValues[3]) ? QuantConnect.Parse.Decimal(similarityValues[3]) : null,
                Litigious = !limited && !string.IsNullOrWhiteSpace(similarityValues[4]) ? QuantConnect.Parse.Decimal(similarityValues[4]) : null,
                Constraining = !limited && !string.IsNullOrWhiteSpace(similarityValues[5]) ? QuantConnect.Parse.Decimal(similarityValues[5]) : null,
                Interesting = !limited && !string.IsNullOrWhiteSpace(similarityValues[6]) ? QuantConnect.Parse.Decimal(similarityValues[6]) : null,
            };
        }
    }
}