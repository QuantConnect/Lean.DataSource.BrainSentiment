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
using QuantConnect.Data;
using QuantConnect.Data.UniverseSelection;
using QuantConnect.DataSource;

namespace QuantConnect.Algorithm.CSharp
{
    public class BrainCompanyFilingLanguageMetricsUniverseAlgorithm : QCAlgorithm
    {
        public override void Initialize()
        {
            // Data ADDED via universe selection is added with Daily resolution.
            UniverseSettings.Resolution = Resolution.Daily;

            SetStartDate(2021, 2, 14);
            SetEndDate(2021, 2, 18);
            SetCash(100000);

            // add a custom universe data source (defaults to usa-equity)
            AddUniverse<BrainCompanyFilingLanguageMetricsUniverseAll>("BrainCompanyFilingLanguageMetricsUniverseAll", Resolution.Daily, data =>
            {
                foreach (var datum in data)
                {
                    Log($@"{datum.Symbol},
                    {datum.ReportSentiment.Sentiment},
                    {datum.RiskFactorsStatementSentiment.Uncertainty},
                    {datum.ManagementDiscussionAnalyasisOfFinancialConditionAndResultsOfOperations.Litigious},
                    {datum.ReportSentiment.Constraining},
                    {datum.RiskFactorsStatementSentiment.Interesting},
                    {datum.ManagementDiscussionAnalyasisOfFinancialConditionAndResultsOfOperations.Readability},
                    {datum.ReportSentiment.LexicalRichness},
                    {datum.RiskFactorsStatementSentiment.LexicalDensity},
                    {datum.ManagementDiscussionAnalyasisOfFinancialConditionAndResultsOfOperations.SpecificDensity},
                    {datum.ReportSentiment.SentenceCount},
                    {datum.RiskFactorsStatementSentiment.MeanSentenceLength}");
                }

                // define our selection criteria
                return from d in data 
                    where d.ReportSentiment.Sentiment > 0m && d.ManagementDiscussionAnalyasisOfFinancialConditionAndResultsOfOperations.Sentiment > 0m
                    select d.Symbol;
            });
        }
        
        public override void OnSecuritiesChanged(SecurityChanges changes)
        {
            Log(changes.ToString());
        }
    }
}