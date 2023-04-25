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

using System.Collections.Generic;
using System.Linq;
using QuantConnect.Data;
using QuantConnect.Data.UniverseSelection;
using QuantConnect.DataSource;

namespace QuantConnect.Algorithm.CSharp
{
    public class BrainSentimentRegressionAlgorithm : QCAlgorithm
    {
        public Symbol _aapl, _customDataSymbol;
        public DateTime _last;

        public override void Initialize()
        {
            // Data ADDED via universe selection is added with Daily resolution.
            UniverseSettings.Resolution = Resolution.Daily;

            SetStartDate(2021, 2, 1);
            SetEndDate(2021, 2, 10);
            SetCash(100000);

            _aapl = AddEquity("AAPL").Symbol;
            _customDataSymbol = AddData<BrainSentimentIndicator7Day>(_aapl).Symbol;

            // add a custom universe data source (defaults to usa-equity)
            AddUniverse<BrainSentimentIndicatorUniverse>("BrainSentimentIndicatorUniverse", Resolution.Daily, (data) => {
                _last = Time;
                return Enumerable.Empty<Symbol>();
            });
        }

        public override void OnData(Slice slice)
        {
            var data = slice.Get<BrainSentimentIndicator7Day>();
            if (data.ContainsKey(_customDataSymbol) && Time !=_last)
            {
                throw new Exception($"Universe and data time is not matching");
            }
        }
    }
}