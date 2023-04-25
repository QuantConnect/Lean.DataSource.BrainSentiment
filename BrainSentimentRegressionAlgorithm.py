# QUANTCONNECT.COM - Democratizing Finance, Empowering Individuals.
# Lean Algorithmic Trading Engine v2.0. Copyright 2014 QuantConnect Corporation.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from AlgorithmImports import *

class BrainSentimentRegressionAlgorithm(QCAlgorithm): 
    
    def Initialize(self):
        # Data ADDED via universe selection is added with Daily resolution.
        self.UniverseSettings.Resolution = Resolution.Daily

        self.SetStartDate(2021, 2, 1)
        self.SetEndDate(2021, 2, 10)
        self.SetCash(100000)

        self.aapl = self.AddEquity("AAPL").Symbol
        self.custom_data_symbol = self.AddData(BrainSentimentIndicator7Day, self.aapl).Symbol

        # add a custom universe data source (defaults to usa-equity)
        self.AddUniverse(BrainSentimentIndicatorUniverse, "BrainSentimentIndicatorUniverse", Resolution.Daily, self.UniverseSelection)

    def OnData(self, slice):
        data = slice.Get(BrainSentimentIndicator7Day)
        if data:
            sentiment = data[self.custom_data_symbol].Sentiment
            self.Debug(f"{self.Time}:: received sentiment base data:: {sentiment}")
    
    def UniverseSelection(self, data):
        aapl = [d for d in data if d.Symbol == self.aapl][0]
        sentiment = aapl.Sentiment7Days
        self.Debug(f"{self.Time}:: received universe selection data:: {sentiment}")
        return [aapl.Symbol]
