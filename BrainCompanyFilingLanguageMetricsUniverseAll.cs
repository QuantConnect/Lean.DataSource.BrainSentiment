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
using NodaTime;
using ProtoBuf;
using System.IO;
using QuantConnect.Data;
using System.Collections.Generic;

namespace QuantConnect.DataSource
{
    /// <summary>
    /// Brain sentiment universe on 10-K/10-Q SEC reports
    /// </summary>
    public class BrainCompanyFilingLanguageMetricsUniverseAll : BrainCompanyFilingLanguageMetricsUniverse<BrainCompanyFilingLanguageMetricsUniverseAll>
    {
        protected override string ReportType { get; set; } = "all";
    }
}