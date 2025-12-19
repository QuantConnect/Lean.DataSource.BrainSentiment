namespace QuantConnect.DataSource
{
    /// <summary>
    /// Brain Earnings Call Language Metrics (BLMECT).
    ///
    /// Provides daily language-analysis metrics computed from the most recent earnings call
    /// available as of each calculation date for a given symbol.
    ///
    /// The dataset decomposes each earnings call transcript into three sections:
    /// <list type="bullet">
    ///   <item>
    ///     <description>
    ///     <b>MD (Management Discussion)</b> – prepared remarks by company management.
    ///     </description>
    ///   </item>
    ///   <item>
    ///     <description>
    ///     <b>AQ (Analyst Questions)</b> – questions asked by sell-side analysts.
    ///     </description>
    ///   </item>
    ///   <item>
    ///     <description>
    ///     <b>MA (Management Answers)</b> – management responses during the Q&amp;A session.
    ///     </description>
    ///   </item>
    /// </list>
    ///
    /// For each section, the dataset includes:
    /// <list type="bullet">
    ///   <item><description>Absolute language metrics (sentiment, uncertainty, readability, etc.)</description></item>
    ///   <item><description>Changes (deltas) relative to the previous earnings call</description></item>
    ///   <item><description>Text similarity scores comparing the latest and previous calls</description></item>
    /// </list>
    ///
    /// The <see cref="BaseData.Time"/> value represents the calculation date (DATE),
    /// not the earnings call date. Transcript metadata fields such as
    /// <c>LastTranscriptDate</c>, <c>LastTranscriptQuarter</c>, and <c>LastTranscriptYear</c>
    /// identify the earnings call used to compute the metrics.
    ///
    /// This dataset is sparse, daily, and mapping-aware, allowing seamless use across
    /// ticker changes and corporate actions in Lean algorithms.
    /// </summary>
    public class BrainLanguageMetricsEarningsCalls
        : BrainLanguageMetricsEarningsCallsBase<BrainLanguageMetricsEarningsCalls>
    {
    }
}
