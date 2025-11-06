"use client";

import { useState } from "react";
import { Loader2 } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { DialectSelector } from "@/components/DialectSelector";
import { FeatureFlagsDialog } from "@/components/FeatureFlagsDialog";
import { CSVUpload } from "@/components/batch/CSVUpload";
import { BatchSummaryStats } from "@/components/batch/BatchSummaryStats";
import { BatchResultsTable } from "@/components/batch/BatchResultsTable";
import { BatchQueryDetail } from "@/components/batch/BatchQueryDetail";
import { useBatchConverter } from "@/hooks/useBatchConverter";
import type { BatchQueryItem, BatchAnalysisResultItem, FeatureFlags, Dialect } from "@/lib/types";

export default function BatchMode() {
  const [queries, setQueries] = useState<BatchQueryItem[]>([]);
  const [fromDialect, setFromDialect] = useState<Dialect>("snowflake");
  const [toDialect, setToDialect] = useState<Dialect>("e6");
  const [stopOnError, setStopOnError] = useState(false);
  const [featureFlags, setFeatureFlags] = useState<FeatureFlags>({
    ENABLE_TABLE_ALIAS_QUALIFICATION: false,
    PRETTY_PRINT: true,
    USE_TWO_PHASE_QUALIFICATION_SCHEME: false,
    SKIP_E6_TRANSPILATION: false,
  });
  const [selectedQuery, setSelectedQuery] = useState<BatchAnalysisResultItem | null>(null);

  const { analyze, loading, error, result, clear } = useBatchConverter();

  const handleAnalyze = async () => {
    if (queries.length === 0) return;

    await analyze({
      queries,
      from_sql: fromDialect,
      to_sql: toDialect,
      options: featureFlags,
      stop_on_error: stopOnError,
    });
  };

  const handleClear = () => {
    setQueries([]);
    clear();
  };

  const handleDownloadAll = () => {
    if (!result) return;

    const allTranspiled = result.results
      .filter((r) => r.status === "success" && r.transpiled_query)
      .map((r) => `-- Query ID: ${r.id}\n${r.transpiled_query}`)
      .join("\n\n-- " + "=".repeat(80) + "\n\n");

    const blob = new Blob([allTranspiled], { type: "text/plain" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = "batch_transpiled_queries.sql";
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(url);
  };

  return (
    <div className="h-full flex flex-col bg-background">
      <header className="border-b border-border px-6 py-4">
        <div className="flex items-center justify-between">
          <div>
            <h2 className="text-xl font-semibold text-foreground">Batch Mode</h2>
            <p className="text-sm text-muted-foreground mt-1">
              Analyze multiple SQL queries at once
            </p>
          </div>
        </div>
      </header>

      <div className="flex-1 overflow-y-auto">
        <div className="max-w-7xl mx-auto p-6 space-y-6">
          {/* Input Section */}
          <div className="space-y-6">
            {/* CSV Upload */}
            <div className="border border-border rounded-lg p-6 space-y-4">
              <h3 className="text-sm font-semibold">Upload Queries</h3>
              <CSVUpload onQueriesLoaded={setQueries} onClear={handleClear} />
            </div>

            {/* Configuration */}
            {queries.length > 0 && (
              <div className="border border-border rounded-lg p-6 space-y-4">
                <h3 className="text-sm font-semibold">Configuration</h3>

                <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                  <div className="space-y-2">
                    <label className="text-sm text-muted-foreground">From Dialect</label>
                    <DialectSelector
                      value={fromDialect}
                      onChange={setFromDialect}
                    />
                  </div>

                  <div className="space-y-2">
                    <label className="text-sm text-muted-foreground">To Dialect</label>
                    <DialectSelector
                      value={toDialect}
                      onChange={setToDialect}
                    />
                  </div>
                </div>

                <div className="flex items-center justify-between pt-4 border-t border-border">
                  <div className="flex items-center gap-4">
                    <label className="flex items-center gap-2 text-sm">
                      <input
                        type="checkbox"
                        checked={stopOnError}
                        onChange={(e) => setStopOnError(e.target.checked)}
                        className="rounded"
                      />
                      <span>Stop on first error</span>
                    </label>

                    <FeatureFlagsDialog
                      flags={featureFlags}
                      onChange={setFeatureFlags}
                    />
                  </div>

                  <Button
                    onClick={handleAnalyze}
                    disabled={loading || queries.length === 0}
                    className="flex items-center gap-2"
                  >
                    {loading ? (
                      <>
                        <Loader2 className="h-4 w-4 animate-spin" />
                        Analyzing {queries.length} {queries.length === 1 ? "query" : "queries"}...
                      </>
                    ) : (
                      `Analyze ${queries.length} ${queries.length === 1 ? "Query" : "Queries"}`
                    )}
                  </Button>
                </div>
              </div>
            )}
          </div>

          {/* Error Display */}
          {error && (
            <div className="p-4 border border-red-500 bg-red-50 text-red-700 rounded-lg">
              {error}
            </div>
          )}

          {/* Results Section */}
          {result && (
            <div className="space-y-4">
              <div className="flex items-center justify-between">
                <h3 className="text-lg font-semibold">Results</h3>
                <Button
                  onClick={handleDownloadAll}
                  variant="outline"
                  size="sm"
                  className="flex items-center gap-2"
                >
                  Download All
                </Button>
              </div>

              <Tabs defaultValue="summary" className="w-full">
                <TabsList>
                  <TabsTrigger value="summary">Summary</TabsTrigger>
                  <TabsTrigger value="results">
                    Results ({result.results.length})
                  </TabsTrigger>
                </TabsList>

                <TabsContent value="summary" className="space-y-4 mt-6">
                  <BatchSummaryStats summary={result.summary} />
                </TabsContent>

                <TabsContent value="results" className="space-y-4 mt-6">
                  <BatchResultsTable
                    results={result.results}
                    onViewDetails={setSelectedQuery}
                  />
                </TabsContent>
              </Tabs>
            </div>
          )}
        </div>
      </div>

      {/* Query Detail Modal */}
      <BatchQueryDetail
        result={selectedQuery}
        onClose={() => setSelectedQuery(null)}
      />
    </div>
  );
}
