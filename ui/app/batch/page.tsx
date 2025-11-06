"use client";

import { useState } from "react";
import { Loader2, Download, X, CheckCircle, XCircle, Clock } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { DialectSelector } from "@/components/DialectSelector";
import { FeatureFlagsDialog } from "@/components/FeatureFlagsDialog";
import { useBatchConverter } from "@/hooks/useBatchConverter";
import type { FeatureFlags, Dialect } from "@/lib/types";

export default function BatchMode() {
  const [s3Uri, setS3Uri] = useState("");
  const [chunkSize, setChunkSize] = useState(1000);
  const [fromDialect, setFromDialect] = useState<Dialect>("snowflake");
  const [toDialect, setToDialect] = useState<Dialect>("e6");
  const [featureFlags, setFeatureFlags] = useState<FeatureFlags>({
    ENABLE_TABLE_ALIAS_QUALIFICATION: false,
    PRETTY_PRINT: true,
    USE_TWO_PHASE_QUALIFICATION_SCHEME: false,
    SKIP_E6_TRANSPILATION: false,
  });

  const { submitJob, downloadResults, cancelJob, loading, error, jobStatus, clear } = useBatchConverter();

  const handleSubmit = async () => {
    if (!s3Uri.trim()) return;

    await submitJob({
      s3_uri: s3Uri,
      from_sql: fromDialect,
      to_sql: toDialect,
      options: {
        pretty_print: featureFlags.PRETTY_PRINT ?? true,
        table_alias_qualification: featureFlags.ENABLE_TABLE_ALIAS_QUALIFICATION ?? false,
        use_two_phase_qualification_scheme: featureFlags.USE_TWO_PHASE_QUALIFICATION_SCHEME ?? false,
        skip_e6_transpilation: featureFlags.SKIP_E6_TRANSPILATION ?? false,
      },
      chunk_size: chunkSize,
    });
  };

  const handleDownload = async () => {
    if (jobStatus?.job_id) {
      await downloadResults(jobStatus.job_id);
    }
  };

  const handleCancel = async () => {
    if (jobStatus?.job_id) {
      await cancelJob(jobStatus.job_id);
    }
  };

  const handleClear = () => {
    setS3Uri("");
    clear();
  };

  const formatDuration = (ms?: number) => {
    if (!ms) return "N/A";
    const seconds = Math.floor(ms / 1000);
    if (seconds < 60) return `${seconds}s`;
    const minutes = Math.floor(seconds / 60);
    const remainingSeconds = seconds % 60;
    return `${minutes}m ${remainingSeconds}s`;
  };

  const isValidS3Uri = (uri: string) => {
    return /^s3:\/\/[a-zA-Z0-9.\-_]+\/.+\.parquet$/.test(uri);
  };

  const canSubmit = s3Uri.trim() && isValidS3Uri(s3Uri) && !loading;

  return (
    <div className="h-full flex flex-col bg-background">
      <header className="border-b border-border px-6 py-4">
        <div className="flex items-center justify-between">
          <div>
            <h2 className="text-xl font-semibold text-foreground">Batch Mode</h2>
            <p className="text-sm text-muted-foreground mt-1">
              Analyze large batches of SQL queries from S3 Parquet files
            </p>
          </div>
        </div>
      </header>

      <div className="flex-1 overflow-y-auto">
        <div className="max-w-7xl mx-auto p-6 space-y-6">
          {/* Input Section */}
          <div className="space-y-6">
            {/* S3 URI Input */}
            <div className="border border-border rounded-lg p-6 space-y-4">
              <h3 className="text-sm font-semibold">S3 Parquet File</h3>
              <div className="space-y-2">
                <label className="text-sm text-muted-foreground">
                  S3 URI (e.g., s3://bucket/queries.parquet)
                </label>
                <Input
                  type="text"
                  value={s3Uri}
                  onChange={(e) => setS3Uri(e.target.value)}
                  placeholder="s3://my-bucket/queries.parquet"
                  className="font-mono"
                  disabled={loading}
                />
                {s3Uri && !isValidS3Uri(s3Uri) && (
                  <p className="text-xs text-red-500">
                    Invalid S3 URI format. Must be: s3://bucket/path/file.parquet
                  </p>
                )}
                <p className="text-xs text-muted-foreground">
                  Parquet file must contain columns: <code className="font-mono">id</code> (string) and <code className="font-mono">query</code> (string)
                </p>
              </div>

              <div className="space-y-2">
                <label className="text-sm text-muted-foreground">
                  Chunk Size (queries per batch)
                </label>
                <Input
                  type="number"
                  value={chunkSize}
                  onChange={(e) => setChunkSize(Math.max(100, Math.min(10000, parseInt(e.target.value) || 1000)))}
                  min={100}
                  max={10000}
                  disabled={loading}
                />
                <p className="text-xs text-muted-foreground">
                  Number of queries to process per chunk (100-10000). Higher values use more memory but may be faster.
                </p>
              </div>
            </div>

            {/* Configuration */}
            {s3Uri && (
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
                    <FeatureFlagsDialog
                      flags={featureFlags}
                      onChange={setFeatureFlags}
                    />
                  </div>

                  <div className="flex items-center gap-2">
                    {jobStatus && (
                      <Button
                        onClick={handleClear}
                        variant="outline"
                        size="sm"
                      >
                        Clear
                      </Button>
                    )}
                    <Button
                      onClick={handleSubmit}
                      disabled={!canSubmit}
                      className="flex items-center gap-2"
                    >
                      {loading ? (
                        <>
                          <Loader2 className="h-4 w-4 animate-spin" />
                          Submitting...
                        </>
                      ) : (
                        "Submit Batch Job"
                      )}
                    </Button>
                  </div>
                </div>
              </div>
            )}
          </div>

          {/* Error Display */}
          {error && (
            <div className="p-4 border border-red-500 bg-red-50 text-red-700 rounded-lg flex items-start gap-2">
              <XCircle className="h-5 w-5 mt-0.5 flex-shrink-0" />
              <div>{error}</div>
            </div>
          )}

          {/* Job Status Display */}
          {jobStatus && (
            <div className="space-y-4">
              <div className="border border-border rounded-lg p-6 space-y-4">
                <div className="flex items-center justify-between">
                  <h3 className="text-lg font-semibold">Job Status</h3>
                  <div className="flex items-center gap-2">
                    {jobStatus.status === "completed" && (
                      <Button
                        onClick={handleDownload}
                        variant="default"
                        size="sm"
                        className="flex items-center gap-2"
                      >
                        <Download className="h-4 w-4" />
                        Download Results
                      </Button>
                    )}
                    {(jobStatus.status === "queued" || jobStatus.status === "processing") && (
                      <Button
                        onClick={handleCancel}
                        variant="destructive"
                        size="sm"
                        className="flex items-center gap-2"
                      >
                        <X className="h-4 w-4" />
                        Cancel
                      </Button>
                    )}
                  </div>
                </div>

                {/* Status Badge */}
                <div className="flex items-center gap-2">
                  <span className="text-sm text-muted-foreground">Status:</span>
                  <span className={`inline-flex items-center gap-1.5 px-2.5 py-0.5 rounded-full text-xs font-medium ${
                    jobStatus.status === "completed" ? "bg-green-100 text-green-800" :
                    jobStatus.status === "failed" ? "bg-red-100 text-red-800" :
                    jobStatus.status === "cancelled" ? "bg-gray-100 text-gray-800" :
                    jobStatus.status === "processing" ? "bg-blue-100 text-blue-800" :
                    "bg-yellow-100 text-yellow-800"
                  }`}>
                    {jobStatus.status === "processing" && <Loader2 className="h-3 w-3 animate-spin" />}
                    {jobStatus.status === "completed" && <CheckCircle className="h-3 w-3" />}
                    {jobStatus.status === "failed" && <XCircle className="h-3 w-3" />}
                    {jobStatus.status.toUpperCase()}
                  </span>
                </div>

                {/* Progress Bar */}
                <div className="space-y-2">
                  <div className="flex items-center justify-between text-sm">
                    <span className="text-muted-foreground">Progress</span>
                    <span className="font-medium">
                      {jobStatus.processed.toLocaleString()} / {jobStatus.total_queries.toLocaleString()} queries ({jobStatus.progress_percentage.toFixed(1)}%)
                    </span>
                  </div>
                  <div className="w-full bg-gray-200 rounded-full h-2.5 overflow-hidden">
                    <div
                      className={`h-2.5 rounded-full transition-all duration-300 ${
                        jobStatus.status === "completed" ? "bg-green-600" :
                        jobStatus.status === "failed" ? "bg-red-600" :
                        "bg-blue-600"
                      }`}
                      style={{ width: `${jobStatus.progress_percentage}%` }}
                    />
                  </div>
                </div>

                {/* Stats Grid */}
                <div className="grid grid-cols-2 md:grid-cols-4 gap-4 pt-4 border-t border-border">
                  <div className="space-y-1">
                    <p className="text-xs text-muted-foreground">Succeeded</p>
                    <p className="text-2xl font-semibold text-green-600">
                      {jobStatus.succeeded.toLocaleString()}
                    </p>
                  </div>
                  <div className="space-y-1">
                    <p className="text-xs text-muted-foreground">Failed</p>
                    <p className="text-2xl font-semibold text-red-600">
                      {jobStatus.failed.toLocaleString()}
                    </p>
                  </div>
                  <div className="space-y-1">
                    <p className="text-xs text-muted-foreground">Success Rate</p>
                    <p className="text-2xl font-semibold">
                      {jobStatus.success_rate.toFixed(1)}%
                    </p>
                  </div>
                  <div className="space-y-1">
                    <p className="text-xs text-muted-foreground">Duration</p>
                    <p className="text-2xl font-semibold flex items-center gap-1">
                      <Clock className="h-5 w-5" />
                      {formatDuration(jobStatus.duration_ms)}
                    </p>
                  </div>
                </div>

                {/* ETA */}
                {jobStatus.status === "processing" && jobStatus.eta_ms && (
                  <div className="p-3 bg-blue-50 border border-blue-200 rounded-lg">
                    <p className="text-sm text-blue-800">
                      <span className="font-medium">Estimated time remaining:</span> {formatDuration(jobStatus.eta_ms)}
                    </p>
                  </div>
                )}

                {/* Error Message */}
                {jobStatus.error && (
                  <div className="p-3 bg-red-50 border border-red-200 rounded-lg">
                    <p className="text-sm text-red-800">
                      <span className="font-medium">Error:</span> {jobStatus.error}
                    </p>
                  </div>
                )}

                {/* Job Details */}
                <div className="pt-4 border-t border-border space-y-2">
                  <div className="grid grid-cols-2 gap-2 text-xs">
                    <div className="flex justify-between">
                      <span className="text-muted-foreground">Job ID:</span>
                      <code className="font-mono">{jobStatus.job_id.substring(0, 8)}...</code>
                    </div>
                    <div className="flex justify-between">
                      <span className="text-muted-foreground">Chunk Size:</span>
                      <span>{jobStatus.chunk_size.toLocaleString()}</span>
                    </div>
                    <div className="flex justify-between">
                      <span className="text-muted-foreground">Created:</span>
                      <span>{new Date(jobStatus.created_at).toLocaleTimeString()}</span>
                    </div>
                    {jobStatus.started_at && (
                      <div className="flex justify-between">
                        <span className="text-muted-foreground">Started:</span>
                        <span>{new Date(jobStatus.started_at).toLocaleTimeString()}</span>
                      </div>
                    )}
                  </div>
                </div>
              </div>
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
