import { useState, useCallback, useRef } from "react";
import type {
  BatchAnalyzeRequest,
  JobSubmitResponse,
  JobStatusResponse,
  BatchAnalyzeResponse,
  BatchAnalysisResultItem,
} from "@/lib/types";

export function useBatchConverter() {
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [jobStatus, setJobStatus] = useState<JobStatusResponse | null>(null);
  const [result, setResult] = useState<BatchAnalyzeResponse | null>(null);
  const pollingIntervalRef = useRef<NodeJS.Timeout | null>(null);

  const submitJob = async (params: BatchAnalyzeRequest): Promise<string | null> => {
    setLoading(true);
    setError(null);
    setJobStatus(null);
    setResult(null);

    try {
      const response = await fetch("/api/batch/analyze", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify(params),
      });

      const data: JobSubmitResponse = await response.json();

      if (!response.ok) {
        throw new Error((data as any).detail || "Failed to submit batch job");
      }

      // Start polling for job status
      startPolling(data.job_id);

      return data.job_id;
    } catch (err) {
      const errorMessage = err instanceof Error ? err.message : "Unknown error occurred";
      setError(errorMessage);
      setLoading(false);
      return null;
    }
  };

  const fetchJobStatus = async (jobId: string): Promise<JobStatusResponse | null> => {
    try {
      const response = await fetch(`/api/batch/jobs/${jobId}`);
      const data: JobStatusResponse = await response.json();

      if (!response.ok) {
        throw new Error((data as any).detail || "Failed to fetch job status");
      }

      return data;
    } catch (err) {
      const errorMessage = err instanceof Error ? err.message : "Unknown error occurred";
      setError(errorMessage);
      return null;
    }
  };

  const startPolling = useCallback((jobId: string) => {
    // Clear any existing polling interval
    if (pollingIntervalRef.current) {
      clearInterval(pollingIntervalRef.current);
    }

    // Poll immediately
    pollJob(jobId);

    // Then poll every 2 seconds
    pollingIntervalRef.current = setInterval(() => {
      pollJob(jobId);
    }, 2000);
  }, []);

  const pollJob = async (jobId: string) => {
    const status = await fetchJobStatus(jobId);

    if (!status) {
      stopPolling();
      setLoading(false);
      return;
    }

    setJobStatus(status);

    // Stop polling if job is complete, failed, or cancelled
    if (status.status === "completed" || status.status === "failed" || status.status === "cancelled") {
      stopPolling();
      setLoading(false);
    }
  };

  const stopPolling = () => {
    if (pollingIntervalRef.current) {
      clearInterval(pollingIntervalRef.current);
      pollingIntervalRef.current = null;
    }
  };

  const downloadResults = async (jobId: string): Promise<void> => {
    try {
      const response = await fetch(`/api/batch/jobs/${jobId}/results`);

      if (!response.ok) {
        const data = await response.json();
        throw new Error(data.detail || "Failed to download results");
      }

      // Download as file
      const blob = await response.blob();
      const url = URL.createObjectURL(blob);
      const a = document.createElement("a");
      a.href = url;
      a.download = `${jobId}.jsonl`;
      document.body.appendChild(a);
      a.click();
      document.body.removeChild(a);
      URL.revokeObjectURL(url);
    } catch (err) {
      const errorMessage = err instanceof Error ? err.message : "Unknown error occurred";
      setError(errorMessage);
      throw err;
    }
  };

  const cancelJob = async (jobId: string): Promise<boolean> => {
    try {
      const response = await fetch(`/api/batch/jobs/${jobId}`, {
        method: "DELETE",
      });

      if (!response.ok) {
        const data = await response.json();
        throw new Error(data.detail || "Failed to cancel job");
      }

      stopPolling();
      return true;
    } catch (err) {
      const errorMessage = err instanceof Error ? err.message : "Unknown error occurred";
      setError(errorMessage);
      return false;
    }
  };

  const clear = () => {
    stopPolling();
    setResult(null);
    setError(null);
    setJobStatus(null);
    setLoading(false);
  };

  return {
    submitJob,
    fetchJobStatus,
    downloadResults,
    cancelJob,
    loading,
    error,
    jobStatus,
    result,
    clear,
  };
}
