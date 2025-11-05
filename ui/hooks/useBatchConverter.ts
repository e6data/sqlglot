import { useState } from "react";
import type {
  BatchAnalyzeRequest,
  BatchAnalyzeResponse,
} from "@/lib/types";

export function useBatchConverter() {
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [result, setResult] = useState<BatchAnalyzeResponse | null>(null);

  const analyze = async (params: BatchAnalyzeRequest) => {
    setLoading(true);
    setError(null);
    setResult(null);

    try {
      const response = await fetch("/api/batch/analyze", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify(params),
      });

      const data = await response.json();

      if (!response.ok) {
        throw new Error(data.detail || "Batch analysis failed");
      }

      setResult(data);
    } catch (err) {
      const errorMessage = err instanceof Error ? err.message : "Unknown error occurred";
      setError(errorMessage);
    } finally {
      setLoading(false);
    }
  };

  const clear = () => {
    setResult(null);
    setError(null);
  };

  return {
    analyze,
    loading,
    error,
    result,
    clear,
  };
}
