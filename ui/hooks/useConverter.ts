"use client";

import { useState } from "react";
import { type ConvertParams, type AnalyzeResponse, type ConvertError } from "@/lib/types";

export function useConverter() {
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [result, setResult] = useState<AnalyzeResponse | null>(null);

  const convert = async (params: ConvertParams) => {
    setLoading(true);
    setError(null);

    try {
      const response = await fetch("/api/convert", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          query: params.query,
          from_sql: params.fromDialect,
          feature_flags: params.featureFlags,
        }),
      });

      const data = await response.json();

      if (!response.ok) {
        throw new Error((data as ConvertError).detail || "Conversion failed");
      }

      setResult(data as AnalyzeResponse);
    } catch (err) {
      const message = err instanceof Error ? err.message : "Unknown error occurred";
      setError(message);
      setResult(null);
    } finally {
      setLoading(false);
    }
  };

  const clear = () => {
    setResult(null);
    setError(null);
  };

  return { convert, loading, error, result, clear };
}
