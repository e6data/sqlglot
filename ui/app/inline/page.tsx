"use client";

import { useState } from "react";
import { Copy, Download, Loader2 } from "lucide-react";
import { SQLEditor } from "@/components/SQLEditor";
import { DialectSelector } from "@/components/DialectSelector";
import { FeatureFlagsDialog } from "@/components/FeatureFlagsDialog";
import { Button } from "@/components/ui/button";
import { useConverter } from "@/hooks/useConverter";
import { ExecutableStatus } from "@/components/analysis/ExecutableStatus";
import { FunctionsList } from "@/components/analysis/FunctionsList";
import { MetadataDisplay } from "@/components/analysis/MetadataDisplay";
import { type Dialect, type FeatureFlags } from "@/lib/types";

export default function InlineMode() {
  const [sourceQuery, setSourceQuery] = useState("");
  const [dialect, setDialect] = useState<Dialect>("snowflake");
  const [flags, setFlags] = useState<FeatureFlags>({ PRETTY_PRINT: true });
  const { convert, loading, error, result } = useConverter();

  const handleConvert = () => {
    if (!sourceQuery.trim()) return;
    convert({ query: sourceQuery, fromDialect: dialect, featureFlags: flags });
  };

  const handleCopy = async () => {
    if (result) {
      await navigator.clipboard.writeText(result.transpiled_query);
    }
  };

  const handleDownload = () => {
    if (result) {
      const blob = new Blob([result.transpiled_query], { type: "text/plain" });
      const url = URL.createObjectURL(blob);
      const a = document.createElement("a");
      a.href = url;
      a.download = "converted_query.sql";
      a.click();
      URL.revokeObjectURL(url);
    }
  };

  return (
    <div className="h-full flex flex-col bg-background">
      <header className="border-b border-border px-6 py-4">
        <div className="flex items-center justify-between">
          <h2 className="text-xl font-semibold text-foreground">Inline Mode</h2>
          <FeatureFlagsDialog flags={flags} onChange={setFlags} />
        </div>
      </header>

      <div className="flex-1 p-6 overflow-auto">
        <div className="grid grid-cols-2 gap-6">
          {/* Source Panel */}
          <div className="space-y-4">
            <div className="flex items-center justify-between">
              <h2 className="text-lg font-semibold">Source SQL</h2>
              <DialectSelector value={dialect} onChange={setDialect} />
            </div>
            <SQLEditor value={sourceQuery} onChange={setSourceQuery} />
            <Button
              onClick={handleConvert}
              disabled={loading || !sourceQuery.trim()}
              className="w-full"
            >
              {loading ? (
                <>
                  <Loader2 className="mr-2 h-4 w-4 animate-spin" />
                  Converting...
                </>
              ) : (
                "Convert to E6"
              )}
            </Button>
          </div>

          {/* Target Panel */}
          <div className="space-y-4">
            <div className="flex items-center justify-between">
              <h2 className="text-lg font-semibold">E6 Output</h2>
              <div className="flex gap-2">
                <Button
                  variant="outline"
                  size="sm"
                  onClick={handleCopy}
                  disabled={!result}
                >
                  <Copy className="h-4 w-4 mr-2" />
                  Copy
                </Button>
                <Button
                  variant="outline"
                  size="sm"
                  onClick={handleDownload}
                  disabled={!result}
                >
                  <Download className="h-4 w-4 mr-2" />
                  Download
                </Button>
              </div>
            </div>
            <SQLEditor value={result?.transpiled_query || ""} onChange={() => {}} readOnly />

            {result && (
              <div className="space-y-4 p-4 border border-border rounded-lg bg-background">
                <ExecutableStatus executable={result.executable} />
                <FunctionsList functions={result.functions} />
                <MetadataDisplay metadata={result.metadata} />
              </div>
            )}

            {error && (
              <div className="p-4 border border-red-500 bg-red-50 text-red-700 text-sm">
                <strong>Error:</strong> {error}
              </div>
            )}
          </div>
        </div>
      </div>
    </div>
  );
}
