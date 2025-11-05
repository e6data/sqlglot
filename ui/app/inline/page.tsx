"use client";

import { useState } from "react";
import { Loader2 } from "lucide-react";
import { SQLEditor } from "@/components/SQLEditor";
import { DialectSelector } from "@/components/DialectSelector";
import { FeatureFlagsDialog } from "@/components/FeatureFlagsDialog";
import { Button } from "@/components/ui/button";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { useConverter } from "@/hooks/useConverter";
import { OutputTab } from "@/components/analysis/OutputTab";
import { StatsTab } from "@/components/analysis/StatsTab";
import { DiffTab } from "@/components/analysis/DiffTab";
import { ASTTab } from "@/components/analysis/ASTTab";
import { type Dialect, type FeatureFlags } from "@/lib/types";

export default function InlineMode() {
  const [sourceQuery, setSourceQuery] = useState("");
  const [dialect, setDialect] = useState<Dialect>("snowflake");
  const [targetDialect, setTargetDialect] = useState<Dialect>("e6");
  const [flags, setFlags] = useState<FeatureFlags>({ PRETTY_PRINT: true });
  const { convert, loading, error, result } = useConverter();

  const handleConvert = () => {
    if (!sourceQuery.trim()) return;
    convert({ query: sourceQuery, fromDialect: dialect, targetDialect, featureFlags: flags });
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
        </div>
      </header>

      <div className="flex-1 p-6 overflow-auto">
        <div className="max-w-7xl mx-auto space-y-6">
          {/* Source SQL Section */}
          <div className="space-y-4">
            <div className="flex items-center justify-between">
              <h2 className="text-lg font-semibold">Source SQL</h2>
              <div className="flex items-center gap-4">
                <FeatureFlagsDialog flags={flags} onChange={setFlags} />
                <div className="flex items-center gap-2">
                  <span className="text-sm text-muted-foreground">From:</span>
                  <DialectSelector value={dialect} onChange={setDialect} />
                </div>
                <div className="flex items-center gap-2">
                  <span className="text-sm text-muted-foreground">To:</span>
                  <DialectSelector value={targetDialect} onChange={setTargetDialect} />
                </div>
              </div>
            </div>
            <SQLEditor value={sourceQuery} onChange={setSourceQuery} />
            <Button
              onClick={handleConvert}
              disabled={loading || !sourceQuery.trim()}
              className="w-full"
              size="lg"
            >
              {loading ? (
                <>
                  <Loader2 className="mr-2 h-4 w-4 animate-spin" />
                  Converting...
                </>
              ) : (
                "Convert"
              )}
            </Button>
          </div>

          {/* Error Display */}
          {error && (
            <div className="p-4 border border-red-500 bg-red-50 text-red-700 text-sm rounded-lg">
              <strong>Error:</strong> {error}
            </div>
          )}

          {/* Results Tabs */}
          {result && (
            <div className="space-y-4">
              <h2 className="text-lg font-semibold">Results</h2>
              <Tabs defaultValue="output" className="w-full">
                <TabsList>
                  <TabsTrigger value="output">Output</TabsTrigger>
                  <TabsTrigger value="stats">Stats</TabsTrigger>
                  <TabsTrigger value="diff">Diff</TabsTrigger>
                  <TabsTrigger value="ast">AST</TabsTrigger>
                </TabsList>
                <TabsContent value="output" className="space-y-4">
                  <OutputTab
                    result={result}
                    onCopy={handleCopy}
                    onDownload={handleDownload}
                  />
                </TabsContent>
                <TabsContent value="stats">
                  <StatsTab result={result} />
                </TabsContent>
                <TabsContent value="diff">
                  <DiffTab
                    sourceQuery={sourceQuery}
                    transpiledQuery={result.transpiled_query}
                    sourceDialect={dialect}
                    targetDialect={targetDialect}
                  />
                </TabsContent>
                <TabsContent value="ast">
                  <ASTTab
                    sourceAst={result.source_ast}
                    transpiledAst={result.transpiled_ast}
                    sourceDialect={dialect}
                    targetDialect={targetDialect}
                  />
                </TabsContent>
              </Tabs>
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
