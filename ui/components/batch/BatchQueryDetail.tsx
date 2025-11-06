import { Dialog, DialogContent, DialogHeader, DialogTitle } from "@/components/ui/dialog";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Button } from "@/components/ui/button";
import { Copy, Download } from "lucide-react";
import { ExecutableStatus } from "@/components/analysis/ExecutableStatus";
import { FunctionsList } from "@/components/analysis/FunctionsList";
import { MetadataDisplay } from "@/components/analysis/MetadataDisplay";
import type { BatchAnalysisResultItem } from "@/lib/types";

interface BatchQueryDetailProps {
  result: BatchAnalysisResultItem | null;
  onClose: () => void;
}

export function BatchQueryDetail({ result, onClose }: BatchQueryDetailProps) {
  if (!result) return null;

  const copyToClipboard = (text: string) => {
    navigator.clipboard.writeText(text);
  };

  const downloadSQL = () => {
    if (!result.transpiled_query) return;

    const blob = new Blob([result.transpiled_query], { type: "text/plain" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = `${result.id}_transpiled.sql`;
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(url);
  };

  return (
    <Dialog open={!!result} onOpenChange={(open) => !open && onClose()}>
      <DialogContent className="max-w-5xl max-h-[90vh] overflow-y-auto">
        <DialogHeader>
          <DialogTitle className="flex items-center justify-between">
            <span>Query Details: {result.id}</span>
            <ExecutableStatus executable={result.executable ?? false} />
          </DialogTitle>
        </DialogHeader>

        <Tabs defaultValue="output" className="w-full">
          <TabsList className="grid w-full grid-cols-3">
            <TabsTrigger value="output">Output</TabsTrigger>
            <TabsTrigger value="stats">Stats</TabsTrigger>
            <TabsTrigger value="metadata">Metadata</TabsTrigger>
          </TabsList>

          {/* Output Tab */}
          <TabsContent value="output" className="space-y-4">
            <div className="flex items-center justify-between">
              <h3 className="text-sm font-semibold">Transpiled SQL</h3>
              <div className="flex gap-2">
                <Button
                  onClick={() => result.transpiled_query && copyToClipboard(result.transpiled_query)}
                  variant="outline"
                  size="sm"
                  className="flex items-center gap-2"
                >
                  <Copy className="h-3 w-3" />
                  Copy
                </Button>
                <Button
                  onClick={downloadSQL}
                  variant="outline"
                  size="sm"
                  className="flex items-center gap-2"
                >
                  <Download className="h-3 w-3" />
                  Download
                </Button>
              </div>
            </div>

            <pre className="p-4 bg-muted border border-border rounded-lg text-sm font-mono whitespace-pre-wrap overflow-x-auto">
              {result.transpiled_query || "No transpiled query available"}
            </pre>
          </TabsContent>

          {/* Stats Tab */}
          <TabsContent value="stats" className="space-y-4">
            <div>
              <h3 className="text-sm font-semibold mb-3">Function Analysis</h3>
              {result.functions ? (
                <FunctionsList
                  functions={result.functions}
                />
              ) : (
                <p className="text-sm text-muted-foreground">No function data available</p>
              )}
            </div>
          </TabsContent>

          {/* Metadata Tab */}
          <TabsContent value="metadata" className="space-y-4">
            {result.metadata ? (
              <MetadataDisplay metadata={result.metadata} />
            ) : (
              <p className="text-sm text-muted-foreground">No metadata available</p>
            )}
          </TabsContent>
        </Tabs>
      </DialogContent>
    </Dialog>
  );
}
