"use client";

import { useState } from "react";
import ReactJson from "@microlink/react-json-view";
import { Button } from "@/components/ui/button";
import { Copy, Download, ChevronDown, ChevronUp } from "lucide-react";

interface ASTTabProps {
  sourceAst?: any;
  transpiledAst?: any;
  sourceDialect: string;
  targetDialect: string;
}

export function ASTTab({ sourceAst, transpiledAst, sourceDialect, targetDialect }: ASTTabProps) {
  const [sourceCollapsed, setSourceCollapsed] = useState<number | boolean>(2);
  const [transpiledCollapsed, setTranspiledCollapsed] = useState<number | boolean>(2);

  const handleCopy = (ast: any) => {
    navigator.clipboard.writeText(JSON.stringify(ast, null, 2));
  };

  const handleDownload = (ast: any, filename: string) => {
    const blob = new Blob([JSON.stringify(ast, null, 2)], { type: "application/json" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = filename;
    a.click();
    URL.revokeObjectURL(url);
  };

  if (!sourceAst && !transpiledAst) {
    return (
      <div className="p-8 text-center text-muted-foreground">
        No AST data available
      </div>
    );
  }

  return (
    <div className="grid grid-cols-2 gap-4">
      {/* Source AST */}
      <div className="space-y-4">
        <div className="flex items-center justify-between">
          <h3 className="text-sm font-medium">Source AST ({sourceDialect.toUpperCase()})</h3>
          <div className="flex gap-2">
            <Button
              variant="outline"
              size="sm"
              onClick={() => setSourceCollapsed(true)}
              disabled={!sourceAst}
            >
              <ChevronUp className="h-4 w-4" />
            </Button>
            <Button
              variant="outline"
              size="sm"
              onClick={() => setSourceCollapsed(false)}
              disabled={!sourceAst}
            >
              <ChevronDown className="h-4 w-4" />
            </Button>
            <Button
              variant="outline"
              size="sm"
              onClick={() => handleCopy(sourceAst)}
              disabled={!sourceAst}
            >
              <Copy className="h-4 w-4" />
            </Button>
            <Button
              variant="outline"
              size="sm"
              onClick={() => handleDownload(sourceAst, `source_ast_${sourceDialect}.json`)}
              disabled={!sourceAst}
            >
              <Download className="h-4 w-4" />
            </Button>
          </div>
        </div>
        <div className="border border-border rounded-lg overflow-hidden">
          <div className="p-4 bg-background overflow-auto max-h-[600px]">
            {sourceAst ? (
              <ReactJson
                src={sourceAst}
                theme="rjv-default"
                collapsed={sourceCollapsed}
                displayDataTypes={false}
                displayObjectSize={true}
                enableClipboard={true}
                name={false}
                style={{
                  backgroundColor: "transparent",
                  fontSize: "13px",
                }}
              />
            ) : (
              <div className="text-center text-muted-foreground py-8">
                No source AST available
              </div>
            )}
          </div>
        </div>
      </div>

      {/* Transpiled AST */}
      <div className="space-y-4">
        <div className="flex items-center justify-between">
          <h3 className="text-sm font-medium">Transpiled AST ({targetDialect.toUpperCase()})</h3>
          <div className="flex gap-2">
            <Button
              variant="outline"
              size="sm"
              onClick={() => setTranspiledCollapsed(true)}
              disabled={!transpiledAst}
            >
              <ChevronUp className="h-4 w-4" />
            </Button>
            <Button
              variant="outline"
              size="sm"
              onClick={() => setTranspiledCollapsed(false)}
              disabled={!transpiledAst}
            >
              <ChevronDown className="h-4 w-4" />
            </Button>
            <Button
              variant="outline"
              size="sm"
              onClick={() => handleCopy(transpiledAst)}
              disabled={!transpiledAst}
            >
              <Copy className="h-4 w-4" />
            </Button>
            <Button
              variant="outline"
              size="sm"
              onClick={() => handleDownload(transpiledAst, `transpiled_ast_${targetDialect}.json`)}
              disabled={!transpiledAst}
            >
              <Download className="h-4 w-4" />
            </Button>
          </div>
        </div>
        <div className="border border-border rounded-lg overflow-hidden">
          <div className="p-4 bg-background overflow-auto max-h-[600px]">
            {transpiledAst ? (
              <ReactJson
                src={transpiledAst}
                theme="rjv-default"
                collapsed={transpiledCollapsed}
                displayDataTypes={false}
                displayObjectSize={true}
                enableClipboard={true}
                name={false}
                style={{
                  backgroundColor: "transparent",
                  fontSize: "13px",
                }}
              />
            ) : (
              <div className="text-center text-muted-foreground py-8">
                No transpiled AST available
              </div>
            )}
          </div>
        </div>
      </div>
    </div>
  );
}
