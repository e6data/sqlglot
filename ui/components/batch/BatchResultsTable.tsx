import { useState } from "react";
import { CheckCircle2, XCircle, Copy, ChevronDown, ChevronRight } from "lucide-react";
import { Button } from "@/components/ui/button";
import type { BatchAnalysisResultItem } from "@/lib/types";

interface BatchResultsTableProps {
  results: BatchAnalysisResultItem[];
  onViewDetails: (result: BatchAnalysisResultItem) => void;
}

export function BatchResultsTable({ results, onViewDetails }: BatchResultsTableProps) {
  const [expandedRows, setExpandedRows] = useState<Set<string>>(new Set());

  const toggleRow = (id: string) => {
    const newExpanded = new Set(expandedRows);
    if (newExpanded.has(id)) {
      newExpanded.delete(id);
    } else {
      newExpanded.add(id);
    }
    setExpandedRows(newExpanded);
  };

  const copyToClipboard = (text: string) => {
    navigator.clipboard.writeText(text);
  };

  return (
    <div className="border border-border rounded-lg overflow-hidden">
      <div className="overflow-x-auto">
        <table className="w-full">
          <thead className="bg-muted border-b border-border">
            <tr>
              <th className="text-left text-xs font-semibold text-muted-foreground p-3 w-8"></th>
              <th className="text-left text-xs font-semibold text-muted-foreground p-3">Query ID</th>
              <th className="text-left text-xs font-semibold text-muted-foreground p-3">Status</th>
              <th className="text-left text-xs font-semibold text-muted-foreground p-3">Query Preview</th>
              <th className="text-left text-xs font-semibold text-muted-foreground p-3">Functions</th>
              <th className="text-left text-xs font-semibold text-muted-foreground p-3">Executable</th>
              <th className="text-left text-xs font-semibold text-muted-foreground p-3">Actions</th>
            </tr>
          </thead>
          <tbody>
            {results.map((result) => {
              const isExpanded = expandedRows.has(result.id);
              const isSuccess = result.status === "success";

              return (
                <>
                  <tr
                    key={result.id}
                    className="border-b border-border hover:bg-muted/50 cursor-pointer"
                    onClick={() => toggleRow(result.id)}
                  >
                    <td className="p-3">
                      {isExpanded ? (
                        <ChevronDown className="h-4 w-4 text-muted-foreground" />
                      ) : (
                        <ChevronRight className="h-4 w-4 text-muted-foreground" />
                      )}
                    </td>
                    <td className="p-3 font-mono text-sm">{result.id}</td>
                    <td className="p-3">
                      {isSuccess ? (
                        <div className="flex items-center gap-1 text-green-600">
                          <CheckCircle2 className="h-4 w-4" />
                          <span className="text-xs">Success</span>
                        </div>
                      ) : (
                        <div className="flex items-center gap-1 text-red-600">
                          <XCircle className="h-4 w-4" />
                          <span className="text-xs">Error</span>
                        </div>
                      )}
                    </td>
                    <td className="p-3">
                      {isSuccess && result.transpiled_query ? (
                        <div className="font-mono text-xs text-muted-foreground truncate max-w-md">
                          {result.transpiled_query.substring(0, 60)}...
                        </div>
                      ) : result.error ? (
                        <div className="text-xs text-red-600 truncate max-w-md">
                          {result.error.message}
                        </div>
                      ) : (
                        <span className="text-xs text-muted-foreground">-</span>
                      )}
                    </td>
                    <td className="p-3">
                      {isSuccess && result.functions ? (
                        <div className="text-xs">
                          <span className="text-green-600">{result.functions.supported.length}</span>
                          {result.functions.unsupported.length > 0 && (
                            <span className="text-red-600"> / {result.functions.unsupported.length}</span>
                          )}
                        </div>
                      ) : (
                        <span className="text-xs text-muted-foreground">-</span>
                      )}
                    </td>
                    <td className="p-3">
                      {isSuccess ? (
                        result.executable ? (
                          <div className="flex items-center gap-1 text-green-600">
                            <CheckCircle2 className="h-3 w-3" />
                            <span className="text-xs">Yes</span>
                          </div>
                        ) : (
                          <div className="flex items-center gap-1 text-orange-600">
                            <XCircle className="h-3 w-3" />
                            <span className="text-xs">No</span>
                          </div>
                        )
                      ) : (
                        <span className="text-xs text-muted-foreground">-</span>
                      )}
                    </td>
                    <td className="p-3" onClick={(e) => e.stopPropagation()}>
                      {isSuccess && result.transpiled_query && (
                        <div className="flex items-center gap-2">
                          <Button
                            onClick={() => copyToClipboard(result.transpiled_query!)}
                            variant="ghost"
                            size="sm"
                            className="h-7 px-2"
                          >
                            <Copy className="h-3 w-3" />
                          </Button>
                          <Button
                            onClick={() => onViewDetails(result)}
                            variant="outline"
                            size="sm"
                            className="h-7 px-3 text-xs"
                          >
                            Details
                          </Button>
                        </div>
                      )}
                    </td>
                  </tr>

                  {isExpanded && (
                    <tr className="border-b border-border bg-muted/30">
                      <td colSpan={7} className="p-4">
                        {isSuccess ? (
                          <div className="space-y-4">
                            {result.transpiled_query && (
                              <div className="space-y-2">
                                <div className="text-xs font-semibold text-muted-foreground">
                                  Transpiled Query
                                </div>
                                <pre className="p-3 bg-background border border-border rounded text-xs font-mono whitespace-pre-wrap">
                                  {result.transpiled_query}
                                </pre>
                              </div>
                            )}

                            {result.metadata && (
                              <div className="grid grid-cols-2 md:grid-cols-3 gap-4 text-xs">
                                {result.metadata.tables.length > 0 && (
                                  <div>
                                    <div className="font-semibold text-muted-foreground mb-1">Tables</div>
                                    <div className="space-y-1">
                                      {result.metadata.tables.map((t, i) => (
                                        <div key={i} className="font-mono">{t}</div>
                                      ))}
                                    </div>
                                  </div>
                                )}

                                {result.metadata.ctes.length > 0 && (
                                  <div>
                                    <div className="font-semibold text-muted-foreground mb-1">CTEs</div>
                                    <div className="space-y-1">
                                      {result.metadata.ctes.map((c, i) => (
                                        <div key={i} className="font-mono">{c}</div>
                                      ))}
                                    </div>
                                  </div>
                                )}

                                {result.metadata.schemas.length > 0 && (
                                  <div>
                                    <div className="font-semibold text-muted-foreground mb-1">Schemas</div>
                                    <div className="space-y-1">
                                      {result.metadata.schemas.map((s, i) => (
                                        <div key={i} className="font-mono">{s}</div>
                                      ))}
                                    </div>
                                  </div>
                                )}
                              </div>
                            )}
                          </div>
                        ) : (
                          <div className="p-4 border border-red-500 bg-red-50 text-red-700 rounded text-sm">
                            <div className="font-semibold mb-1">Error</div>
                            <div>{result.error?.message}</div>
                            {result.error?.code && (
                              <div className="text-xs mt-2">Code: {result.error.code}</div>
                            )}
                          </div>
                        )}
                      </td>
                    </tr>
                  )}
                </>
              );
            })}
          </tbody>
        </table>
      </div>
    </div>
  );
}
