import { useState, useRef } from "react";
import { Upload, X, FileText } from "lucide-react";
import { Button } from "@/components/ui/button";
import type { BatchQueryItem } from "@/lib/types";

interface CSVUploadProps {
  onQueriesLoaded: (queries: BatchQueryItem[]) => void;
  onClear: () => void;
}

export function CSVUpload({ onQueriesLoaded, onClear }: CSVUploadProps) {
  const [queries, setQueries] = useState<BatchQueryItem[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [fileName, setFileName] = useState<string | null>(null);
  const fileInputRef = useRef<HTMLInputElement>(null);

  const parseCSV = (text: string): BatchQueryItem[] => {
    const lines = text.trim().split("\n");

    if (lines.length < 2) {
      throw new Error("CSV must have at least a header row and one data row");
    }

    // Parse header
    const header = lines[0].split(",").map(h => h.trim().toLowerCase());

    if (header.length !== 2) {
      throw new Error("CSV must have exactly 2 columns: query_id and query");
    }

    if (!header.includes("query_id") && !header.includes("queryid") && !header.includes("id")) {
      throw new Error("First column must be 'query_id', 'queryid', or 'id'");
    }

    if (!header.includes("query") && !header.includes("sql")) {
      throw new Error("Second column must be 'query' or 'sql'");
    }

    // Parse data rows
    const queries: BatchQueryItem[] = [];

    for (let i = 1; i < lines.length; i++) {
      const line = lines[i].trim();
      if (!line) continue; // Skip empty lines

      // Handle CSV with quoted values
      const match = line.match(/^([^,]+),(.+)$/);
      if (!match) {
        throw new Error(`Invalid CSV format at line ${i + 1}`);
      }

      const id = match[1].trim().replace(/^"|"$/g, "");
      let query = match[2].trim().replace(/^"|"$/g, "");

      // Unescape quotes
      query = query.replace(/""/g, '"');

      if (!id) {
        throw new Error(`Empty query_id at line ${i + 1}`);
      }

      if (!query) {
        throw new Error(`Empty query at line ${i + 1}`);
      }

      queries.push({ id, query });
    }

    if (queries.length === 0) {
      throw new Error("No valid queries found in CSV");
    }

    return queries;
  };

  const handleFileChange = (event: React.ChangeEvent<HTMLInputElement>) => {
    const file = event.target.files?.[0];
    if (!file) return;

    setError(null);
    setFileName(file.name);

    const reader = new FileReader();
    reader.onload = (e) => {
      try {
        const text = e.target?.result as string;
        const parsedQueries = parseCSV(text);
        setQueries(parsedQueries);
        onQueriesLoaded(parsedQueries);
      } catch (err) {
        const errorMessage = err instanceof Error ? err.message : "Failed to parse CSV";
        setError(errorMessage);
        setQueries([]);
        setFileName(null);
      }
    };

    reader.readAsText(file);
  };

  const handleClear = () => {
    setQueries([]);
    setError(null);
    setFileName(null);
    if (fileInputRef.current) {
      fileInputRef.current.value = "";
    }
    onClear();
  };

  const handleUploadClick = () => {
    fileInputRef.current?.click();
  };

  return (
    <div className="space-y-4">
      <div className="flex items-center gap-4">
        <input
          ref={fileInputRef}
          type="file"
          accept=".csv"
          onChange={handleFileChange}
          className="hidden"
        />

        {!fileName ? (
          <Button
            onClick={handleUploadClick}
            variant="outline"
            className="flex items-center gap-2"
          >
            <Upload className="h-4 w-4" />
            Upload CSV File
          </Button>
        ) : (
          <div className="flex items-center gap-2">
            <div className="flex items-center gap-2 px-4 py-2 bg-muted rounded-lg">
              <FileText className="h-4 w-4 text-muted-foreground" />
              <span className="text-sm font-medium">{fileName}</span>
              <span className="text-sm text-muted-foreground">
                ({queries.length} {queries.length === 1 ? "query" : "queries"})
              </span>
            </div>
            <Button
              onClick={handleClear}
              variant="ghost"
              size="sm"
              className="h-8 w-8 p-0"
            >
              <X className="h-4 w-4" />
            </Button>
          </div>
        )}
      </div>

      {error && (
        <div className="p-4 border border-red-500 bg-red-50 text-red-700 rounded-lg text-sm">
          {error}
        </div>
      )}

      {queries.length > 0 && !error && (
        <div className="border border-border rounded-lg p-4 space-y-3">
          <div className="flex items-center justify-between">
            <h4 className="text-sm font-semibold">Preview (first 5 queries)</h4>
            <span className="text-xs text-muted-foreground">
              Total: {queries.length} {queries.length === 1 ? "query" : "queries"}
            </span>
          </div>

          <div className="space-y-2">
            {queries.slice(0, 5).map((q, idx) => (
              <div key={idx} className="p-3 bg-muted rounded text-sm space-y-1">
                <div className="font-medium text-xs text-muted-foreground">
                  {q.id}
                </div>
                <div className="font-mono text-xs truncate">
                  {q.query}
                </div>
              </div>
            ))}

            {queries.length > 5 && (
              <div className="text-xs text-muted-foreground text-center py-2">
                ... and {queries.length - 5} more {queries.length - 5 === 1 ? "query" : "queries"}
              </div>
            )}
          </div>
        </div>
      )}

      <div className="text-xs text-muted-foreground space-y-1">
        <p><strong>CSV Format:</strong> Two columns with headers</p>
        <p className="font-mono bg-muted px-2 py-1 rounded">
          query_id,query<br />
          q1,"SELECT * FROM users"<br />
          q2,"SELECT COUNT(*) FROM orders"
        </p>
      </div>
    </div>
  );
}
