import { Database, GitMerge, Layers, FunctionSquare, Code, FolderTree } from "lucide-react";
import { type QueryMetadata } from "@/lib/types";

interface MetadataDisplayProps {
  metadata: QueryMetadata;
}

export function MetadataDisplay({ metadata }: MetadataDisplayProps) {
  const hasTables = metadata.tables.length > 0;
  const hasJoins = metadata.joins.length > 0;
  const hasCtes = metadata.ctes.length > 0;
  const hasSubqueries = metadata.subqueries.length > 0;
  const hasUdfs = metadata.udfs.length > 0;
  const hasSchemas = metadata.schemas && metadata.schemas.length > 0;

  if (!hasTables && !hasJoins && !hasCtes && !hasSubqueries && !hasUdfs && !hasSchemas) {
    return null;
  }

  return (
    <div className="space-y-3">
      <h3 className="text-sm font-semibold text-foreground">Query Metadata</h3>

      {hasSchemas && (
        <div className="flex items-start gap-2">
          <FolderTree className="h-4 w-4 text-muted-foreground mt-0.5" />
          <div className="flex-1">
            <p className="text-xs text-muted-foreground mb-1">Schemas/Databases</p>
            <div className="flex flex-wrap gap-2">
              {metadata.schemas.map((schema) => (
                <span
                  key={schema}
                  className="inline-flex items-center rounded-md bg-muted px-2 py-1 text-xs font-medium text-foreground"
                >
                  {schema}
                </span>
              ))}
            </div>
          </div>
        </div>
      )}

      {hasTables && (
        <div className="flex items-start gap-2">
          <Database className="h-4 w-4 text-muted-foreground mt-0.5" />
          <div className="flex-1">
            <p className="text-xs text-muted-foreground mb-1">Tables</p>
            <div className="flex flex-wrap gap-2">
              {metadata.tables.map((table) => (
                <span
                  key={table}
                  className="inline-flex items-center rounded-md bg-muted px-2 py-1 text-xs font-medium text-foreground"
                >
                  {table}
                </span>
              ))}
            </div>
          </div>
        </div>
      )}

      {hasJoins && (
        <div className="flex items-start gap-2">
          <GitMerge className="h-4 w-4 text-muted-foreground mt-0.5" />
          <div className="flex-1">
            <p className="text-xs text-muted-foreground mb-1">Joins</p>
            <p className="text-xs text-foreground">{metadata.joins.length} join(s) detected</p>
          </div>
        </div>
      )}

      {hasCtes && (
        <div className="flex items-start gap-2">
          <Layers className="h-4 w-4 text-muted-foreground mt-0.5" />
          <div className="flex-1">
            <p className="text-xs text-muted-foreground mb-1">CTEs</p>
            <div className="flex flex-wrap gap-2">
              {metadata.ctes.map((cte) => (
                <span
                  key={cte}
                  className="inline-flex items-center rounded-md bg-muted px-2 py-1 text-xs font-medium text-foreground"
                >
                  {cte}
                </span>
              ))}
            </div>
          </div>
        </div>
      )}

      {hasSubqueries && (
        <div className="flex items-start gap-2">
          <Code className="h-4 w-4 text-muted-foreground mt-0.5" />
          <div className="flex-1">
            <p className="text-xs text-muted-foreground mb-1">Subqueries</p>
            <div className="flex flex-wrap gap-2">
              {metadata.subqueries.map((sq, idx) => (
                <span
                  key={sq || idx}
                  className="inline-flex items-center rounded-md bg-muted px-2 py-1 text-xs font-medium text-foreground"
                >
                  {sq || `Subquery ${idx + 1}`}
                </span>
              ))}
            </div>
          </div>
        </div>
      )}

      {hasUdfs && (
        <div className="flex items-start gap-2">
          <FunctionSquare className="h-4 w-4 text-muted-foreground mt-0.5" />
          <div className="flex-1">
            <p className="text-xs text-muted-foreground mb-1">User-Defined Functions</p>
            <div className="flex flex-wrap gap-2">
              {metadata.udfs.map((udf) => (
                <span
                  key={udf}
                  className="inline-flex items-center rounded-md bg-muted px-2 py-1 text-xs font-medium text-foreground"
                >
                  {udf}
                </span>
              ))}
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
