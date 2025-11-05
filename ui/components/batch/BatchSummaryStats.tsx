import { CheckCircle2, XCircle, Database, Clock, ArrowRight } from "lucide-react";
import type { BatchSummary } from "@/lib/types";

interface BatchSummaryStatsProps {
  summary: BatchSummary;
}

export function BatchSummaryStats({ summary }: BatchSummaryStatsProps) {
  const { execution, functions, complexity, timing, dialects } = summary;

  return (
    <div className="space-y-6">
      {/* Dialects */}
      <div className="flex items-center gap-2 text-sm text-muted-foreground">
        <span className="font-semibold text-foreground">{dialects.source_dialect}</span>
        <ArrowRight className="h-4 w-4" />
        <span className="font-semibold text-foreground">{dialects.target_dialect}</span>
      </div>

      {/* Execution Summary */}
      <div className="border border-border rounded-lg p-4 space-y-4">
        <h3 className="text-sm font-semibold flex items-center gap-2">
          <CheckCircle2 className="h-4 w-4" />
          Execution Summary
        </h3>

        <div className="grid grid-cols-2 md:grid-cols-3 gap-4">
          <StatCard
            label="Total Queries"
            value={execution.total_queries}
            color="text-foreground"
          />
          <StatCard
            label="Succeeded"
            value={execution.succeeded}
            color="text-green-600"
          />
          <StatCard
            label="Failed"
            value={execution.failed}
            color={execution.failed > 0 ? "text-red-600" : "text-muted-foreground"}
          />
          <StatCard
            label="Executable"
            value={execution.executable_queries}
            color="text-green-600"
          />
          <StatCard
            label="Non-Executable"
            value={execution.non_executable_queries}
            color={execution.non_executable_queries > 0 ? "text-orange-600" : "text-muted-foreground"}
          />
          <StatCard
            label="Success Rate"
            value={`${execution.success_rate_percentage.toFixed(1)}%`}
            color="text-foreground"
          />
        </div>
      </div>

      {/* Functions Summary */}
      <div className="border border-border rounded-lg p-4 space-y-4">
        <h3 className="text-sm font-semibold flex items-center gap-2">
          <Database className="h-4 w-4" />
          Functions Summary
        </h3>

        <div className="grid grid-cols-3 gap-4">
          <StatCard
            label="Supported Functions"
            value={functions.unique_supported_count}
            color="text-green-600"
          />
          <StatCard
            label="Unsupported Functions"
            value={functions.unique_unsupported_count}
            color={functions.unique_unsupported_count > 0 ? "text-red-600" : "text-muted-foreground"}
          />
          <StatCard
            label="User-Defined Functions"
            value={functions.total_udfs}
            color="text-blue-600"
          />
        </div>
      </div>

      {/* Complexity Summary */}
      <div className="border border-border rounded-lg p-4 space-y-4">
        <h3 className="text-sm font-semibold flex items-center gap-2">
          <Database className="h-4 w-4" />
          Complexity Metrics
        </h3>

        <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
          <StatCard
            label="Total Tables"
            value={complexity.total_unique_tables}
          />
          <StatCard
            label="Total Schemas"
            value={complexity.total_unique_schemas}
          />
          <StatCard
            label="Total Joins"
            value={complexity.total_joins}
          />
          <StatCard
            label="Total CTEs"
            value={complexity.total_ctes}
          />
        </div>

        <div className="grid grid-cols-2 md:grid-cols-3 gap-4 pt-2 border-t border-border">
          <StatCard
            label="Avg Tables/Query"
            value={complexity.avg_tables_per_query.toFixed(2)}
            color="text-muted-foreground"
            isMetric
          />
          <StatCard
            label="Avg Functions/Query"
            value={complexity.avg_functions_per_query.toFixed(2)}
            color="text-muted-foreground"
            isMetric
          />
          <StatCard
            label="Total Subqueries"
            value={complexity.total_subqueries}
            color="text-muted-foreground"
          />
        </div>
      </div>

      {/* Timing Summary */}
      <div className="border border-border rounded-lg p-4 space-y-4">
        <h3 className="text-sm font-semibold flex items-center gap-2">
          <Clock className="h-4 w-4" />
          Timing Statistics
        </h3>

        <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
          <StatCard
            label="Total Duration"
            value={`${timing.total_duration_ms.toFixed(2)}ms`}
            color="text-foreground"
          />
          <StatCard
            label="Avg Query Time"
            value={`${timing.avg_query_duration_ms.toFixed(2)}ms`}
          />
          <StatCard
            label="Min Query Time"
            value={`${timing.min_query_duration_ms.toFixed(2)}ms`}
            color="text-green-600"
          />
          <StatCard
            label="Max Query Time"
            value={`${timing.max_query_duration_ms.toFixed(2)}ms`}
            color="text-orange-600"
          />
        </div>

        {(timing.avg_parsing_ms || timing.avg_transpilation_ms || timing.avg_function_analysis_ms) && (
          <div className="grid grid-cols-3 gap-4 pt-2 border-t border-border">
            {timing.avg_parsing_ms && (
              <StatCard
                label="Avg Parsing"
                value={`${timing.avg_parsing_ms.toFixed(2)}ms`}
                color="text-muted-foreground"
                isMetric
              />
            )}
            {timing.avg_transpilation_ms && (
              <StatCard
                label="Avg Transpilation"
                value={`${timing.avg_transpilation_ms.toFixed(2)}ms`}
                color="text-muted-foreground"
                isMetric
              />
            )}
            {timing.avg_function_analysis_ms && (
              <StatCard
                label="Avg Function Analysis"
                value={`${timing.avg_function_analysis_ms.toFixed(2)}ms`}
                color="text-muted-foreground"
                isMetric
              />
            )}
          </div>
        )}
      </div>
    </div>
  );
}

interface StatCardProps {
  label: string;
  value: string | number;
  color?: string;
  isMetric?: boolean;
}

function StatCard({ label, value, color = "text-foreground", isMetric = false }: StatCardProps) {
  return (
    <div className="space-y-1">
      <div className="text-xs text-muted-foreground">{label}</div>
      <div className={`text-2xl font-semibold ${color}`}>
        {value}
      </div>
    </div>
  );
}
