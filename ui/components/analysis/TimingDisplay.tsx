import { type TimingInfo } from "@/lib/types";
import { Clock } from "lucide-react";

interface TimingDisplayProps {
  timing?: TimingInfo;
}

interface PhaseGroup {
  name: string;
  total?: number;
  subPhases: { label: string; value?: number }[];
}

export function TimingDisplay({ timing }: TimingDisplayProps) {
  if (!timing) return null;

  const phaseGroups: PhaseGroup[] = [
    {
      name: "Preprocessing",
      total: (timing.normalization_ms || 0) + (timing.config_loading_ms || 0),
      subPhases: [
        { label: "Normalization", value: timing.normalization_ms },
        { label: "Config Loading", value: timing.config_loading_ms },
      ],
    },
    {
      name: "Parsing",
      total: timing.parsing_ms,
      subPhases: [{ label: "Parsing", value: timing.parsing_ms }],
    },
    {
      name: "Function Analysis",
      total: timing.function_analysis_ms,
      subPhases: [
        { label: "Function Extraction", value: timing.function_extraction_ms },
        { label: "Function Categorization", value: timing.function_categorization_ms },
        { label: "UDF Extraction", value: timing.udf_extraction_ms },
        { label: "Unsupported Detection", value: timing.unsupported_detection_ms },
      ],
    },
    {
      name: "Metadata Extraction",
      total: timing.metadata_extraction_ms,
      subPhases: [
        { label: "Table Extraction", value: timing.table_extraction_ms },
        { label: "Join Extraction", value: timing.join_extraction_ms },
        { label: "CTE Extraction", value: timing.cte_extraction_ms },
        { label: "Schema Extraction", value: timing.schema_extraction_ms },
      ],
    },
    {
      name: "Transpilation",
      total: timing.transpilation_ms,
      subPhases: [
        { label: "AST Preprocessing", value: timing.ast_preprocessing_ms },
        { label: "Transpilation Parsing", value: timing.transpilation_parsing_ms },
        { label: "Identifier Qualification", value: timing.identifier_qualification_ms },
        { label: "SQL Generation", value: timing.sql_generation_ms },
        { label: "Post-Processing", value: timing.post_processing_ms },
      ],
    },
    {
      name: "Post-Analysis",
      total: timing.post_analysis_ms,
      subPhases: [
        { label: "Transpiled Parsing", value: timing.transpiled_parsing_ms },
        { label: "Transpiled Function Extraction", value: timing.transpiled_function_extraction_ms },
        { label: "Transpiled Function Analysis", value: timing.transpiled_function_analysis_ms },
      ],
    },
    {
      name: "AST Serialization",
      total: timing.ast_serialization_ms,
      subPhases: [{ label: "AST Serialization", value: timing.ast_serialization_ms }],
    },
  ].filter((group) => group.total !== undefined && group.total > 0);

  return (
    <div className="space-y-4">
      <div className="flex items-center gap-2">
        <Clock className="h-4 w-4 text-muted-foreground" />
        <h3 className="text-sm font-semibold">Performance Timing</h3>
      </div>

      <div className="space-y-3">
        {/* Total Time */}
        <div className="p-3 bg-muted rounded-lg">
          <div className="flex items-center justify-between">
            <span className="text-sm font-medium text-foreground">Total Time</span>
            <span className="text-sm font-bold text-foreground">
              {timing.total_ms.toFixed(2)}ms
            </span>
          </div>
        </div>

        {/* Detailed Phase Breakdown */}
        <div className="space-y-3">
          {phaseGroups.map((group) => {
            const percentage = ((group.total || 0) / timing.total_ms) * 100;
            const hasSubPhases = group.subPhases.filter(sp => sp.value !== undefined).length > 1;

            return (
              <div key={group.name} className="space-y-1">
                {/* Group Header */}
                <div className="flex items-center justify-between py-1.5">
                  <span className="text-sm text-muted-foreground">{group.name}</span>
                  <span className="text-sm font-medium">
                    {group.total?.toFixed(2)}ms
                  </span>
                </div>

                {/* Sub-phases (if more than one) */}
                {hasSubPhases && (
                  <div className="ml-4 space-y-1 mb-2">
                    {group.subPhases
                      .filter((sp) => sp.value !== undefined)
                      .map((subPhase) => (
                        <div
                          key={subPhase.label}
                          className="flex items-center justify-between text-xs"
                        >
                          <span className="text-muted-foreground">
                            {subPhase.label}
                          </span>
                          <span className="text-muted-foreground">
                            {subPhase.value?.toFixed(2)}ms
                          </span>
                        </div>
                      ))}
                  </div>
                )}

                {/* Progress Bar */}
                <div className="h-2 bg-muted rounded-full overflow-hidden">
                  <div
                    className="h-full bg-primary rounded-full transition-all"
                    style={{ width: `${percentage}%` }}
                  />
                </div>
                <div className="flex justify-end">
                  <span className="text-xs text-muted-foreground">
                    {percentage.toFixed(1)}%
                  </span>
                </div>
              </div>
            );
          })}
        </div>

        {/* Note */}
        <div className="mt-4 p-2 bg-orange-950/20 border border-orange-900/30 rounded text-xs text-orange-200">
          <span className="font-semibold">Note:</span> These timings are for the{" "}
          <code className="px-1 py-0.5 bg-orange-900/30 rounded">/analyze</code>{" "}
          endpoint which includes detailed analysis. For production transpilation, use the{" "}
          <code className="px-1 py-0.5 bg-orange-900/30 rounded">/transpile</code>{" "}
          endpoint which is much more lightweight and faster.
        </div>
      </div>
    </div>
  );
}
