import { type TimingInfo } from "@/lib/types";
import { Clock } from "lucide-react";

interface TimingDisplayProps {
  timing?: TimingInfo;
}

export function TimingDisplay({ timing }: TimingDisplayProps) {
  if (!timing) return null;

  const phases = [
    { label: "Parsing", value: timing.parsing_ms },
    { label: "Function Analysis", value: timing.function_analysis_ms },
    { label: "Metadata Extraction", value: timing.metadata_extraction_ms },
    { label: "Transpilation", value: timing.transpilation_ms },
    { label: "Post-Analysis", value: timing.post_analysis_ms },
  ].filter((phase) => phase.value !== undefined);

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

        {/* Phase Breakdown */}
        <div className="space-y-2">
          {phases.map((phase) => (
            <div
              key={phase.label}
              className="flex items-center justify-between py-2 border-b border-border last:border-0"
            >
              <span className="text-xs text-muted-foreground">{phase.label}</span>
              <span className="text-xs font-medium">
                {phase.value?.toFixed(2)}ms
              </span>
            </div>
          ))}
        </div>

        {/* Visual Bar Chart */}
        <div className="space-y-1 pt-2">
          {phases.map((phase) => {
            const percentage = ((phase.value || 0) / timing.total_ms) * 100;
            return (
              <div key={`bar-${phase.label}`} className="space-y-1">
                <div className="flex items-center justify-between text-xs">
                  <span className="text-muted-foreground">{phase.label}</span>
                  <span className="text-muted-foreground">
                    {percentage.toFixed(1)}%
                  </span>
                </div>
                <div className="h-2 bg-muted rounded-full overflow-hidden">
                  <div
                    className="h-full bg-primary rounded-full transition-all"
                    style={{ width: `${percentage}%` }}
                  />
                </div>
              </div>
            );
          })}
        </div>
      </div>
    </div>
  );
}
