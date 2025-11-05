import { FunctionsList } from "./FunctionsList";
import { MetadataDisplay } from "./MetadataDisplay";
import { TimingDisplay } from "./TimingDisplay";
import { type AnalyzeResponse } from "@/lib/types";
import { AlertCircle } from "lucide-react";

interface StatsTabProps {
  result: AnalyzeResponse;
}

export function StatsTab({ result }: StatsTabProps) {
  return (
    <div className="space-y-6 p-4 border border-border rounded-lg bg-background">
      <FunctionsList functions={result.functions} />
      <MetadataDisplay metadata={result.metadata} />

      {result.timing && (
        <>
          <div className="border-t border-border pt-6">
            <TimingDisplay timing={result.timing} />
          </div>

          <div className="flex items-start gap-2 p-3 bg-yellow-50 dark:bg-yellow-950 border border-yellow-200 dark:border-yellow-800 rounded-lg">
            <AlertCircle className="h-4 w-4 text-yellow-600 dark:text-yellow-400 mt-0.5 flex-shrink-0" />
            <p className="text-xs text-yellow-800 dark:text-yellow-200">
              <strong>Note:</strong> These timings are for the <code className="px-1 py-0.5 bg-yellow-100 dark:bg-yellow-900 rounded">/analyze</code> endpoint which includes detailed analysis.
              For production transpilation, use the <code className="px-1 py-0.5 bg-yellow-100 dark:bg-yellow-900 rounded">/transpile</code> endpoint which is much more lightweight and faster.
            </p>
          </div>
        </>
      )}
    </div>
  );
}
