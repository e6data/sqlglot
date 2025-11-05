import { FunctionsList } from "./FunctionsList";
import { MetadataDisplay } from "./MetadataDisplay";
import { type AnalyzeResponse } from "@/lib/types";

interface StatsTabProps {
  result: AnalyzeResponse;
}

export function StatsTab({ result }: StatsTabProps) {
  return (
    <div className="space-y-6 p-4 border border-border rounded-lg bg-background">
      <FunctionsList functions={result.functions} />
      <MetadataDisplay metadata={result.metadata} />
    </div>
  );
}
