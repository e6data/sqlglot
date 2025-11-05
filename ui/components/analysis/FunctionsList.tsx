import { type FunctionAnalysis } from "@/lib/types";

interface FunctionsListProps {
  functions: FunctionAnalysis;
}

export function FunctionsList({ functions }: FunctionsListProps) {
  const hasSupported = functions.supported.length > 0;
  const hasUnsupported = functions.unsupported.length > 0;

  if (!hasSupported && !hasUnsupported) {
    return null;
  }

  return (
    <div className="space-y-3">
      <h3 className="text-sm font-semibold text-foreground">Functions</h3>

      {hasSupported && (
        <div>
          <p className="text-xs text-muted-foreground mb-2">Supported</p>
          <div className="flex flex-wrap gap-2">
            {functions.supported.map((fn) => (
              <span
                key={fn}
                className="inline-flex items-center rounded-md bg-green-50 px-2 py-1 text-xs font-medium text-green-700 ring-1 ring-inset ring-green-600/20"
              >
                {fn}
              </span>
            ))}
          </div>
        </div>
      )}

      {hasUnsupported && (
        <div>
          <p className="text-xs text-muted-foreground mb-2">Unsupported</p>
          <div className="flex flex-wrap gap-2">
            {functions.unsupported.map((fn) => (
              <span
                key={fn}
                className="inline-flex items-center rounded-md bg-red-50 px-2 py-1 text-xs font-medium text-red-700 ring-1 ring-inset ring-red-600/20"
              >
                {fn}
              </span>
            ))}
          </div>
        </div>
      )}
    </div>
  );
}
