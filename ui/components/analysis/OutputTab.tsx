import { Copy, Download } from "lucide-react";
import { Button } from "@/components/ui/button";
import { SQLEditor } from "@/components/SQLEditor";
import { ExecutableStatus } from "./ExecutableStatus";
import { type AnalyzeResponse } from "@/lib/types";

interface OutputTabProps {
  result: AnalyzeResponse;
  onCopy: () => void;
  onDownload: () => void;
}

export function OutputTab({ result, onCopy, onDownload }: OutputTabProps) {
  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <ExecutableStatus executable={result.executable} />
        <div className="flex gap-2">
          <Button variant="outline" size="sm" onClick={onCopy}>
            <Copy className="h-4 w-4 mr-2" />
            Copy
          </Button>
          <Button variant="outline" size="sm" onClick={onDownload}>
            <Download className="h-4 w-4 mr-2" />
            Download
          </Button>
        </div>
      </div>
      <SQLEditor value={result.transpiled_query} onChange={() => {}} readOnly />
    </div>
  );
}
