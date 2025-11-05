import { CheckCircle2, XCircle } from "lucide-react";

interface ExecutableStatusProps {
  executable: boolean;
}

export function ExecutableStatus({ executable }: ExecutableStatusProps) {
  return (
    <div className="flex items-center gap-2">
      {executable ? (
        <>
          <CheckCircle2 className="h-5 w-5 text-green-600" />
          <span className="text-sm font-medium text-green-600">Executable on E6</span>
        </>
      ) : (
        <>
          <XCircle className="h-5 w-5 text-red-600" />
          <span className="text-sm font-medium text-red-600">Not Executable</span>
        </>
      )}
    </div>
  );
}
