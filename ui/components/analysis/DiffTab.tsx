"use client";

import { parseDiff, Diff, Hunk } from "react-diff-view";
import { diffLines, formatLines } from "unidiff";
import "react-diff-view/style/index.css";

interface DiffTabProps {
  sourceQuery: string;
  transpiledQuery: string;
  sourceDialect: string;
  targetDialect: string;
}

export function DiffTab({ sourceQuery, transpiledQuery, sourceDialect, targetDialect }: DiffTabProps) {
  const diffText = formatLines(diffLines(sourceQuery, transpiledQuery), { context: 3 });
  const [diff] = parseDiff(diffText, { nearbySequences: "zip" });

  return (
    <div className="border border-border rounded-lg overflow-hidden">
      <div className="flex items-center justify-between p-3 bg-muted border-b border-border">
        <span className="text-sm font-medium text-foreground">
          {sourceDialect.toUpperCase()} → E6
        </span>
      </div>
      <div className="overflow-auto max-h-[600px]">
        <Diff
          viewType="split"
          diffType={diff.type}
          hunks={diff.hunks || []}
        >
          {(hunks) =>
            hunks.map((hunk) => (
              <Hunk key={hunk.content} hunk={hunk} />
            ))
          }
        </Diff>
      </div>
    </div>
  );
}
