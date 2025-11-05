"use client";

import CodeMirror from "@uiw/react-codemirror";
import { sql } from "@codemirror/lang-sql";

interface SQLEditorProps {
  value: string;
  onChange: (value: string) => void;
  readOnly?: boolean;
}

export function SQLEditor({ value, onChange, readOnly = false }: SQLEditorProps) {
  return (
    <CodeMirror
      value={value}
      height="500px"
      extensions={[sql()]}
      onChange={onChange}
      readOnly={readOnly}
      basicSetup={{
        lineNumbers: true,
        foldGutter: true,
        highlightActiveLineGutter: true,
      }}
      className="border border-border"
    />
  );
}
