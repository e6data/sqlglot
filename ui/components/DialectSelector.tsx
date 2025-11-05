"use client";

import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { DIALECTS, type Dialect } from "@/lib/types";

interface DialectSelectorProps {
  value: Dialect;
  onChange: (value: Dialect) => void;
}

export function DialectSelector({ value, onChange }: DialectSelectorProps) {
  return (
    <Select value={value} onValueChange={onChange}>
      <SelectTrigger className="w-[200px]">
        <SelectValue placeholder="Select dialect" />
      </SelectTrigger>
      <SelectContent>
        {DIALECTS.map((dialect) => (
          <SelectItem key={dialect} value={dialect}>
            {dialect.charAt(0).toUpperCase() + dialect.slice(1)}
          </SelectItem>
        ))}
      </SelectContent>
    </Select>
  );
}
