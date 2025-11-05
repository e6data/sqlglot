"use client";

import { Settings } from "lucide-react";
import { Button } from "@/components/ui/button";
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogTrigger,
} from "@/components/ui/dialog";
import { Switch } from "@/components/ui/switch";
import { type FeatureFlags } from "@/lib/types";

interface FeatureFlagsDialogProps {
  flags: FeatureFlags;
  onChange: (flags: FeatureFlags) => void;
}

export function FeatureFlagsDialog({ flags, onChange }: FeatureFlagsDialogProps) {
  const updateFlag = (key: keyof FeatureFlags, value: boolean) => {
    onChange({ ...flags, [key]: value });
  };

  return (
    <Dialog>
      <DialogTrigger asChild>
        <Button variant="outline" size="sm">
          <Settings className="h-4 w-4 mr-2" />
          Options
        </Button>
      </DialogTrigger>
      <DialogContent>
        <DialogHeader>
          <DialogTitle>Options</DialogTitle>
        </DialogHeader>
        <div className="space-y-6">
          <div className="space-y-2">
            <div className="flex items-center justify-between">
              <label className="text-sm font-medium">
                Enable Table Alias Qualification
              </label>
              <Switch
                checked={flags.ENABLE_TABLE_ALIAS_QUALIFICATION || false}
                onCheckedChange={(checked) =>
                  updateFlag("ENABLE_TABLE_ALIAS_QUALIFICATION", checked)
                }
              />
            </div>
            <p className="text-xs text-muted-foreground">
              Automatically adds table aliases to column references for clearer SQL (e.g., users.id instead of id)
            </p>
          </div>
          <div className="space-y-2">
            <div className="flex items-center justify-between">
              <label className="text-sm font-medium">Pretty Print</label>
              <Switch
                checked={flags.PRETTY_PRINT !== false}
                onCheckedChange={(checked) => updateFlag("PRETTY_PRINT", checked)}
              />
            </div>
            <p className="text-xs text-muted-foreground">
              Formats the output SQL with proper indentation and line breaks for better readability
            </p>
          </div>
          <div className="space-y-2">
            <div className="flex items-center justify-between">
              <label className="text-sm font-medium">
                Two Phase Qualification Scheme
              </label>
              <Switch
                checked={flags.USE_TWO_PHASE_QUALIFICATION_SCHEME || false}
                onCheckedChange={(checked) =>
                  updateFlag("USE_TWO_PHASE_QUALIFICATION_SCHEME", checked)
                }
              />
            </div>
            <p className="text-xs text-muted-foreground">
              Uses advanced column resolution to handle complex queries with multiple joins and subqueries
            </p>
          </div>
          <div className="space-y-2">
            <div className="flex items-center justify-between">
              <label className="text-sm font-medium">Skip E6 Transpilation</label>
              <Switch
                checked={flags.SKIP_E6_TRANSPILATION || false}
                onCheckedChange={(checked) =>
                  updateFlag("SKIP_E6_TRANSPILATION", checked)
                }
              />
            </div>
            <p className="text-xs text-muted-foreground">
              Only parse and validate the SQL without converting to E6 dialect
            </p>
          </div>
        </div>
      </DialogContent>
    </Dialog>
  );
}
