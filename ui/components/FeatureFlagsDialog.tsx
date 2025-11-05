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
          Feature Flags
        </Button>
      </DialogTrigger>
      <DialogContent>
        <DialogHeader>
          <DialogTitle>Feature Flags</DialogTitle>
        </DialogHeader>
        <div className="space-y-4">
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
          <div className="flex items-center justify-between">
            <label className="text-sm font-medium">Pretty Print</label>
            <Switch
              checked={flags.PRETTY_PRINT !== false}
              onCheckedChange={(checked) => updateFlag("PRETTY_PRINT", checked)}
            />
          </div>
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
          <div className="flex items-center justify-between">
            <label className="text-sm font-medium">Skip E6 Transpilation</label>
            <Switch
              checked={flags.SKIP_E6_TRANSPILATION || false}
              onCheckedChange={(checked) =>
                updateFlag("SKIP_E6_TRANSPILATION", checked)
              }
            />
          </div>
        </div>
      </DialogContent>
    </Dialog>
  );
}
