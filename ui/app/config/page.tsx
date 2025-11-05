"use client";

import { useState, useEffect } from "react";

interface ConfigField {
  name: string;
  value: any;
  description: string;
  type: string;
}

interface ConfigData {
  server: ConfigField[];
  api: ConfigField[];
  transpilation_defaults: ConfigField[];
}

const API_URL = process.env.NEXT_PUBLIC_API_URL || "http://localhost:8100";

export default function ConfigPage() {
  const [config, setConfig] = useState<ConfigData | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    const fetchConfig = async () => {
      try {
        const response = await fetch(`${API_URL}/api/v1/config`);
        if (!response.ok) {
          throw new Error(`Failed to fetch config: ${response.statusText}`);
        }
        const data = await response.json();
        setConfig(data);
      } catch (err) {
        setError(err instanceof Error ? err.message : "Failed to fetch config");
      } finally {
        setLoading(false);
      }
    };

    fetchConfig();
  }, []);

  const formatValue = (value: any, type: string): string => {
    if (type === "boolean") {
      return value ? "true" : "false";
    }
    return String(value);
  };

  const renderConfigSection = (title: string, fields: ConfigField[]) => (
    <div className="space-y-4">
      <h3 className="text-lg font-semibold text-foreground border-b border-border pb-2">
        {title}
      </h3>
      <div className="space-y-4">
        {fields.map((field, idx) => (
          <div key={idx} className="space-y-1">
            <div className="flex items-center justify-between">
              <label className="text-sm font-medium text-foreground">
                {field.name}
              </label>
              <span className="text-sm font-mono bg-muted px-3 py-1 rounded">
                {formatValue(field.value, field.type)}
              </span>
            </div>
            <p className="text-xs text-muted-foreground">{field.description}</p>
          </div>
        ))}
      </div>
    </div>
  );

  return (
    <div className="h-full flex flex-col bg-background">
      <header className="border-b border-border px-6 py-4">
        <div className="flex items-center justify-between">
          <div>
            <h2 className="text-xl font-semibold text-foreground">Deployment Configuration</h2>
            <p className="text-sm text-muted-foreground mt-1">
              Read-only view of deployment-level configuration set via environment variables.
            </p>
          </div>
        </div>
      </header>

      <div className="flex-1 p-6 overflow-auto">
        <div className="max-w-4xl mx-auto">
          {loading && (
            <div className="py-8 text-center text-sm text-muted-foreground">
              Loading configuration...
            </div>
          )}

          {error && (
            <div className="py-4 px-4 bg-[#4a1010] border border-[#8b0000] rounded-lg">
              <p className="text-sm text-[#ffcccc]">{error}</p>
            </div>
          )}

          {config && !loading && !error && (
            <div className="space-y-8">
              {renderConfigSection("Server Configuration", config.server)}
              {renderConfigSection("API Configuration", config.api)}
              {renderConfigSection("Transpilation Defaults", config.transpilation_defaults)}
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
