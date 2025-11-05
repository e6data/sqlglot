export default function BatchMode() {
  return (
    <div className="h-full flex flex-col bg-background">
      <header className="border-b border-border px-6 py-4">
        <div className="flex items-center justify-between">
          <h2 className="text-xl font-semibold text-foreground">Batch Mode</h2>
        </div>
      </header>

      <div className="flex-1 flex items-center justify-center p-6">
        <div className="text-center">
          <h3 className="text-2xl font-bold text-foreground mb-2">
            Batch Mode Coming Soon
          </h3>
          <p className="text-foreground/60">
            Convert multiple SQL files at once
          </p>
        </div>
      </div>
    </div>
  );
}
