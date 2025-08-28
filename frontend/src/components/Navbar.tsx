interface NavbarProps {
  connectionStatus: string
}

export default function Navbar({ connectionStatus }: NavbarProps) {
  return (
    <nav className="bg-gray-900 text-white shadow-lg">
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
        <div className="flex items-center justify-between h-16">
          <div className="flex items-center">
            <div className="flex-shrink-0">
              <h1 className="text-xl font-bold">
                <span className="mr-2">🗄️</span>
                SQLGlot Parquet Processing
              </h1>
            </div>
          </div>
          <div className="flex items-center">
            <span className="text-sm flex items-center">
              <div className={`w-3 h-3 rounded-full mr-2 ${
                connectionStatus === 'Connected' ? 'bg-green-400' : 'bg-red-400'
              }`}></div>
              {connectionStatus}
            </span>
          </div>
        </div>
      </div>
    </nav>
  )
}