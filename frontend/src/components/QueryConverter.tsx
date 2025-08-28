'use client'

import { useState } from 'react'
import { ConversionResult } from '@/types/api'

interface QueryConverterProps {
  setIsLoading: (loading: boolean) => void
}

export default function QueryConverter({ setIsLoading }: QueryConverterProps) {
  const [formData, setFormData] = useState({
    query: '',
    query_id: '',
    from_sql: '',
    to_sql: 'e6',
    feature_flags: ''
  })

  const [result, setResult] = useState<ConversionResult | null>(null)
  const [error, setError] = useState<string | null>(null)

  const handleInputChange = (e: React.ChangeEvent<HTMLInputElement | HTMLSelectElement | HTMLTextAreaElement>) => {
    const { name, value } = e.target
    setFormData(prev => ({ ...prev, [name]: value }))
  }

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault()
    if (!formData.query.trim() || !formData.from_sql) {
      setError('Query and source dialect are required')
      return
    }

    setIsLoading(true)
    setError(null)
    setResult(null)

    try {
      const response = await fetch('/api/convert-query', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify(formData)
      })

      if (!response.ok) {
        const errorData = await response.json()
        throw new Error(errorData.error || `HTTP ${response.status}`)
      }

      const data = await response.json()
      setResult(data)
    } catch (error) {
      console.error('Conversion error:', error)
      setError(error instanceof Error ? error.message : 'Failed to convert query')
    } finally {
      setIsLoading(false)
    }
  }

  const handleClear = () => {
    setFormData({
      query: '',
      query_id: '',
      from_sql: '',
      to_sql: 'e6',
      feature_flags: ''
    })
    setResult(null)
    setError(null)
  }

  return (
    <div className="max-w-6xl mx-auto">
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* Input Form */}
        <div className="bg-white rounded-lg shadow-md p-6">
          <h2 className="text-xl font-semibold text-gray-800 mb-6">
            <span className="mr-2">🔄</span>
            SQL Query Converter
          </h2>

          <form onSubmit={handleSubmit} className="space-y-4">
            <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-1">
                  Source Dialect *
                </label>
                <select
                  name="from_sql"
                  value={formData.from_sql}
                  onChange={handleInputChange}
                  className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
                  required
                >
                  <option value="">Select source dialect</option>
                  <option value="snowflake">Snowflake</option>
                  <option value="athena">Athena</option>
                  <option value="bigquery">BigQuery</option>
                  <option value="databricks">Databricks</option>
                  <option value="postgres">PostgreSQL</option>
                  <option value="mysql">MySQL</option>
                  <option value="redshift">Redshift</option>
                  <option value="oracle">Oracle</option>
                  <option value="mssql">SQL Server</option>
                  <option value="hive">Hive</option>
                  <option value="spark">Spark SQL</option>
                </select>
              </div>

              <div>
                <label className="block text-sm font-medium text-gray-700 mb-1">
                  Target Dialect
                </label>
                <select
                  name="to_sql"
                  value={formData.to_sql}
                  onChange={handleInputChange}
                  className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
                >
                  <option value="e6">E6 (default)</option>
                  <option value="postgres">PostgreSQL</option>
                  <option value="mysql">MySQL</option>
                  <option value="spark">Spark SQL</option>
                  <option value="snowflake">Snowflake</option>
                  <option value="bigquery">BigQuery</option>
                </select>
              </div>
            </div>

            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1">
                Query ID (Optional)
              </label>
              <input
                type="text"
                name="query_id"
                value={formData.query_id}
                onChange={handleInputChange}
                placeholder="e.g., query_001"
                className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500 placeholder:text-gray-700"
              />
            </div>

            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1">
                SQL Query *
              </label>
              <textarea
                name="query"
                value={formData.query}
                onChange={handleInputChange}
                rows={8}
                placeholder="Enter your SQL query here..."
                className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500 font-mono text-sm placeholder:text-gray-700"
                required
              />
            </div>

            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1">
                Feature Flags (JSON, Optional)
              </label>
              <textarea
                name="feature_flags"
                value={formData.feature_flags}
                onChange={handleInputChange}
                rows={2}
                placeholder='{"PRETTY_PRINT": true, "USE_TWO_PHASE_QUALIFICATION_SCHEME": false}'
                className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500 font-mono text-sm placeholder:text-gray-700"
              />
              <p className="text-xs text-gray-500 mt-1">Optional configuration flags</p>
            </div>

            <div className="flex gap-3">
              <button
                type="submit"
                className="px-6 py-2 bg-blue-600 text-white rounded-md hover:bg-blue-700 focus:outline-none focus:ring-2 focus:ring-blue-500"
              >
                <span className="mr-2">🔄</span>
                Convert Query
              </button>
              <button
                type="button"
                onClick={handleClear}
                className="px-4 py-2 border border-gray-300 text-gray-700 rounded-md hover:bg-gray-50 focus:outline-none focus:ring-2 focus:ring-blue-500"
              >
                <span className="mr-2">🧹</span>
                Clear
              </button>
            </div>
          </form>
        </div>

        {/* Results */}
        <div className="bg-white rounded-lg shadow-md p-6">
          <h2 className="text-xl font-semibold text-gray-800 mb-6">
            <span className="mr-2">📄</span>
            Converted Query
          </h2>

          {error && (
            <div className="bg-red-50 border border-red-200 rounded-md p-4 mb-4">
              <div className="text-red-800">
                <strong>Error:</strong> {error}
              </div>
            </div>
          )}

          {result ? (
            <div className="space-y-4">
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-2">
                  Converted SQL:
                </label>
                <div className="relative">
                  <textarea
                    value={result.converted_query}
                    readOnly
                    rows={12}
                    className="w-full px-3 py-2 border border-gray-300 rounded-md bg-gray-50 font-mono text-sm"
                  />
                  <button
                    onClick={() => navigator.clipboard.writeText(result.converted_query)}
                    className="absolute top-2 right-2 px-2 py-1 bg-blue-500 text-white text-xs rounded hover:bg-blue-600"
                    title="Copy to clipboard"
                  >
                    📋 Copy
                  </button>
                </div>
              </div>
            </div>
          ) : (
            <div className="text-center text-gray-500 py-8">
              <div className="text-4xl mb-2">🔄</div>
              <p>Enter a query and click &quot;Convert Query&quot; to see the results</p>
            </div>
          )}
        </div>
      </div>
    </div>
  )
}