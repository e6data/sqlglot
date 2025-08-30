'use client'

import { useState } from 'react'
import { StatsResult } from '@/types/api'

interface QueryStatsProps {
  setIsLoading: (loading: boolean) => void
}

export default function QueryStats({ setIsLoading }: QueryStatsProps) {
  const [formData, setFormData] = useState({
    query: '',
    query_id: '',
    from_sql: '',
    to_sql: 'e6',
    feature_flags: ''
  })

  const [result, setResult] = useState<StatsResult | null>(null)
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
      const response = await fetch('/api/statistics', {
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
      console.error('Statistics error:', error)
      setError(error instanceof Error ? error.message : 'Failed to get query statistics')
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

  const renderList = (items: string[] | unknown[], title: string, emptyMessage: string) => {
    const itemArray = Array.isArray(items) ? items : Array.from(items || [])
    
    return (
      <div className="bg-gray-50 rounded-lg p-4">
        <h4 className="font-medium text-gray-800 mb-2">{title} ({itemArray.length})</h4>
        {itemArray.length > 0 ? (
          <div className="flex flex-wrap gap-2">
            {itemArray.map((item, index) => (
              <span
                key={index}
                className="px-2 py-1 bg-blue-100 text-blue-800 text-xs rounded-full"
              >
                {String(item)}
              </span>
            ))}
          </div>
        ) : (
          <p className="text-sm text-gray-500">{emptyMessage}</p>
        )}
      </div>
    )
  }

  return (
    <div className="max-w-7xl mx-auto">
      <div className="grid grid-cols-1 xl:grid-cols-3 gap-6">
        {/* Input Form */}
        <div className="bg-white rounded-lg shadow-md p-6">
          <h2 className="text-xl font-semibold text-gray-800 mb-6">
            <span className="mr-2">📊</span>
            Query Statistics
          </h2>

          <form onSubmit={handleSubmit} className="space-y-4">
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
                placeholder='{"PRETTY_PRINT": true}'
                className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500 font-mono text-sm placeholder:text-gray-700"
              />
            </div>

            <div className="flex gap-3">
              <button
                type="submit"
                className="px-6 py-2 bg-blue-600 text-white rounded-md hover:bg-blue-700 focus:outline-none focus:ring-2 focus:ring-blue-500"
              >
                <span className="mr-2">📊</span>
                Analyze Query
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
        <div className="xl:col-span-2 space-y-6">
          {error && (
            <div className="bg-red-50 border border-red-200 rounded-md p-4">
              <div className="text-red-800">
                <strong>Error:</strong> {error}
              </div>
            </div>
          )}

          {result ? (
            <>
              {/* Summary Card */}
              <div className="bg-white rounded-lg shadow-md p-6">
                <h3 className="text-lg font-semibold text-gray-800 mb-4">Query Analysis Summary</h3>
                <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
                  <div className={`text-center p-4 rounded-lg ${
                    result.executable === 'YES' ? 'bg-green-50 text-green-800' : 'bg-red-50 text-red-800'
                  }`}>
                    <div className="text-2xl font-bold">{result.executable}</div>
                    <div className="text-sm">Executable</div>
                  </div>
                  <div className="bg-blue-50 text-blue-800 text-center p-4 rounded-lg">
                    <div className="text-2xl font-bold">{result.supported_functions?.length || 0}</div>
                    <div className="text-sm">Supported Functions</div>
                  </div>
                  <div className="bg-orange-50 text-orange-800 text-center p-4 rounded-lg">
                    <div className="text-2xl font-bold">{Array.from(result.unsupported_functions || []).length}</div>
                    <div className="text-sm">Unsupported Functions</div>
                  </div>
                </div>
              </div>

              {/* Function Analysis */}
              <div className="bg-white rounded-lg shadow-md p-6">
                <h3 className="text-lg font-semibold text-gray-800 mb-4">Function Analysis</h3>
                <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                  {renderList(result.supported_functions, 'Supported Functions', 'No supported functions found')}
                  {renderList(Array.from(result.unsupported_functions || []), 'Unsupported Functions', 'All functions are supported!')}
                </div>
                <div className="mt-4">
                  {renderList(Array.from(result.udf_list || []), 'User Defined Functions (UDFs)', 'No UDFs detected')}
                </div>
                {result.unsupported_functions_after_transpilation && Array.from(result.unsupported_functions_after_transpilation).length > 0 && (
                  <div className="mt-4">
                    {renderList(Array.from(result.unsupported_functions_after_transpilation), 'Unsupported After Transpilation', 'All functions supported after conversion')}
                  </div>
                )}
              </div>

              {/* Query Structure */}
              <div className="bg-white rounded-lg shadow-md p-6">
                <h3 className="text-lg font-semibold text-gray-800 mb-4">Query Structure</h3>
                <div className="space-y-4">
                  {renderList(Array.from(result.tables_list || []), 'Tables Used', 'No tables detected')}
                  {renderList(result.joins_list || [], 'Join Types', 'No joins found')}
                  {renderList(result.cte_values_subquery_list || [], 'CTEs & Subqueries', 'No CTEs or subqueries found')}
                </div>
              </div>

              {/* Converted Query */}
              <div className="bg-white rounded-lg shadow-md p-6">
                <h3 className="text-lg font-semibold text-gray-800 mb-4">Converted Query</h3>
                <div className="relative">
                  <textarea
                    value={result['converted-query']}
                    readOnly
                    rows={12}
                    className={`w-full px-3 py-2 border rounded-md font-mono text-sm ${
                      result.error ? 'bg-red-50 border-red-200 text-red-800' : 'bg-gray-50 border-gray-300'
                    }`}
                  />
                  {!result.error && (
                    <button
                      onClick={() => navigator.clipboard.writeText(result['converted-query'])}
                      className="absolute top-2 right-2 px-2 py-1 bg-blue-500 text-white text-xs rounded hover:bg-blue-600"
                      title="Copy to clipboard"
                    >
                      📋 Copy
                    </button>
                  )}
                </div>
              </div>
            </>
          ) : (
            <div className="bg-white rounded-lg shadow-md p-6">
              <div className="text-center text-gray-500 py-8">
                <div className="text-4xl mb-2">📊</div>
                <p>Enter a query and click &quot;Analyze Query&quot; to see detailed statistics</p>
              </div>
            </div>
          )}
        </div>
      </div>
    </div>
  )
}