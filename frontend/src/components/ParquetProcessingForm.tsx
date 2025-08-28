'use client'

import { useState } from 'react'
import { ValidationResult } from '@/types/api'

interface ParquetProcessingFormProps {
  onProcessingStart: (sessionId: string) => void
  setIsLoading: (loading: boolean) => void
}

export default function ParquetProcessingForm({ 
  onProcessingStart, 
  setIsLoading 
}: ParquetProcessingFormProps) {
  const [formData, setFormData] = useState({
    directory_path: '',
    company_name: '',
    from_dialect: '',
    to_dialect: 'e6',
    query_column: '',
    batch_size: 10000,
    filters: ''
  })

  const [validationResult, setValidationResult] = useState<ValidationResult | null>(null)
  const [showValidation, setShowValidation] = useState(false)

  const handleInputChange = (e: React.ChangeEvent<HTMLInputElement | HTMLSelectElement | HTMLTextAreaElement>) => {
    const { name, value } = e.target
    setFormData(prev => ({
      ...prev,
      [name]: name === 'batch_size' ? parseInt(value) || 10000 : value
    }))
  }

  const handleValidateS3 = async () => {
    if (!formData.directory_path) {
      alert('Please enter a directory path first')
      return
    }

    setIsLoading(true)
    try {
      const formDataToSend = new FormData()
      formDataToSend.append('s3_path', formData.directory_path)

      const response = await fetch('/api/validate-s3-bucket', {
        method: 'POST',
        body: formDataToSend
      })

      const result = await response.json()
      setValidationResult(result)
      setShowValidation(true)

      // Auto-populate query column if found
      if (result.query_column) {
        setFormData(prev => ({
          ...prev,
          query_column: result.query_column
        }))
      }
    } catch (error) {
      console.error('Validation error:', error)
      setValidationResult({ error: 'Failed to validate S3 path' })
      setShowValidation(true)
    } finally {
      setIsLoading(false)
    }
  }

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault()
    setIsLoading(true)

    try {
      const formDataToSend = new FormData()
      Object.entries(formData).forEach(([key, value]) => {
        formDataToSend.append(key, value.toString())
      })

      const response = await fetch('/api/process-parquet', {
        method: 'POST',
        body: formDataToSend
      })

      if (!response.ok) {
        throw new Error(`HTTP error! status: ${response.status}`)
      }

      const result = await response.json()
      
      // Store session ID in localStorage
      const existingSessions = JSON.parse(localStorage.getItem('processing_sessions') || '[]')
      if (!existingSessions.includes(result.session_id)) {
        existingSessions.push(result.session_id)
        localStorage.setItem('processing_sessions', JSON.stringify(existingSessions))
      }
      
      onProcessingStart(result.session_id)
    } catch (error) {
      console.error('Processing error:', error)
      alert('Failed to start processing. Please try again.')
    } finally {
      setIsLoading(false)
    }
  }

  return (
    <div className="bg-white rounded-lg shadow-md p-6">
      <div className="flex items-center mb-6">
        <h2 className="text-xl font-semibold text-gray-800">
          <span className="mr-2">▶️</span>
          Start Processing
        </h2>
      </div>

      <form onSubmit={handleSubmit} className="space-y-4">
        <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-1">
              Directory Path *
            </label>
            <input
              type="text"
              name="directory_path"
              value={formData.directory_path}
              onChange={handleInputChange}
              placeholder="/path/to/parquet/files"
              className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent"
              required
            />
            <p className="text-xs text-gray-500 mt-1">Local path or S3 path to parquet files</p>
          </div>

          <div>
            <label className="block text-sm font-medium text-gray-700 mb-1">
              Company Name *
            </label>
            <input
              type="text"
              name="company_name"
              value={formData.company_name}
              onChange={handleInputChange}
              placeholder="e.g., acme_corp"
              className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent"
              required
            />
            <p className="text-xs text-gray-500 mt-1">Used for Iceberg partitioning</p>
          </div>
        </div>

        <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-1">
              Source Dialect *
            </label>
            <select
              name="from_dialect"
              value={formData.from_dialect}
              onChange={handleInputChange}
              className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent"
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
              name="to_dialect"
              value={formData.to_dialect}
              onChange={handleInputChange}
              className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent"
            >
              <option value="e6">E6 (default)</option>
              <option value="postgres">PostgreSQL</option>
              <option value="mysql">MySQL</option>
              <option value="spark">Spark SQL</option>
            </select>
          </div>
        </div>

        <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-1">
              Query Column *
            </label>
            <input
              type="text"
              name="query_column"
              value={formData.query_column}
              onChange={handleInputChange}
              placeholder="e.g., hashed_query, sql_text, query_string"
              className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent"
              required
            />
            <p className="text-xs text-gray-500 mt-1">Column containing SQL queries</p>
          </div>

          <div>
            <label className="block text-sm font-medium text-gray-700 mb-1">
              Batch Size
            </label>
            <input
              type="number"
              name="batch_size"
              value={formData.batch_size}
              onChange={handleInputChange}
              min="10"
              max="100000"
              className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent"
            />
            <p className="text-xs text-gray-500 mt-1">Number of queries per batch</p>
          </div>
        </div>

        <div>
          <label className="block text-sm font-medium text-gray-700 mb-1">
            Filters (JSON)
          </label>
          <textarea
            name="filters"
            value={formData.filters}
            onChange={handleInputChange}
            rows={2}
            placeholder='{"statement_type": "SELECT", "client_application": "PowerBI"}'
            className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent"
          />
          <p className="text-xs text-gray-500 mt-1">Optional column filters</p>
        </div>

        <div className="flex gap-3">
          <button
            type="button"
            onClick={handleValidateS3}
            className="px-4 py-2 border border-blue-500 text-blue-500 rounded-md hover:bg-blue-50 focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent"
          >
            <span className="mr-2">✓</span>
            Validate S3 Path
          </button>
          <button
            type="submit"
            className="px-6 py-2 bg-blue-600 text-white rounded-md hover:bg-blue-700 focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent"
          >
            <span className="mr-2">▶️</span>
            Start Processing
          </button>
        </div>
      </form>

      {showValidation && validationResult && (
        <div className={`mt-4 p-4 rounded-md ${
          validationResult.error ? 'bg-red-50 border border-red-200' : 'bg-green-50 border border-green-200'
        }`}>
          {validationResult.error ? (
            <div className="text-red-800">
              <strong>Validation Failed:</strong> {validationResult.error}
            </div>
          ) : (
            <div className="text-green-800">
              <strong>Validation Successful!</strong>
              <ul className="mt-2 text-sm">
                <li>Files found: {validationResult.files_found}</li>
                <li>Total size: {validationResult.total_size_mb} MB</li>
                {validationResult.query_column && (
                  <li>Query column: {validationResult.query_column}</li>
                )}
              </ul>
            </div>
          )}
        </div>
      )}
    </div>
  )
}