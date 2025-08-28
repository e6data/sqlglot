'use client'

import { useState, useEffect, useCallback } from 'react'
import { ProcessingStatus } from '@/types/api'

interface ProcessingResultsProps {
  sessionId: string | null
  refreshTrigger: number
}

export default function ProcessingResults({ sessionId, refreshTrigger }: ProcessingResultsProps) {
  const [status, setStatus] = useState<ProcessingStatus | null>(null)
  const [loading, setLoading] = useState(false)

  const fetchStatus = useCallback(async () => {
    if (!sessionId) return
    
    setLoading(true)
    try {
      const response = await fetch(`/api/processing-status/${sessionId}`)
      if (response.ok) {
        const data = await response.json()
        setStatus(data)
      }
    } catch (error) {
      console.error('Failed to fetch status:', error)
    } finally {
      setLoading(false)
    }
  }, [sessionId])

  useEffect(() => {
    if (sessionId) {
      fetchStatus()
      const interval = setInterval(fetchStatus, 5000) // Poll every 5 seconds
      return () => clearInterval(interval)
    }
  }, [sessionId, refreshTrigger, fetchStatus])

  if (!sessionId) {
    return (
      <div className="bg-white rounded-lg shadow-md p-6">
        <h2 className="text-xl font-semibold text-gray-800 mb-6">
          <span className="mr-2">📊</span>
          Processing Results
        </h2>
        <div className="text-center text-gray-500 py-8">
          <div className="text-4xl mb-2">📋</div>
          <p className="text-lg mb-2">No session selected</p>
          <p className="text-sm">Select a session from the "Select Session" panel above to view detailed results</p>
        </div>
      </div>
    )
  }

  return (
    <div className="bg-white rounded-lg shadow-md p-6">
      <div className="mb-6">
        <h2 className="text-xl font-semibold text-gray-800">
          <span className="mr-2">📊</span>
          Processing Results
        </h2>
        <p className="text-sm text-gray-500 mt-1">
          Session: <span className="font-mono text-gray-700">{sessionId}</span>
        </p>
      </div>

      {status && (
        <>
          {/* Stats Cards */}
          <div className="grid grid-cols-2 md:grid-cols-5 gap-4 mb-6">
            <div className="bg-purple-500 text-white rounded-lg p-4 text-center">
              <div className="text-2xl font-bold">{status.total_batches || 0}</div>
              <div className="text-sm">Total Batches</div>
            </div>
            <div className="bg-blue-500 text-white rounded-lg p-4 text-center">
              <div className="text-2xl font-bold">{status.total_tasks || 0}</div>
              <div className="text-sm">Total Tasks</div>
            </div>
            <div className="bg-green-500 text-white rounded-lg p-4 text-center">
              <div className="text-2xl font-bold">{status.completed || 0}</div>
              <div className="text-sm">Completed</div>
            </div>
            <div className="bg-red-500 text-white rounded-lg p-4 text-center">
              <div className="text-2xl font-bold">{status.failed || 0}</div>
              <div className="text-sm">Failed</div>
            </div>
            <div className="bg-yellow-500 text-white rounded-lg p-4 text-center">
              <div className="text-2xl font-bold">{status.processing || 0}</div>
              <div className="text-sm">Processing</div>
            </div>
          </div>

          {/* Progress Bar */}
          <div className="mb-6">
            <div className="flex justify-between text-sm text-gray-600 mb-1">
              <span>Progress</span>
              <span>{Math.round(status.progress_percentage || 0)}%</span>
            </div>
            <div className="w-full bg-gray-200 rounded-full h-4">
              <div
                className="bg-blue-600 h-4 rounded-full transition-all duration-300"
                style={{ width: `${status.progress_percentage || 0}%` }}
              ></div>
            </div>
          </div>

          {/* Session Details */}
          <div className="mb-6">
            <h3 className="font-semibold text-gray-800 mb-3">Session Details</h3>
            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4 text-sm mb-4">
              <div className="bg-gray-50 rounded-lg p-3">
                <div className="font-medium text-gray-600">Session ID</div>
                <div className="text-gray-800 text-xs">{sessionId}</div>
              </div>
              <div className="bg-gray-50 rounded-lg p-3">
                <div className="font-medium text-gray-600">Status</div>
                <span className={`inline-block px-2 py-1 rounded-full text-xs ${
                  status.overall_status === 'completed' ? 'bg-green-100 text-green-800' :
                  status.overall_status === 'failed' ? 'bg-red-100 text-red-800' :
                  'bg-yellow-100 text-yellow-800'
                }`}>
                  {status.overall_status || 'processing'}
                </span>
              </div>
              <div className="bg-gray-50 rounded-lg p-3">
                <div className="font-medium text-gray-600">Total Batches</div>
                <div className="text-gray-800">{status.total_batches || 0}</div>
              </div>
              <div className="bg-gray-50 rounded-lg p-3">
                <div className="font-medium text-gray-600">Successful Batches</div>
                <div className="text-gray-800">{status.successful_batches || 0}</div>
              </div>
            </div>
            
            {/* Timing Information */}
            <div className="grid grid-cols-1 md:grid-cols-3 gap-4 text-sm">
              <div className="bg-blue-50 rounded-lg p-3">
                <div className="font-medium text-blue-700">Start Time</div>
                <div className="text-blue-900">
                  {status.start_time ? new Date(status.start_time).toLocaleString() : 'Not available'}
                </div>
              </div>
              <div className="bg-green-50 rounded-lg p-3">
                <div className="font-medium text-green-700">End Time</div>
                <div className="text-green-900">
                  {status.end_time ? new Date(status.end_time).toLocaleString() : 
                   status.overall_status === 'completed' ? 'Just completed' : 'Still processing'}
                </div>
              </div>
              <div className="bg-purple-50 rounded-lg p-3">
                <div className="font-medium text-purple-700">Total Duration</div>
                <div className="text-purple-900">
                  {status.duration || (status.overall_status === 'completed' ? 'Calculating...' : 'In progress')}
                </div>
              </div>
            </div>
          </div>

          {/* All Batch Details - Full Width at Bottom */}
          <div>
            <div className="flex items-center justify-between mb-4">
              <h3 className="text-lg font-semibold text-gray-800">All Batch Details</h3>
              {status.task_details && (
                <span className="text-sm text-gray-500">
                  {status.task_details.length} batches total
                </span>
              )}
            </div>
            
            {status.task_details && status.task_details.length > 0 ? (
              <div className="max-h-96 overflow-y-auto space-y-3 border border-gray-300 rounded-lg p-4 bg-gray-50">
                {status.task_details.map((task, index: number) => (
                  <div key={task.task_id || index} className="border border-gray-200 rounded-lg p-4 bg-white shadow-sm">
                    <div className="flex justify-between items-center mb-3">
                      <div>
                        <span className="text-lg font-medium text-gray-800">Batch {index + 1}</span>
                        <span className="text-sm text-gray-500 ml-2">({task.task_id})</span>
                      </div>
                      <span className={`px-4 py-2 text-sm rounded-full font-medium ${
                        task.state === 'SUCCESS' ? 'bg-green-100 text-green-800' :
                        task.state === 'FAILURE' ? 'bg-red-100 text-red-800' :
                        task.state === 'PROGRESS' ? 'bg-blue-100 text-blue-800' :
                        'bg-gray-100 text-gray-800'
                      }`}>
                        {task.state || 'PENDING'}
                      </span>
                    </div>
                    
                    <div className="grid grid-cols-1 md:grid-cols-3 gap-4 text-sm">
                      {task.processed_count && (
                        <div className="bg-blue-50 rounded p-3">
                          <div className="font-medium text-blue-700">Processed Queries</div>
                          <div className="text-blue-900 text-lg">{task.processed_count}</div>
                        </div>
                      )}
                      
                      {task.successful_count !== undefined && (
                        <div className="bg-green-50 rounded p-3">
                          <div className="font-medium text-green-700">Successful Queries</div>
                          <div className="text-green-900 text-lg">{task.successful_count}</div>
                        </div>
                      )}
                      
                      {task.status && task.status !== task.state && (
                        <div className="bg-gray-50 rounded p-3">
                          <div className="font-medium text-gray-700">Detailed Status</div>
                          <div className="text-gray-900">{task.status}</div>
                        </div>
                      )}
                    </div>
                    
                    {task.error && (
                      <div className="mt-3 p-3 bg-red-50 border border-red-200 rounded-lg">
                        <div className="font-medium text-red-800 mb-1">Error Details</div>
                        <div className="text-sm text-red-700">{task.error}</div>
                      </div>
                    )}
                  </div>
                ))}
              </div>
            ) : (
              <div className="text-center py-12 text-gray-500 border border-gray-300 rounded-lg bg-gray-50">
                <div className="text-4xl mb-4">📋</div>
                <p className="text-lg">No batch details available</p>
                <p className="text-sm mt-2">Batch details will appear here once processing starts</p>
              </div>
            )}
          </div>
        </>
      )}

      {loading && (
        <div className="flex justify-center py-4">
          <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-blue-600"></div>
        </div>
      )}
    </div>
  )
}