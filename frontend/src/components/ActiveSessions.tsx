'use client'

import { useState, useEffect } from 'react'
import { SessionData } from '@/types/api'

interface ActiveSessionsProps {
  refreshTrigger: number
  onRefresh: () => void
  onSessionSelect: (sessionId: string) => void
  selectedSession: string | null
}

export default function ActiveSessions({ refreshTrigger, onRefresh, onSessionSelect, selectedSession }: ActiveSessionsProps) {
  const [sessions, setSessions] = useState<SessionData[]>([])
  const [loading, setLoading] = useState(false)

  useEffect(() => {
    let intervalId: NodeJS.Timeout | null = null
    let isMounted = true
    
    const fetchSessionsWithPolling = async () => {
      if (!isMounted) return
      
      setLoading(true)
      try {
        // Discover active sessions from Redis (not localStorage)
        const activeSessionIds = await discoverSessionsFromRedis()
        
        if (activeSessionIds.length === 0) {
          setSessions([])
          return
        }
        
        // Fetch status for each discovered session
        const sessionPromises = activeSessionIds.map(async (sessionId: string) => {
          try {
            const response = await fetch(`/api/processing-status/${sessionId}`)
            if (response.ok) {
              const status = await response.json()
              return {
                id: sessionId,
                company_name: 'Processing Session',
                status: status.overall_status === 'completed' ? 'completed' : 
                        status.failed > 0 ? 'failed' : 'processing',
                completed_tasks: status.completed,
                total_tasks: status.total_tasks,
                created_at: status.start_time || sessionId,
                currentStatus: status
              }
            }
            return null
          } catch (error) {
            console.warn(`Failed to get status for session ${sessionId}:`, error)
            return null
          }
        })
        
        const sessionsData = await Promise.all(sessionPromises)
        const validSessions = sessionsData.filter(Boolean) as SessionData[]
        
        if (isMounted) {
          setSessions(validSessions)
          
          // Update localStorage to match Redis reality (keep in sync but Redis is source of truth)
          const redisSessionIds = validSessions.map(s => s.id)
          localStorage.setItem('processing_sessions', JSON.stringify(redisSessionIds))
          
          // Check if any session is still processing
          const hasActiveSession = validSessions.some(s => 
            s.status === 'processing' || 
            (s.currentStatus && s.currentStatus.overall_status === 'processing')
          )
          
          // Schedule next poll only if there are active sessions
          if (hasActiveSession && isMounted) {
            intervalId = setTimeout(fetchSessionsWithPolling, 10000)
          }
        }
      } catch (error) {
        console.error('Failed to discover sessions from Redis:', error)
        if (isMounted) {
          setSessions([])
        }
      } finally {
        if (isMounted) {
          setLoading(false)
        }
      }
    }
    
    // Start initial fetch
    fetchSessionsWithPolling()
    
    // Cleanup
    return () => {
      isMounted = false
      if (intervalId) {
        clearTimeout(intervalId)
      }
    }
  }, [refreshTrigger])

  const clearAllSessions = () => {
    if (confirm('Clear browser storage? Sessions will reload from Redis automatically.')) {
      localStorage.removeItem('processing_sessions')
      localStorage.removeItem('session_names')
      
      // Trigger a refresh by updating refreshTrigger
      onRefresh()
    }
  }

  const discoverSessionsFromRedis = async () => {
    try {
      // Use the special 'discover_all' session ID to get all active sessions from Redis
      const response = await fetch(`/api/processing-status/discover_all`)
      if (response.ok) {
        const data = await response.json()
        return data.discovered_sessions || []
      } else {
        console.error('Failed to discover sessions from Redis')
        return []
      }
    } catch (error) {
      console.error('Error discovering sessions from Redis:', error)
      return []
    }
  }


  return (
    <div className="bg-white rounded-lg shadow-md p-6">
      <div className="flex items-center justify-between mb-6">
        <div>
          <h2 className="text-xl font-semibold text-gray-800">
            <span className="mr-2">📋</span>
            Select Session
          </h2>
          <p className="text-sm text-gray-500 mt-1">Click on a session to view detailed batch status</p>
        </div>
        <div className="flex gap-2">
          <button
            onClick={clearAllSessions}
            className="px-3 py-1 text-sm text-red-600 border border-red-300 rounded-md hover:bg-red-50"
            title="Clear all sessions from browser storage"
          >
            Clear All
          </button>
          <button
            onClick={onRefresh}
            className="p-2 text-gray-500 hover:text-gray-700 border border-gray-300 rounded-md hover:bg-gray-50"
            disabled={loading}
            title="Refresh sessions"
          >
            <span className={loading ? 'animate-spin' : ''}>🔄</span>
          </button>
        </div>
      </div>

      <div className="max-h-96 overflow-y-auto">
        {sessions.length === 0 ? (
          <div className="text-center text-gray-500 py-8">
            <div className="text-4xl mb-2">⏳</div>
            <p>No active sessions</p>
          </div>
        ) : (
          <div className="space-y-3">
            {sessions.map((session: SessionData) => {
              const isSelected = selectedSession === session.id
              return (
                <div 
                  key={session.id} 
                  onClick={() => onSessionSelect(session.id)}
                  className={`border rounded-lg p-4 cursor-pointer transition-all duration-200 hover:shadow-md ${
                    isSelected 
                      ? 'border-blue-500 bg-blue-50 shadow-md' 
                      : 'border-gray-200 hover:border-gray-300'
                  }`}
                >
                  <div className="flex justify-between items-start">
                    <div className="flex-1">
                      <div className="flex items-center gap-2">
                        <h3 className={`font-medium ${
                          isSelected ? 'text-blue-900' : 'text-gray-800'
                        }`}>
                          {session.id}
                        </h3>
                        {isSelected && (
                          <span className="text-blue-600 text-sm">✓</span>
                        )}
                      </div>
                      <p className={`text-sm ${
                        isSelected ? 'text-blue-700' : 'text-gray-600'
                      }`}>
                        {session.company_name}
                      </p>
                    </div>
                    <span className={`px-2 py-1 text-xs rounded-full ${
                      session.status === 'completed' ? 'bg-green-100 text-green-800' :
                      session.status === 'failed' ? 'bg-red-100 text-red-800' :
                      'bg-yellow-100 text-yellow-800'
                    }`}>
                      {session.status}
                    </span>
                  </div>
                  <div className={`mt-2 text-sm ${
                    isSelected ? 'text-blue-600' : 'text-gray-500'
                  }`}>
                    <div>Tasks: {session.completed_tasks || 0}/{session.total_tasks || 0}</div>
                    <div>Started: {session.created_at}</div>
                    {session.currentStatus && (
                      <div className="mt-1">
                        Progress: {session.currentStatus.progress_percentage}%
                        {session.currentStatus.duration && ` • Duration: ${session.currentStatus.duration}`}
                        {session.currentStatus.failed > 0 && (
                          <span className="text-red-600 ml-2">• {session.currentStatus.failed} failed batches</span>
                        )}
                      </div>
                    )}
                  </div>
                </div>
              )
            })}
          </div>
        )}
      </div>
    </div>
  )
}