'use client'

import { useState, useEffect } from 'react'
import ParquetProcessingForm from '@/components/ParquetProcessingForm'
import ActiveSessions from '@/components/ActiveSessions'
import ProcessingResults from '@/components/ProcessingResults'
import QueryConverter from '@/components/QueryConverter'
import QueryStats from '@/components/QueryStats'
import Navbar from '@/components/Navbar'
import LoadingModal from '@/components/LoadingModal'

type TabType = 'batch-processing' | 'query-converter' | 'query-stats'

export default function Home() {
  const [activeTab, setActiveTab] = useState<TabType>('batch-processing')
  const [isLoading, setIsLoading] = useState(false)
  const [currentSession, setCurrentSession] = useState<string | null>(null)
  const [connectionStatus] = useState('Connected')
  
  // Trigger for manual refreshes only (no auto-refresh)
  const [refreshTrigger, setRefreshTrigger] = useState(0)
  
  // Load selected session from localStorage on page load
  useEffect(() => {
    const savedSession = localStorage.getItem('selected_session')
    if (savedSession) {
      setCurrentSession(savedSession)
    }
  }, [])
  
  // Removed auto-refresh interval - components now handle their own intelligent polling

  const handleProcessingStart = (sessionId: string) => {
    setCurrentSession(sessionId)
    localStorage.setItem('selected_session', sessionId)
    setRefreshTrigger(prev => prev + 1)
  }

  const handleSessionSelect = (sessionId: string) => {
    setCurrentSession(sessionId)
    localStorage.setItem('selected_session', sessionId)
    setRefreshTrigger(prev => prev + 1)
  }

  const handleRefreshSessions = () => {
    setRefreshTrigger(prev => prev + 1)
  }

  const tabs = [
    { id: 'batch-processing' as TabType, label: '📦 Batch Processing', icon: '📦' },
    { id: 'query-converter' as TabType, label: '🔄 Query Converter', icon: '🔄' },
    { id: 'query-stats' as TabType, label: '📊 Query Statistics', icon: '📊' },
  ]

  return (
    <div className="min-h-screen bg-gray-50">
      <Navbar connectionStatus={connectionStatus} />
      
      <div className="container mx-auto px-4 mt-8">
        {/* Tab Navigation */}
        <div className="bg-white rounded-lg shadow-md mb-6">
          <div className="flex border-b border-gray-200">
            {tabs.map((tab) => (
              <button
                key={tab.id}
                onClick={() => setActiveTab(tab.id)}
                className={`px-6 py-4 text-sm font-medium rounded-t-lg transition-colors ${
                  activeTab === tab.id
                    ? 'bg-blue-50 text-blue-600 border-b-2 border-blue-600'
                    : 'text-gray-500 hover:text-gray-700 hover:bg-gray-50'
                }`}
              >
                <span className="mr-2">{tab.icon}</span>
                {tab.label}
              </button>
            ))}
          </div>
        </div>

        {/* Tab Content */}
        {activeTab === 'batch-processing' && (
          <>
            <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
              {/* Processing Form */}
              <ParquetProcessingForm 
                onProcessingStart={handleProcessingStart}
                setIsLoading={setIsLoading}
              />
              
              {/* Active Sessions */}
              <ActiveSessions 
                refreshTrigger={refreshTrigger}
                onRefresh={handleRefreshSessions}
                onSessionSelect={handleSessionSelect}
                selectedSession={currentSession}
              />
            </div>

            {/* Processing Results */}
            <div className="mt-6">
              <ProcessingResults 
                sessionId={currentSession}
                refreshTrigger={refreshTrigger}
              />
            </div>
          </>
        )}

        {activeTab === 'query-converter' && (
          <QueryConverter setIsLoading={setIsLoading} />
        )}

        {activeTab === 'query-stats' && (
          <QueryStats setIsLoading={setIsLoading} />
        )}
      </div>

      {/* Loading Modal */}
      <LoadingModal isOpen={isLoading} />
    </div>
  )
}