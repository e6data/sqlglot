import { NextRequest } from 'next/server'
import { forwardToFastAPI } from '@/lib/api-utils'

export async function POST(request: NextRequest) {
  const body = await request.json()
  
  // Convert JSON to FormData as required by FastAPI endpoint
  const formData = new FormData()
  formData.append('query', body.query || '')
  formData.append('query_id', body.query_id || 'NO_ID_MENTIONED')
  formData.append('from_sql', body.from_sql || '')
  formData.append('to_sql', body.to_sql || 'e6')
  if (body.feature_flags) {
    formData.append('feature_flags', body.feature_flags)
  }
  
  return forwardToFastAPI('/statistics', 'POST', formData)
}