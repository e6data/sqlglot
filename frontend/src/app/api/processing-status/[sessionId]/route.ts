import { NextRequest } from 'next/server'
import { forwardToFastAPI } from '@/lib/api-utils'

export async function GET(
  request: NextRequest,
  { params }: { params: Promise<{ sessionId: string }> }
) {
  const { sessionId } = await params
  return forwardToFastAPI(`/processing-status/${sessionId}`)
}