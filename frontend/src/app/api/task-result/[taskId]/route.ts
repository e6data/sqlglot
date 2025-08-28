import { NextRequest } from 'next/server'
import { forwardToFastAPI } from '@/lib/api-utils'

export async function GET(
  request: NextRequest,
  { params }: { params: Promise<{ taskId: string }> }
) {
  const { taskId } = await params
  return forwardToFastAPI(`/task-result/${taskId}`)
}