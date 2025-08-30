import { NextRequest } from 'next/server'
import { forwardToFastAPI } from '@/lib/api-utils'

export async function POST(request: NextRequest) {
  const formData = await request.formData()
  return forwardToFastAPI('/process-parquet-directory-automated', 'POST', formData)
}