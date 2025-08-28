import { NextRequest } from 'next/server'
import { forwardToFastAPI } from '@/lib/api-utils'

export async function POST(request: NextRequest) {
  const formData = await request.formData()
  return forwardToFastAPI('/validate-s3-bucket', 'POST', formData)
}