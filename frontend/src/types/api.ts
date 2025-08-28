export interface SessionData {
  id: string
  session_id?: string
  company_name: string
  status: 'processing' | 'completed' | 'failed'
  completed_tasks?: number
  total_tasks?: number
  created_at: string
  startTime?: string
  currentStatus?: ProcessingStatus
  lastUpdated?: string
}

export interface ProcessingStatus {
  session_id: string
  total_tasks: number
  total_batches: number
  completed: number
  failed: number
  pending: number
  processing: number
  successful_batches: number
  progress_percentage: number
  overall_status: string
  task_details: TaskDetail[]
  start_time?: string
  end_time?: string
  duration?: string
}

export interface TaskDetail {
  task_id: string
  state: 'PENDING' | 'PROGRESS' | 'SUCCESS' | 'FAILURE'
  status: string
  error?: string
  processed_count?: number
  successful_count?: number
}

export interface TaskStatus {
  task_id: string
  state: 'PENDING' | 'PROGRESS' | 'SUCCESS' | 'FAILURE'
  result?: {
    processed_queries: number
  }
}

export interface ValidationResult {
  error?: string
  files_found?: number
  total_size_mb?: number
  query_column?: string
}

export interface ConversionResult {
  converted_query: string
}

export interface StatsResult {
  supported_functions: string[]
  unsupported_functions: string[]
  udf_list: string[]
  'converted-query': string
  unsupported_functions_after_transpilation: string[]
  executable: 'YES' | 'NO'
  tables_list: string[]
  joins_list: string[]
  cte_values_subquery_list: string[]
  error: boolean
  log_records?: unknown[]
}