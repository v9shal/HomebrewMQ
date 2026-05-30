export interface Job {
  id: string;
  queue: string;
  payload: string;
  priority: number;
  attempts: number;
  maxRetries: number;
  status: string;
  affinity: string;
  createdAt: number;
  lastError?: string;
}
