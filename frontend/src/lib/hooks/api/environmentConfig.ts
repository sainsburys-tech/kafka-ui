import { useQuery } from '@tanstack/react-query';
import { QUERY_REFETCH_OFF_OPTIONS } from 'lib/constants';

interface EnvironmentConfig {
  label: string;
  color: string;
}

async function fetchEnvironmentConfig(): Promise<EnvironmentConfig> {
  const response = await fetch('/api/environment-config');
  if (!response.ok) {
    throw new Error('Failed to fetch environment config');
  }
  return response.json();
}

export function useEnvironmentConfig() {
  return useQuery<EnvironmentConfig>(
    ['app', 'environmentConfig'],
    fetchEnvironmentConfig,
    QUERY_REFETCH_OFF_OPTIONS
  );
}

